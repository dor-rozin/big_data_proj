"""
Streamlit dashboard — the RESULTS stage of the pipeline.

    snapshot -> Kafka -> Spark (transform + MLlib KMeans) -> Elasticsearch -> HERE

Seven fundamentals charts per company over the last five fiscal years, every
number read from Elasticsearch and computed in `kpis.py`. Nothing on this page is
hardcoded: the company list, the fiscal years and every value come from whatever
the pipeline actually loaded.

## How the four modules divide the work

    es_client.py   reads Elasticsearch. No arithmetic, no plotting.
    kpis.py        all arithmetic. No Elasticsearch, no plotting. Testable
                   offline against the parquet snapshot via verify_kpis.py.
    charts.py      all plotting. Takes a computed KPI, returns a figure.
    app.py         wiring and layout only.

The split exists so the numbers can be verified without a running stack — which
matters because standing this pipeline up takes Docker, Kafka, a producer run
and a Spark run before a single chart can be checked.

## Reading the missing values

Roughly a third of the fact fields are null somewhere in this dataset, and every
one of those nulls is real reported behaviour rather than a defect: banks file no
classified balance sheet, BRK.B does not tag EPS in XBRL, Amazon does not tag
total liabilities. Where an input is missing the chart shows a gap and states
which field was absent. It never shows a zero, because a zero is a claim about
the business and a gap is a claim about the filing.
"""
import hashlib
import os
import subprocess
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

import ai_analyst
import charts
import es_client
import indicators
import kafka_tail
import kpis
import woodies_chart
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Financial KPIs & AI Analyst",
                   page_icon="📊", layout="wide")

FISCAL_YEARS = int(os.getenv("DASHBOARD_YEARS") or 5)

# Display-only: every `ts`/`ingested_at` on the wire and in Elasticsearch stays
# UTC, per the frozen schema contract (schemas/README.md) -- this only affects
# how the live price ticker below renders a timestamp on screen.
DASHBOARD_TZ = ZoneInfo(os.getenv("DASHBOARD_TZ") or "Asia/Jerusalem")


def md(text) -> str:
    """Escape `$` before any model-written text reaches st.markdown().

    Streamlit renders `$...$` as LaTeX. Analyst prose is full of dollar amounts,
    so "revenue climbed from $16.7 B in 2021 to $130.5 B in 2025" has its middle
    swallowed and re-rendered as an equation -- the words run together in italic
    maths type and the figures vanish. Escaping is the whole fix; the text is
    otherwise fine.
    """
    return str(text).replace("$", r"\$") if text else ""

# Set by docker-compose.yml so the "Refresh data" button below can run
# `docker compose` from the SAME absolute path as a host terminal would --
# see the comment on the dashboard service's volumes for why this matters.
PROJECT_DIR = os.getenv("PROJECT_DIR")


def run_spark_refresh() -> tuple[bool, str]:
    """Re-run the Spark batch job (Kafka -> Elasticsearch) via the host's Docker
    daemon, Docker-out-of-Docker: this container mounts /var/run/docker.sock,
    so `docker compose` here launches a sibling container, not a nested one.

    Spark does a full reprocess each run (no offset tracking, see RUNBOOK), so
    this always picks up everything currently on the Kafka topics -- whatever
    the replay/live producers have sent since the stack came up.
    """
    if not PROJECT_DIR:
        return False, ("PROJECT_DIR is not set. This button only works when the "
                        "dashboard is run via `docker compose up`, not a bare "
                        "`streamlit run app.py`.")
    try:
        result = subprocess.run(
            ["docker", "compose", "--profile", "jobs", "run", "--rm", "spark"],
            cwd=PROJECT_DIR, capture_output=True, text=True, timeout=300,
        )
    except FileNotFoundError:
        return False, "docker CLI not found in this container."
    except subprocess.TimeoutExpired:
        return False, "Spark job did not finish within 300s."
    output = result.stdout + result.stderr
    return result.returncode == 0, output


# ---------------------------------------------------------------------------
# Data access, cached
# ---------------------------------------------------------------------------
# The client is a cache_resource (one connection, shared, never serialised) while
# the query results are cache_data (serialised per argument). Mixing them up is
# the classic Streamlit mistake: caching the client as data makes Streamlit try
# to pickle a live socket.
@st.cache_resource
def get_es():
    return es_client.connect()


@st.cache_data(ttl=60, show_spinner=False)
def load_tickers():
    return es_client.list_tickers(get_es())


@st.cache_data(ttl=60, show_spinner=False)
def load_status():
    return es_client.index_status(get_es())


@st.cache_data(ttl=60, show_spinner=False)
def load_company(ticker: str):
    """Everything one company needs, in one cached call keyed on the ticker."""
    es = get_es()
    return (es_client.fetch_filings(es, ticker),
            es_client.fetch_prices(es, ticker),
            es_client.fetch_latest_analysis(es, ticker),
            es_client.fetch_latest_context(es, ticker))


@st.cache_resource
def get_kafka_tail():
    """One background consumer thread per Streamlit server process (not per
    session/browser tab) -- see kafka_tail.py for why this reads Kafka
    directly instead of going through Elasticsearch/Spark like everything
    else on this page."""
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    return kafka_tail.start(bootstrap)


# ---------------------------------------------------------------------------
# Sidebar — connection state first, because an empty chart is almost always a
# stage that has not been run rather than a company with no data.
# ---------------------------------------------------------------------------
st.sidebar.title("📊 Pipeline")

try:
    status = load_status()
    connected = True
except Exception as exc:                                    # noqa: BLE001
    connected = False
    st.sidebar.error("Elasticsearch unreachable")
    st.title("📊 Financial KPIs & AI Analyst")
    st.error(f"Cannot reach Elasticsearch at `{es_client.ES_HOST}`.")
    st.markdown(
        "```\n"
        "docker compose up -d                        # start the stack\n"
        "bash scripts/verify_stack.sh                # confirm it is healthy\n"
        "```")
    st.caption(f"Underlying error: `{type(exc).__name__}: {exc}`")
    st.stop()

st.sidebar.caption(f"`{es_client.ES_HOST}`")
for name, count in status.items():
    if count is None:
        st.sidebar.markdown(f"- `{name}` — **missing**")
    else:
        st.sidebar.markdown(f"- `{name}` — {count:,} docs")

if st.sidebar.button("🔄 Refresh data", use_container_width=True,
                     help="Re-run the Spark job so Elasticsearch (and this "
                          "page) picks up everything currently on the Kafka "
                          "topics -- new bars from the live producer, a new "
                          "day's filings, etc."):
    with st.sidebar.status("Running Spark job...", expanded=False) as status_box:
        ok, log = run_spark_refresh()
        if ok:
            status_box.update(label="Done — data refreshed", state="complete")
        else:
            status_box.update(label="Spark job failed", state="error")
            st.sidebar.code(log[-2000:] or "no output", language=None)
    if ok:
        st.cache_data.clear()
        st.rerun()

tickers = load_tickers()
if not tickers:
    st.title("📊 Financial KPIs & AI Analyst")
    st.warning(f"The `{es_client.FILINGS_INDEX}` index holds no filings yet.")
    st.markdown(
        "Load the pipeline first:\n"
        "```\n"
        "docker compose run --rm producer                     # Kafka <- snapshot\n"
        "docker compose --profile jobs run --rm spark         # ES   <- Kafka\n"
        "```")
    st.stop()

st.sidebar.divider()
ticker = st.sidebar.selectbox("Company", tickers,
                              index=tickers.index("AAPL") if "AAPL" in tickers else 0)
years = st.sidebar.slider("Fiscal years", 3, 8, FISCAL_YEARS)

st.sidebar.divider()
live_tick_on = st.sidebar.checkbox("Live price ticker", value=False,
                                   help="Reads market.prices.v1 directly from "
                                        "Kafka via a background consumer -- "
                                        "bypasses Elasticsearch and Spark "
                                        "entirely, so it reflects a bar within "
                                        "seconds of it being produced, not just "
                                        "after the next Refresh data run. Only "
                                        "sees bars produced AFTER the dashboard "
                                        "container started (no history replay).")
live_tick_seconds = st.sidebar.slider("Refresh every (s)", 1, 30, 3,
                                      disabled=not live_tick_on)

# ---------------------------------------------------------------------------
# Compute
# ---------------------------------------------------------------------------
filings, prices, analysis, context = load_company(ticker)
result = kpis.compute_all(filings, prices, years=years)

st.title(f"{ticker} — Financial KPIs")

if live_tick_on:
    st_autorefresh(interval=live_tick_seconds * 1000, key="live_tick_autorefresh")
    tick = get_kafka_tail().get(ticker)
    if tick is None:
        st.info(f"No bar received for {ticker} on Kafka yet since this dashboard "
                f"started. Start a producer (e.g. `producer/live_producer.py` or "
                f"`--mode live`) and the first completed bar will appear here "
                f"within seconds -- no Spark run needed.")
    else:
        age_s = (pd.Timestamp.now(tz="UTC")
                 - pd.Timestamp(tick["ingested_at"], tz="UTC")).total_seconds()
        bar_start_local = (pd.Timestamp(tick["ts"], tz="UTC")
                           .tz_convert(DASHBOARD_TZ).strftime("%H:%M"))
        cols = st.columns(4)
        cols[0].metric("Latest price", f"${tick['close']:,.2f}")
        cols[1].metric("Bar", tick["interval"])
        cols[2].metric(f"Bar start ({DASHBOARD_TZ.key})", bar_start_local)
        cols[3].metric("Produced", f"{age_s:,.0f}s ago")
        st.caption(
            "Read directly off Kafka's `market.prices.v1` by a background "
            "consumer -- no Elasticsearch, no Spark run, in this widget's path.")
    st.divider()

if not result:
    st.warning(f"No annual (FY) filings found for {ticker}. The seven charts are "
               f"built from annual filings; this company has only quarterly data "
               f"in the index.")
    st.stop()

fy = kpis.annual_frame(filings, years=years)
st.caption(f"FY{int(fy.fiscal_year.min())}–FY{int(fy.fiscal_year.max())}")

# ---------------------------------------------------------------------------
# Headline figures — the latest fiscal year, with the change on the year before
# ---------------------------------------------------------------------------
def _delta(kpi, col=None):
    """Latest value and its change on the prior year, for a stat tile."""
    d, c = kpi.data, col or kpi.value_col
    s = d[c].dropna()
    if s.empty:
        return "n/a", None
    latest = charts.fmt_value(s.iloc[-1], kpi.unit)
    if len(s) < 2 or s.iloc[-2] == 0:
        return latest, None
    change = (s.iloc[-1] - s.iloc[-2]) / abs(s.iloc[-2]) * 100
    return latest, f"{change:+.1f}% vs prior year"


tiles = [("Revenue", result["revenue"]), ("Net profit", result["net_profit"]),
         ("Diluted EPS", result["eps"]), ("Debt / equity", result["debt_equity"])]
for col, (label, kpi) in zip(st.columns(len(tiles)), tiles):
    value, delta = _delta(kpi)
    col.metric(label, value, delta, delta_color="normal" if delta else "off")

st.divider()

# ---------------------------------------------------------------------------
# Share price, with a selectable window.
#
# Above the fundamentals on purpose: it is the one series here that moves daily,
# and it is the context a reader wants before reading a five-year metric. It is
# also the only chart on the page drawn against a real time axis.
# ---------------------------------------------------------------------------
st.subheader("Share price")
window = st.radio("Window", list(kpis.PRICE_WINDOWS), index=4, horizontal=True,
                  key=f"pxwin-{ticker}", label_visibility="collapsed")
view = kpis.price_view(prices, window)

if view.available:
    st.plotly_chart(charts.price_chart(view, ticker),
                    use_container_width=True, key=f"{ticker}-price-{window}")
    covered = view.data["date"]
    left, right = st.columns([3, 1])
    # The dates are stated rather than implied. The snapshot is replayed history
    # with a fixed end, so "1W" is the last week *of the data*, not of today —
    # without saying so, a reader checks it against a live quote and concludes
    # the dashboard is broken.
    left.caption(f"{len(view.data)} daily bars · "
                 f"{covered.min():%d %b %Y} – {covered.max():%d %b %Y}")
    if view.last_close is not None:
        right.metric("Last close", f"${view.last_close:,.2f}",
                     None if view.change_pct is None
                     else f"{view.change_pct:+.1f}% over {window}")
    # -----------------------------------------------------------------------
    # Woodies CCI sub-panels, collapsed by default.
    #
    # Behind an expander rather than on the page: it is a 1050px four-row
    # technical chart with nine tunable parameters, which is a different kind of
    # question from the fundamentals below it. Nothing is computed until it is
    # opened — Streamlit does not run the body of a closed expander.
    # -----------------------------------------------------------------------
    with st.expander("See more details — Woodies CCI, Stochastic, MACD"):
        _p = dict(indicators.DEFAULTS)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.caption("**CCI**")
            _p["cci_period"] = st.number_input(
                "CCI period", 2, 100, _p["cci_period"], key=f"cci-{ticker}")
            _p["turbo_period"] = st.number_input(
                "Turbo period", 2, 100, _p["turbo_period"], key=f"turbo-{ticker}")
            _p["trend_bars"] = st.number_input(
                "Trend confirm bars", 2, 30, _p["trend_bars"], key=f"tb-{ticker}")
        with c2:
            st.caption("**Stochastic**")
            _p["stoch_k"] = st.number_input(
                "%K period", 1, 100, _p["stoch_k"], key=f"sk-{ticker}")
            _p["stoch_k_smooth"] = st.number_input(
                "%K smoothing", 1, 50, _p["stoch_k_smooth"], key=f"sks-{ticker}")
            _p["stoch_d"] = st.number_input(
                "%D period", 1, 50, _p["stoch_d"], key=f"sd-{ticker}")
        with c3:
            st.caption("**MACD**")
            _p["macd_fast"] = st.number_input(
                "Fast EMA", 1, 100, _p["macd_fast"], key=f"mf-{ticker}")
            _p["macd_slow"] = st.number_input(
                "Slow EMA", 2, 200, _p["macd_slow"], key=f"ms-{ticker}")
            _p["macd_signal"] = st.number_input(
                "Signal", 1, 100, _p["macd_signal"], key=f"msig-{ticker}")

        _bars = woodies_chart.bars_frame(prices)
        if _bars is None:
            st.info("Price documents carry no OHLC — the sub-panels need "
                    "`open`/`high`/`low`/`close`.")
        else:
            _ind_df, _df, _short = woodies_chart.prepare_frames(
                _bars, view.data["date"], **_p)

            # Readout strip — the Woodies "numbers" line above the sub-panels.
            _fig, _wdf = woodies_chart.build_figure(
                _ind_df, _df, ticker, **_p)
            _last = _wdf.iloc[-1]
            m1, m2, m3, m4 = st.columns(4)
            m1.metric(f"CCI ({int(_p['cci_period'])})", f"{_last['cci']:.2f}")
            m2.metric(f"Turbo ({int(_p['turbo_period'])})", f"{_last['turbo']:.2f}")
            m3.metric("Trend", indicators.trend_label(_last["trend"]),
                      f"{int(_last['trend_count'])} bars")
            m4.metric("Last close", f"{_df['close'].iloc[-1]:.2f}")

            st.plotly_chart(_fig, use_container_width=True,
                            key=f"{ticker}-woodies-{window}")

            if _short:
                # A short warm-up is visible as a blank left edge. Saying so
                # beats letting a reader conclude the indicator is broken.
                st.caption(
                    f"⚠️ {_short} warm-up bar(s) short — the snapshot does not "
                    f"reach far enough before this window, so the leftmost "
                    f"values are still settling. Pick a shorter window for a "
                    f"fully warm chart.")
else:
    st.info(f"No price bars for {ticker} in `{es_client.PRICES_INDEX}`.")

st.divider()

# ---------------------------------------------------------------------------
# The seven charts, two per row
# ---------------------------------------------------------------------------
ORDER = [("revenue", "1. Annual Revenue"), ("buyback", "2. Buyback"),
         ("debt_equity", "3. Debt vs Equity"), ("net_profit", "4. Net Profit"),
         ("cash_flow", "5. Cash Flow"), ("rule_of_40", "6. Rule of 40"),
         ("eps", "7. Earnings per Share")]

for i in range(0, len(ORDER), 2):
    for col, (slug, heading) in zip(st.columns(2), ORDER[i:i + 2]):
        kpi = result[slug]
        with col:
            st.plotly_chart(charts.CHART_BUILDERS[slug](kpi),
                            use_container_width=True, key=f"{ticker}-{slug}")
            # Definition and provenance under every chart. A number nobody can
            # trace back to a named field in a named index is a number nobody
            # can defend when asked where it came from.
            st.caption(f"**{heading}** · {kpi.definition}")

# ---------------------------------------------------------------------------
# AI analyst — the graded AI capability (stage 3).
#
# Grounded on the seven metrics above, which is what separates it from the note
# the Spark job writes into `stock_analysis` (grounded on price anomalies). Both
# are shown, labelled, and allowed to disagree.
#
# Placed BELOW the charts deliberately: the evidence is on screen before anything
# interprets it, and inside the panel the computed numbers and the model's prose
# stay in separate blocks. A reader must never have to guess which of the two
# they are looking at.
# ---------------------------------------------------------------------------
st.divider()
st.header("🏠 Our Home Analyst — grounded on the seven metrics above")

ai_cfg = ai_analyst.provider_config()
ai_can_call, ai_why = ai_analyst.availability()

evidence = ai_analyst.build_evidence(ticker, result, years, context=context)

# One optional instruction from the reader, appended to the prompt as an emphasis
# note. Scoped to wording on purpose — the rules it sits beneath are what keep the
# model from reading a stock split as a share issue or a missing fact as a zero,
# so they are not the reader's to overwrite. `ai_analyst.FOCUS_SECTION` says as
# much to the model rather than trusting it to infer the boundary.
focus = st.text_input(
    "Emphasis for the analyst (optional)",
    key=f"focus-{ticker}",
    max_chars=ai_analyst.FOCUS_MAX_CHARS,
    placeholder="e.g. focus on leverage and cash generation · explain it for a "
                "non-specialist · weigh the share count trend most heavily",
    help="Appended to the prompt as an emphasis note. It changes what the "
         "analyst stresses and how it words the note. It cannot change the "
         "seven metrics, the rules for reading them, or the JSON contract — "
         "and it cannot add data the model was not given.")

try:
    ai_prompt = ai_analyst.build_prompt(evidence, focus=focus)
except ai_analyst.AnalystError as exc:
    ai_prompt = None
    st.error(f"Cannot assemble the prompt: {exc}")

if ai_prompt:
    st.caption(f"{len(evidence['metrics'])} metrics")

    # --- the facts half: exactly the numbers that go into the prompt ----------
    with st.container(border=True):
        fact_rows = []
        for slug, metric in evidence["metrics"].items():
            rows = metric["by_fiscal_year"]
            kpi = result[slug]
            if rows:
                latest = rows[-1]
                raw = latest.get(kpi.value_col)
                fact_rows.append({
                    "Metric": slug,
                    "Latest FY": str(latest["fiscal_year"]),
                    "Value": (charts.fmt_value(raw, kpi.unit)
                              if raw is not None else "not reported"),
                    "Years supplied": len(rows),
                    "Gap notes": len(metric["data_gaps"]),
                })
            else:
                fact_rows.append({"Metric": slug, "Latest FY": "—",
                                  "Value": "no usable data", "Years supplied": 0,
                                  "Gap notes": len(metric["data_gaps"])})
        st.dataframe(pd.DataFrame(fact_rows), hide_index=True,
                     use_container_width=True)

    # --- the interpretation half ---------------------------------------------
    digest = hashlib.sha256(ai_prompt.encode("utf-8")).hexdigest()[:12]
    state_key = f"ai::{ticker}::{years}::{digest}"
    held = st.session_state.get(state_key)
    succeeded = isinstance(held, dict) and "error" not in held

    if not ai_can_call:
        st.info(f"**No model will be called.** {ai_why}")
    else:
        if st.button(f"Ask Our Home Analyst about {ticker}", type="primary",
                     key=f"btn-{state_key}", disabled=succeeded):
            with st.spinner(f"Asking {ai_cfg['provider']} ({ai_cfg['model']}) …"):
                try:
                    st.session_state[state_key] = ai_analyst.analyse(
                        evidence, focus=focus)
                except ai_analyst.RateLimited as exc:
                    msg = str(exc)
                    if exc.retry_after:
                        msg += f" It asked for {exc.retry_after:.0f}s before retrying."
                    st.session_state[state_key] = {"error": msg}
                except ai_analyst.AnalystError as exc:
                    st.session_state[state_key] = {
                        "error": f"{type(exc).__name__}: {exc}"}
            held = st.session_state.get(state_key)
            succeeded = isinstance(held, dict) and "error" not in held

            # Persist it. Without this the note lives only in this browser
            # session: `stock_analysis` stays empty, the next reader sees
            # nothing, and nobody can query recommendations across companies.
            # Keyed exactly as the Spark stage keys its own, so asking twice in
            # a day overwrites rather than accumulating.
            if succeeded:
                try:
                    doc_id = es_client.write_analysis(
                        get_es(), ticker, held,
                        provider=ai_cfg["provider"], model=ai_cfg["model"],
                        source="home_analyst")
                    st.session_state[f"{state_key}::saved"] = doc_id
                except Exception as exc:                        # noqa: BLE001
                    # A failed write must not be reported as a success, but it
                    # must not throw away the answer either — the reader can
                    # still see it, they just know it was not stored.
                    st.session_state[f"{state_key}::saved"] = None
                    st.warning(
                        f"The note was generated but could not be saved to "
                        f"`{es_client.ANALYSIS_INDEX}`: "
                        f"{type(exc).__name__}: {exc}")

        saved_id = st.session_state.get(f"{state_key}::saved")
        if succeeded and saved_id:
            st.caption(f"Saved to `{es_client.ANALYSIS_INDEX}` as `{saved_id}` — "
                       f"it will still be here on the next visit, and is "
                       f"queryable alongside every other company's.")
        # One click is one call, on purpose. Streamlit re-runs this whole script
        # on every widget change, so calling automatically would fire a request
        # for each nudge of the year slider — and Gemini's free tier is ~30 calls
        # a day. The answer is cached against this exact prompt, so re-reads and
        # unrelated interactions cost nothing.
        st.caption("One click is one API call. Not automatic: Streamlit re-runs "
                   "the page on every widget change, and the free tier is about "
                   "30 calls a day. The answer is kept for this company and these "
                   "inputs, so re-reading it is free.")

    if isinstance(held, dict) and "error" in held:
        st.error(held["error"])
        st.caption("Nothing above this panel is affected — every chart is computed "
                   "locally and needs no API.")
    elif succeeded:
        with st.container(border=True):
            st.markdown("**Model interpretation — generated text, not a computed "
                        "figure.**")
            badge = {"buy": "🟢 BUY", "hold": "🟡 HOLD", "sell": "🔴 SELL"}
            c1, c2, c3 = st.columns([1, 1, 2])
            c1.metric("Recommendation",
                      badge.get(held["recommendation"],
                                held["recommendation"].upper()))
            c2.metric("Confidence", held["confidence"].capitalize())
            c3.caption(f"Written by `{held.get('model_used')}` via "
                       f"`{held.get('provider_used')}`. Confidence is the model's "
                       f"own claim about how well the data supports the direction "
                       f"— it is not a computed score.")

            # The instruction that shaped this answer is shown beside it. An
            # emphasised note read without knowing what it was asked to emphasise
            # invites the reader to mistake a slant they requested for a finding.
            if held.get("focus_used"):
                st.caption(f"✏️ Emphasis requested: *{held['focus_used']}*")

            st.markdown(md(held["summary"]))

            s1, s2 = st.columns(2)
            with s1:
                st.markdown("**Signals**")
                for item in held["signals"] or ["(none given)"]:
                    st.markdown(f"- {md(item)}")
            with s2:
                st.markdown("**Key risks**")
                for item in held["key_risks"] or ["(none given)"]:
                    st.markdown(f"- {md(item)}")

        # Inspectability, borrowed from the Spark stage: the prompt is always
        # available, so an answer that looks wrong can be traced to what was
        # actually sent rather than to what someone assumes was sent.
        with st.expander("Exactly what the model was sent"):
            st.caption("The full prompt string, verbatim. The `Data` section is "
                       "the JSON below it.")
            st.code(ai_prompt, language="markdown")
            st.caption("The evidence object on its own:")
            st.json(evidence)
    elif ai_can_call:
        with st.expander("What would be sent, before spending a call"):
            st.code(ai_prompt, language="markdown")
    else:
        with st.expander("What would be sent, if a model were reachable"):
            st.code(ai_prompt, language="markdown")

# ---------------------------------------------------------------------------
# The second analyst: price behaviour and the MLlib anomalies.
#
# Same question, deliberately different evidence -- the trading days KMeans
# flagged, not the fundamentals above -- so the two are allowed to disagree.
# On demand like the first: one click, one call.
# ---------------------------------------------------------------------------
st.divider()
st.header("🤖 AI Analyst")

# The evidence this analyst reasons over is deliberately not laid out above the
# input. It reads the full stage-3 context -- price behaviour, the trading days
# MLlib flagged, the latest filing facts and the press-release excerpt -- and the
# note it writes says as much. Listing the anomaly counts here framed it as an
# anomaly tool, which undersells what it is actually given.
if not context:
    st.info(f"Nothing to analyse for {ticker} yet.")
else:
    an_focus = st.text_input(
        "Ask the analyst something (optional)",
        key=f"anfocus-{ticker}", max_chars=ai_analyst.FOCUS_MAX_CHARS,
        placeholder="e.g. what stands out about the last year · explain the "
                    "biggest moves · how healthy is the balance sheet")

    an_key = f"anom::{ticker}::{context.get('as_of')}"
    an_held = st.session_state.get(an_key)
    an_ok = isinstance(an_held, dict) and "error" not in an_held

    if not ai_can_call:
        st.info(f"**No model will be called.** {ai_why}")
    else:
        if st.button(f"Ask the AI Analyst about {ticker}", type="primary",
                     key=f"anbtn-{an_key}", disabled=an_ok):
            with st.spinner(f"Asking {ai_cfg['provider']} …"):
                try:
                    st.session_state[an_key] = ai_analyst.analyse_anomalies(
                        context, focus=an_focus)
                except ai_analyst.AnalystError as exc:
                    st.session_state[an_key] = {
                        "error": f"{type(exc).__name__}: {exc}"}
            an_held = st.session_state.get(an_key)
            an_ok = isinstance(an_held, dict) and "error" not in an_held
            if an_ok:
                try:
                    es_client.write_analysis(
                        get_es(), ticker, an_held,
                        provider=ai_cfg["provider"], model=ai_cfg["model"],
                        source="ai_analyst")
                except Exception as exc:                        # noqa: BLE001
                    st.warning(f"Generated but not saved: "
                               f"{type(exc).__name__}: {exc}")

    if an_held and "error" in an_held:
        st.error(an_held["error"])
    elif an_ok:
        with st.container(border=True):
            badge = {"buy": "🟢 BUY", "hold": "🟡 HOLD", "sell": "🔴 SELL"}
            rec = str(an_held.get("recommendation", "")).lower()
            d1, d2 = st.columns([1, 1])
            d1.metric("Recommendation", badge.get(rec, rec.upper() or "—"))
            d2.metric("Confidence",
                      str(an_held.get("confidence", "—")).capitalize())
            flagged = context.get("anomaly_count")
            near = context.get("anomalies_near_filing")
            st.caption(
                "Considered price behaviour over "
                f"{context.get('bar_count', '—')} trading days"
                + (f", including {flagged} statistically unusual ones"
                   f"{f' ({near} close to a filing date)' if near else ''}"
                   if flagged else "")
                + ", the latest reported financials, and the most recent "
                  "filing text.")
            if an_held.get("focus_used"):
                st.caption(f"Asked to emphasise: _{an_held['focus_used']}_")
            if an_held.get("summary"):
                st.markdown(md(an_held["summary"]))
            for label, field_name in (("Signals", "signals"),
                                      ("Key risks", "key_risks")):
                items = an_held.get(field_name) or []
                if items:
                    st.markdown(f"**{label}**")
                    for item in items:
                        st.markdown(f"- {md(item)}")
    # Deliberately nothing before the first click. A saved note from an earlier
    # run used to render on page load, which reads as though the model had
    # already been asked -- and in a demo it is indistinguishable from a live
    # answer. The note is still in `stock_analysis`; it is just not shown here
    # until this session asks for one.

# ---------------------------------------------------------------------------
# The underlying rows, so any chart can be checked against its own inputs
# ---------------------------------------------------------------------------
st.divider()
with st.expander("Underlying annual filings (the exact rows behind every chart)"):
    show = ["fiscal_year", "form_type", "period_end", "filed_date", "accession_no",
            "revenue", "net_income", "eps_diluted", "eps_basic", "equity",
            "liabilities", "operating_cash_flow", "capex", "free_cash_flow",
            "revenue_yoy", "net_margin"]
    st.dataframe(fy[[c for c in show if c in fy.columns]]
                 .sort_values("fiscal_year", ascending=False),
                 use_container_width=True, hide_index=True)
    st.caption(f"Index `{es_client.FILINGS_INDEX}`, `_id` = accession_no. "
               f"Blank cells are facts the company did not report — "
               f"`es_writer._clean()` omits null fields from the document.")

st.divider()
