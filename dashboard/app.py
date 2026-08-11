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

import pandas as pd
import streamlit as st

import ai_analyst
import charts
import es_client
import kpis

st.set_page_config(page_title="Financial KPIs & AI Analyst",
                   page_icon="📊", layout="wide")

FISCAL_YEARS = int(os.getenv("DASHBOARD_YEARS") or 5)


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

# ---------------------------------------------------------------------------
# Compute
# ---------------------------------------------------------------------------
filings, prices, analysis, context = load_company(ticker)
result = kpis.compute_all(filings, prices, years=years)

st.title(f"{ticker} — Financial KPIs")

if not result:
    st.warning(f"No annual (FY) filings found for {ticker}. The seven charts are "
               f"built from annual filings; this company has only quarterly data "
               f"in the index.")
    st.stop()

fy = kpis.annual_frame(filings, years=years)
st.caption(
    f"{len(fy)} annual filings · FY{int(fy.fiscal_year.min())}–"
    f"FY{int(fy.fiscal_year.max())} · every value read from "
    f"`{es_client.FILINGS_INDEX}` and `{es_client.PRICES_INDEX}`")

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
    left.caption(
        f"{len(view.data)} daily bars · "
        f"{covered.min():%d %b %Y} – {covered.max():%d %b %Y} · "
        f"window measured back from the most recent bar in `"
        f"{es_client.PRICES_INDEX}`, not from today")
    if view.last_close is not None:
        right.metric("Last close", f"${view.last_close:,.2f}",
                     None if view.change_pct is None
                     else f"{view.change_pct:+.1f}% over {window}")
    for note in view.notes:
        st.caption(f"⚠️ {note}")
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
            st.caption("Source: " + " · ".join(f"`{f}`" for f in kpi.source_fields))
            if kpi.notes:
                with st.expander(f"⚠️ {len(kpi.notes)} note(s) on missing data"):
                    for note in kpi.notes:
                        st.markdown(f"- {note}")

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
st.header("🤖 AI Analyst — grounded on the seven metrics above")

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
    st.caption(
        f"Provider `{ai_cfg['provider']}` · model `{ai_cfg['model']}` · "
        f"{len(evidence['metrics'])} metrics · {len(ai_prompt):,} character prompt"
        + (" · enriched with the MLlib anomaly summary from `stock_context`"
           if "price_anomaly_context" in evidence else
           " · no `stock_context` yet, so fundamentals only"))

    # --- the facts half: exactly the numbers that go into the prompt ----------
    with st.container(border=True):
        st.markdown("**Computed facts — no model involved.** "
                    "The latest fiscal year of each metric handed to the model.")
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
        if st.button(f"Generate recommendation for {ticker}", type="primary",
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

            st.markdown(held["summary"])

            s1, s2 = st.columns(2)
            with s1:
                st.markdown("**Signals**")
                for item in held["signals"] or ["(none given)"]:
                    st.markdown(f"- {item}")
            with s2:
                st.markdown("**Key risks**")
                for item in held["key_risks"] or ["(none given)"]:
                    st.markdown(f"- {item}")

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

# --- the other analyst, kept visibly separate -------------------------------
st.subheader("Spark's note — the same question from price behaviour")
if analysis:
    with st.container(border=True):
        st.markdown("**A different analyst, on different evidence.** Written "
                    "inside the Spark job from the trading days MLlib's KMeans "
                    "flagged as unusual, not from the fundamentals above. It may "
                    "disagree with the panel above; that is informative, not a bug.")
        badge = {"buy": "🟢 BUY", "hold": "🟡 HOLD", "sell": "🔴 SELL"}
        rec = str(analysis.get("recommendation", "")).lower()
        d1, d2, d3 = st.columns([1, 1, 2])
        d1.metric("Recommendation", badge.get(rec, rec.upper() or "—"))
        d2.metric("Confidence",
                  str(analysis.get("confidence", "—")).capitalize())
        d3.caption(f"Index `{es_client.ANALYSIS_INDEX}` · as of "
                   f"`{analysis.get('as_of', 'unknown')}` · produced by "
                   f"`{analysis.get('provider_used', 'unknown')}`")
        if analysis.get("summary"):
            st.markdown(analysis["summary"])
        for label, field_name in (("Signals", "signals"),
                                  ("Key risks", "key_risks")):
            items = analysis.get(field_name) or []
            if items:
                st.markdown(f"**{label}**")
                for item in items:
                    st.markdown(f"- {item}")
else:
    st.info(f"The `{es_client.ANALYSIS_INDEX}` index holds no note for {ticker}. "
            "That is a supported state, not an error: the Spark analyst stage is "
            "skipped when `LLM_ENABLED=false` or no key is set. Run "
            "`docker compose --profile jobs run --rm spark` to produce one.")

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
st.caption("Educational demonstration for a university Big Data course. "
           "Not financial advice.")
