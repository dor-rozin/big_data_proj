"""
LLM client for the analyst stage. Providers: gemini, groq.

Only the transport differs between them — the prompt, the context, the JSON
contract, the pacing and the retry policy are shared, so adding a provider is a
request shape and a response path, not a second pipeline.

Deliberately not a Spark UDF. A UDF would run this on the executors, once per
row, with no shared rate limiting and no useful retry control — and a failed
task would re-run the whole partition, re-issuing calls that already succeeded
and already cost quota. Instead the pipeline aggregates first, collects a small
number of rows to the driver, and this module makes the calls from there with a
bounded worker pool.

Everything that bounds cost lives here: the call cap, the concurrency limit, the
context truncation and the backoff. All of them are configurable, and any
truncation is logged rather than applied silently — a capped run that reports
full coverage is worse than one that fails.
"""
import json
import os
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

GEMINI_ROOT = "https://generativelanguage.googleapis.com/v1beta/models"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
OLLAMA_URL = "http://ollama:11434"

# Transient conditions worth retrying: rate limit, and the 5xx family.
RETRY_STATUS = {429, 500, 502, 503, 504}

# A 429 is not a "try again shortly" condition like a 503 — the free tier
# allows a fixed number of requests per window, so backing off by a couple of
# seconds just burns another attempt against the same exhausted quota. Gemini
# usually returns a RetryInfo telling us how long the window has left; this is
# the floor used when it does not.
RATE_LIMIT_BACKOFF = 30.0

# Hard ceiling on any single wait, including one the server asked for.
#
# Providers can return a retry-after measured in minutes or hours when a longer
# window (daily quota, token-per-minute budget) is exhausted. Honouring that
# literally makes the job sit idle for that long with nothing on the console —
# indistinguishable from a hang, and fatal in a demo. Past this ceiling the
# right answer is to fail the instrument, record why, and let the run finish:
# nine analyses and one recorded failure beats an hour of silence.
MAX_BACKOFF = 60.0

# Consecutive per-row failures before a provider is retired for the run.
#
# Not every exhausted provider announces itself with one long retry-after.
# Gemini's per-minute limit returns delays of 25-46s — under MAX_BACKOFF, so
# each row dutifully retries four times and fails anyway. Ten rows then cost
# twenty minutes to produce nothing. Three consecutive failures is enough
# evidence that the provider is not serving right now; move to the fallback and
# stop paying for the discovery.
#
# Counted consecutively, not cumulatively, so a provider that recovers resets.
MAX_CONSECUTIVE_FAILURES = 2

# Retries are for when there is nowhere else to go.
#
# With a fallback configured, grinding through five attempts against a provider
# that is already refusing is strictly worse than handing the row to the next
# provider immediately: same outcome, minutes later. When this provider IS the
# last resort, retrying hard is the only option left and the budget is generous.
RETRIES_WITH_FALLBACK = 2
RETRIES_LAST_RESORT = 5


class GeminiError(RuntimeError):
    """A call failed. Retrying the same provider may still be worthwhile."""


class ProviderUnavailable(GeminiError):
    """This provider cannot serve *any* row for the rest of this run.

    The distinction matters for cost. An exhausted daily budget, a missing key
    or a refused connection will fail identically for every remaining
    instrument, so re-discovering that ten times wastes the run. Raising this
    marks the provider dead for the run and sends the remaining work straight to
    the fallback. A malformed response or a one-off 500 is *not* this — those are
    per-row problems and the provider stays in play.
    """


class Pacer:
    """Space calls at least `min_interval` seconds apart, across all threads.

    The free tier's limit is low enough that concurrency is actively harmful:
    four workers firing at once exhaust a 5-request window instantly and then
    all four retry into the same wall. Serialising with a deliberate gap turns a
    burst of failures into a slower run that finishes.
    """

    def __init__(self, min_interval):
        self._min_interval = max(0.0, min_interval)
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self):
        if not self._min_interval:
            return
        with self._lock:
            now = time.monotonic()
            sleep_for = max(0.0, self._next_at - now)
            self._next_at = max(now, self._next_at) + self._min_interval
        if sleep_for:
            time.sleep(sleep_for)


def _float_or_none(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _retry_delay_from(body):
    """Pull the server's own RetryInfo out of a 429 body, when present."""
    try:
        for detail in json.loads(body).get("error", {}).get("details", []):
            delay = detail.get("retryDelay")
            if delay and delay.endswith("s"):
                return float(delay[:-1])
    except (ValueError, AttributeError, TypeError):
        pass
    return None


# ---------------------------------------------------------------------------
# Prompt assembly
# ---------------------------------------------------------------------------
def load_prompt(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def build_context(row, max_text_chars=6000):
    """Turn one aggregated Spark Row into the dict the prompt embeds.

    Truncation is deterministic (a prefix of the filing text, and the top-N
    anomalies already bounded upstream) so the same input always produces the
    same prompt. A run that silently varied its context would make the output
    impossible to reason about.
    """
    d = row.asDict(recursive=True)

    text = d.get("filing_text")
    truncated = False
    if text and len(text) > max_text_chars:
        text = text[:max_text_chars]
        truncated = True

    context = {
        "ticker": d.get("ticker"),
        "interval": d.get("interval"),
        "price_history": {
            "bars": d.get("bar_count"),
            "from": _s(d.get("first_date")),
            "to": _s(d.get("last_date")),
            "latest_close": _r(d.get("latest_close"), 2),
            "latest_ma_30": _r(d.get("latest_ma_30"), 2),
            "avg_daily_return": _r(d.get("avg_return"), 5),
            "return_volatility": _r(d.get("return_volatility"), 5),
            "avg_volume": _r(d.get("avg_volume"), 0),
        },
        "anomalies": {
            "total_flagged": d.get("anomaly_count"),
            "near_a_filing_date": d.get("anomalies_near_filing"),
            "most_extreme": d.get("top_anomalies") or [],
        },
        "latest_filing": {
            "form_type": d.get("latest_form_type"),
            "fiscal_period": d.get("latest_fiscal_period"),
            "filed_date": d.get("latest_filed_date"),
            "revenue": d.get("revenue"),
            "net_income": d.get("net_income"),
            "eps_diluted": d.get("eps_diluted"),
            "gross_margin": _r(d.get("gross_margin"), 4),
            "net_margin": _r(d.get("net_margin"), 4),
            "return_on_equity": _r(d.get("return_on_equity"), 4),
            "current_ratio": _r(d.get("current_ratio"), 3),
            "debt_to_equity": _r(d.get("debt_to_equity"), 3),
            "revenue_yoy": _r(d.get("revenue_yoy"), 4),
            "net_income_yoy": _r(d.get("net_income_yoy"), 4),
        },
        "filing_text": {
            "available": bool(text),
            "section": d.get("filing_text_section"),
            "title": d.get("filing_text_title"),
            "filed_date": d.get("filing_text_date"),
            "truncated": truncated,
            "text": text,
        },
    }
    return context


def build_prompt(row, prompt_template, max_text_chars=6000):
    """Return (context dict, context JSON, the literal prompt string).

    The prompt is assembled here and nowhere else, so what gets dumped to disk
    and indexed for inspection is byte-identical to what is sent to the API —
    an inspection artifact that is merely *similar* to the real request is worse
    than none, because it invites debugging the wrong text.
    """
    context = build_context(row, max_text_chars=max_text_chars)
    context_json = json.dumps(context, indent=2, default=str)
    return context, context_json, prompt_template.replace("{{DATA}}", context_json)


def dump_prompts(rows, prompt_template, out_dir, max_text_chars=6000):
    """Write the exact prompt for every row, and return the contexts.

    Costs no API quota, so it runs whether or not the analyst stage does. That
    is the point: the free tier is limited enough that being able to see what
    *would* be sent, without spending a call to find out, is what makes the
    prompt iterable.
    """
    os.makedirs(out_dir, exist_ok=True)
    contexts = []
    for row in rows:
        context, context_json, prompt = build_prompt(
            row, prompt_template, max_text_chars=max_text_chars)
        path = os.path.join(out_dir, f"{row['ticker']}.txt")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(prompt)
        contexts.append({
            "ticker": row["ticker"],
            "interval": row["interval"],
            "context": context,
            "context_json": context_json,
            "prompt_chars": len(prompt),
        })
    print(f"[llm] dumped {len(contexts)} prompt(s) to {out_dir}/")
    return contexts


def _s(v):
    return None if v is None else str(v)


def _r(v, places):
    try:
        return round(float(v), places)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------
# urllib's default User-Agent is `Python-urllib/3.x`, which Cloudflare rejects
# outright in front of Groq — a 403 with body `error code: 1010`, which reads
# like an auth or quota problem and is neither. Sending a real one is the fix.
USER_AGENT = "big-data-proj-spark/1.0"


def _post(url, headers, payload, timeout):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json",
                 "User-Agent": USER_AGENT, **headers},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# --- providers -------------------------------------------------------------
# Only the transport differs. The prompt, the context, the JSON contract, the
# pacing and the retry policy are shared, so adding a provider is a request
# shape and a response path — not a second pipeline.

def _gemini_request(prompt, model, api_key):
    return (
        f"{GEMINI_ROOT}/{model}:generateContent",
        {"x-goog-api-key": api_key},
        {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,      # analysis, not creative writing
                "responseMimeType": "application/json",
            },
        },
    )


def _gemini_extract(data):
    try:
        return data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError) as exc:
        raise GeminiError(f"no text in response: {json.dumps(data)[:400]}") from exc


def _groq_request(prompt, model, api_key):
    """Groq speaks the OpenAI chat-completions dialect."""
    return (
        GROQ_URL,
        {"Authorization": f"Bearer {api_key}"},
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
        },
    )


def _groq_extract(data):
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise GeminiError(f"no text in response: {json.dumps(data)[:400]}") from exc


def _ollama_request(prompt, model, api_key, base_url=None):
    """Ollama exposes an OpenAI-compatible endpoint, so this mirrors Groq.

    No key: it is a container on the compose network, not a hosted service.
    That is the whole point of having it — it cannot run out of quota.
    """
    return (
        f"{(base_url or OLLAMA_URL).rstrip('/')}/v1/chat/completions",
        {},
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2,
            "response_format": {"type": "json_object"},
            "stream": False,
        },
    )


PROVIDERS = {
    "gemini": (_gemini_request, _gemini_extract),
    "groq": (_groq_request, _groq_extract),
    "ollama": (_ollama_request, _groq_extract),   # same response shape as Groq
}


def call_llm(prompt, model, api_key, provider="gemini", timeout=90,
             max_retries=5, backoff_base=2.0, pacer=None, label="",
             base_url=None):
    """One completion call, with retries on transient failures.

    A 4xx that is not 429 is a request problem — a bad key, an unknown model, a
    malformed payload — and retrying it just burns time to fail identically, so
    those raise immediately.
    """
    try:
        build, extract = PROVIDERS[provider]
    except KeyError:
        raise GeminiError(
            f"unknown LLM_PROVIDER {provider!r}; expected one of "
            f"{sorted(PROVIDERS)}") from None

    url, headers, payload = (build(prompt, model, api_key, base_url=base_url)
                             if provider == "ollama"
                             else build(prompt, model, api_key))

    last = None
    for attempt in range(max_retries):
        if pacer:
            pacer.wait()
        try:
            data = _post(url, headers, payload, timeout)
            return extract(data)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            last = GeminiError(f"HTTP {exc.code}: {body[:300]}")
            if exc.code in (401, 403):
                # Bad or missing credentials. Every remaining row fails the same
                # way, so stop asking.
                raise ProviderUnavailable(
                    f"{provider}: auth rejected (HTTP {exc.code})") from exc
            if exc.code not in RETRY_STATUS:
                raise last from exc
            if exc.code == 429:
                header_hint = exc.headers.get("retry-after") if exc.headers else None
                delay = (_retry_delay_from(body)
                         or _float_or_none(header_hint)
                         or RATE_LIMIT_BACKOFF)
            else:
                delay = backoff_base ** attempt
        except urllib.error.URLError as exc:
            # Not every URLError means the provider is gone. A refused
            # connection or a DNS failure does — nothing is listening, and that
            # will not change mid-run (Ollama not started, a typo'd host). But a
            # dropped TLS handshake or a reset connection is ordinary internet
            # turbulence, and retiring a working provider over one of those
            # sends the rest of the run to a slower fallback for no reason.
            # Observed in practice: a single `SSL: UNEXPECTED_EOF_WHILE_READING`
            # retired Groq mid-run while it was perfectly healthy.
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (ConnectionRefusedError, socket.gaierror)):
                raise ProviderUnavailable(
                    f"{provider}: unreachable at {url} ({reason})") from exc
            last = GeminiError(f"{type(reason).__name__}: {reason}")
            delay = backoff_base ** attempt
        except (TimeoutError, json.JSONDecodeError) as exc:
            last = GeminiError(f"{type(exc).__name__}: {exc}")
            delay = backoff_base ** attempt

        if delay > MAX_BACKOFF:
            # A wait this long means a longer window is exhausted (a daily token
            # or request budget), not a momentary burst. Every remaining row
            # would hit the same wall, so retire the provider for this run and
            # let the fallback take over.
            raise ProviderUnavailable(
                f"{provider}: budget exhausted - asked for a {delay:.0f}s wait, "
                f"above the {MAX_BACKOFF:.0f}s ceiling. {last}")

        if attempt < max_retries - 1:
            reason = "rate limited" if "429" in str(last) else "transient error"
            print(f"[llm] {label}{reason}, waiting {delay:.0f}s "
                  f"(attempt {attempt + 1}/{max_retries - 1})", flush=True)
            time.sleep(delay)

    raise last


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------
REQUIRED_KEYS = {"recommendation", "confidence", "key_risks", "signals", "summary"}
VALID_RECOMMENDATION = {"buy", "hold", "sell"}
VALID_CONFIDENCE = {"low", "medium", "high"}


def parse_analysis(raw):
    """Parse the model's JSON, tolerating a markdown fence around it.

    `responseMimeType: application/json` asks for bare JSON, but a fenced block
    is a common enough deviation that stripping it is cheaper than failing a
    whole ticker over formatting.
    """
    text = raw.strip()
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", text, re.S)
    if fence:
        text = fence.group(1)

    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError(f"expected a JSON object, got {type(obj).__name__}")

    missing = REQUIRED_KEYS - obj.keys()
    if missing:
        raise ValueError(f"missing keys: {sorted(missing)}")

    rec = str(obj["recommendation"]).strip().lower()
    conf = str(obj["confidence"]).strip().lower()
    if rec not in VALID_RECOMMENDATION:
        raise ValueError(f"recommendation {rec!r} not in {sorted(VALID_RECOMMENDATION)}")
    if conf not in VALID_CONFIDENCE:
        raise ValueError(f"confidence {conf!r} not in {sorted(VALID_CONFIDENCE)}")

    return {
        "recommendation": rec,
        "confidence": conf,
        "key_risks": [str(x) for x in obj.get("key_risks") or []],
        "signals": [str(x) for x in obj.get("signals") or []],
        "summary": str(obj["summary"]),
    }


# ---------------------------------------------------------------------------
# Driver-side orchestration
# ---------------------------------------------------------------------------
def analyse_rows(rows, prompt_template, providers, max_calls=0,
                 max_text_chars=6000, concurrency=1):
    """Run the analyst prompt over aggregated rows, falling back down a chain.

    `providers` is an ordered list of dicts:
        {name, api_key, model, min_interval, timeout, base_url}

    Each instrument tries the first available provider; if that call fails it
    tries the next. A provider that raises `ProviderUnavailable` — exhausted
    budget, bad credentials, unreachable host — is retired for the rest of the
    run, so the remaining instruments skip straight past it instead of
    rediscovering the same wall ten times.

    Every result records `provider_used` and `model_used`. A note written by a
    3B local model and one written by a 70B hosted model are not equivalent, and
    the output should never leave that ambiguous.
    """
    rows = list(rows)
    if max_calls and len(rows) > max_calls:
        dropped = [r["ticker"] for r in rows[max_calls:]]
        print(f"[llm] LLM_MAX_CALLS={max_calls} reached; analysing "
              f"{max_calls}/{len(rows)} rows. Not analysed: {', '.join(dropped)}",
              flush=True)
        rows = rows[:max_calls]

    pacers = {p["name"]: Pacer(p.get("min_interval", 1.0)) for p in providers}
    dead = {}                      # provider name -> why it was retired
    streak = {p["name"]: 0 for p in providers}   # consecutive failures
    lock = threading.Lock()
    done = [0]
    total = len(rows)

    def one(row):
        ticker = row["ticker"]
        _, _, prompt = build_prompt(row, prompt_template,
                                    max_text_chars=max_text_chars)
        attempts = []

        for idx, p in enumerate(providers):
            name = p["name"]
            with lock:
                if name in dead:
                    attempts.append(f"{name}: skipped ({dead[name]})")
                    continue
            if not p.get("api_key") and name != "ollama":
                with lock:
                    dead.setdefault(name, "no API key configured")
                attempts.append(f"{name}: no API key")
                continue

            # Is anything left after this provider? That decides how hard to
            # retry before moving on.
            with lock:
                has_fallback = any(q["name"] not in dead
                                   for q in providers[idx + 1:])
            try:
                raw = call_llm(prompt, p["model"], p.get("api_key", ""),
                               provider=name, pacer=pacers[name],
                               timeout=p.get("timeout", 90),
                               base_url=p.get("base_url"),
                               max_retries=(RETRIES_WITH_FALLBACK if has_fallback
                                            else RETRIES_LAST_RESORT),
                               label=f"{ticker} [{name}]: ")
                parsed = parse_analysis(raw)
                with lock:
                    streak[name] = 0
                    done[0] += 1
                    print(f"[llm] ({done[0]}/{total}) {ticker}: "
                          f"{parsed['recommendation']} "
                          f"(confidence {parsed['confidence']}) "
                          f"via {name}/{p['model']}", flush=True)
                return {"ticker": ticker, "interval": row["interval"],
                        "provider_used": name, "model_used": p["model"],
                        "error": None, **parsed}

            except ProviderUnavailable as exc:
                with lock:
                    if name not in dead:
                        dead[name] = str(exc)
                        nxt = next((q["name"] for q in providers
                                    if q["name"] not in dead), None)
                        print(f"[llm] {name} retired for this run: {exc}",
                              flush=True)
                        print(f"[llm] falling back to: {nxt or 'nothing left'}",
                              flush=True)
                attempts.append(f"{name}: {exc}")

            except Exception as exc:                  # noqa: BLE001 - recorded
                # A row-level problem (malformed JSON, a one-off 5xx). Try the
                # next provider for this row, but keep this one in play — unless
                # it keeps failing, which is the same as being unavailable.
                attempts.append(f"{name}: {type(exc).__name__}: {exc}")
                with lock:
                    streak[name] += 1
                    retire = (streak[name] >= MAX_CONSECUTIVE_FAILURES
                              and name not in dead)
                    if retire:
                        dead[name] = (f"{MAX_CONSECUTIVE_FAILURES} consecutive "
                                      f"failures; last: {exc}")
                        nxt = next((q["name"] for q in providers
                                    if q["name"] not in dead), None)
                print(f"[llm] {ticker}: {name} failed ({type(exc).__name__}: "
                      f"{exc}); trying next provider", flush=True)
                if retire:
                    print(f"[llm] {name} retired for this run after "
                          f"{MAX_CONSECUTIVE_FAILURES} consecutive failures",
                          flush=True)
                    print(f"[llm] falling back to: {nxt or 'nothing left'}",
                          flush=True)

        with lock:
            done[0] += 1
            print(f"[llm] ({done[0]}/{total}) {ticker}: FAILED on all "
                  f"{len(providers)} provider(s)", flush=True)
        return {"ticker": ticker, "interval": row["interval"],
                "provider_used": None, "model_used": None,
                "recommendation": None, "confidence": None,
                "key_risks": [], "signals": [], "summary": None,
                "error": " | ".join(attempts)}

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        results = list(pool.map(one, rows))

    used = {}
    for r in results:
        if r.get("provider_used"):
            used[r["provider_used"]] = used.get(r["provider_used"], 0) + 1
    if used:
        print("[llm] produced by: "
              + ", ".join(f"{k} x{v}" for k, v in sorted(used.items())),
              flush=True)
    return results


def write_reports(results, out_dir):
    """Write one .txt analyst note per ticker alongside the Elasticsearch load.

    The directory always reflects *this* run. A ticker whose call failed has any
    previous note removed rather than left in place: a stale file carries no
    marker saying so, and a note dated from an earlier run sitting next to fresh
    ones is indistinguishable from current output. Removing it is the honest
    outcome — the failure is already in the log and on the console.
    """
    os.makedirs(out_dir, exist_ok=True)
    written, cleared = [], []
    for r in results:
        path = os.path.join(out_dir, f"{r['ticker']}.txt")
        if r.get("error"):
            if os.path.exists(path):
                os.remove(path)
                cleared.append(r["ticker"])
            continue
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(f"{r['ticker']} ({r['interval']})\n")
            fh.write("=" * 60 + "\n")
            fh.write(f"model: {r.get('provider_used')}/{r.get('model_used')}\n\n")
            fh.write(f"RECOMMENDATION: {r['recommendation'].upper()}"
                     f"   (confidence: {r['confidence']})\n\n")
            fh.write("SIGNALS\n")
            for s in r["signals"]:
                fh.write(f"  - {s}\n")
            fh.write("\nKEY RISKS\n")
            for s in r["key_risks"]:
                fh.write(f"  - {s}\n")
            fh.write(f"\nSUMMARY\n{r['summary']}\n")
        written.append(path)
    print(f"[llm] wrote {len(written)} report(s) to {out_dir}/")
    if cleared:
        print(f"[llm] removed {len(cleared)} stale report(s) for failed "
              f"tickers: {', '.join(cleared)}")
    return written
