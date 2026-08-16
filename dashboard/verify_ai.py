#!/usr/bin/env python3
"""
Offline check of dashboard/ai_analyst.py against the parquet snapshot.

    python dashboard/verify_ai.py                 # build evidence + prompt, all tickers
    python dashboard/verify_ai.py AAPL            # one ticker, print the whole prompt
    python dashboard/verify_ai.py AAPL --call     # ...and make ONE real API call

Why this exists: the AI panel lives at the end of a long chain — Docker, Kafka, a
producer run, a Spark run, Elasticsearch — and none of that is needed to check
whether the evidence handed to the model is right. This script rebuilds the same
document shape from `historical_data/` (reusing `verify_kpis.load_filings`, so
there is one definition of that shape, not two), computes the seven KPIs, and
assembles the exact prompt `app.py` would send.

**Without `--call` it touches no network and spends no quota.** That is the point:
the prompt is the part worth iterating on, and Gemini's free tier is roughly 30
calls a day. Read the prompt, fix the prompt, and only then spend a call.

`--call` makes exactly one request for one ticker and validates the response
against the same contract `app.py` enforces. It needs a key in `.env`; without
one it says so and exits 0, because a keyless run is a supported state.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ai_analyst  # noqa: E402
import kpis  # noqa: E402
import verify_kpis  # noqa: E402

REPO = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    """Read `.env` from the repo root, since this runs outside the container.

    Only fills variables that are not already set, so a real environment always
    wins. Values are not printed anywhere — the key must not reach stdout.
    """
    import os
    path = REPO / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        os.environ.setdefault(name.strip(), value.strip())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("ticker", nargs="?", help="one ticker; omit for all")
    ap.add_argument("--call", action="store_true",
                    help="make ONE real API call (needs a key; spends quota)")
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--focus", default="",
                    help="reader's emphasis note, exactly as the dashboard's "
                         "text field passes it")
    args = ap.parse_args()

    _load_dotenv()

    if not verify_kpis.FILINGS.exists():
        print(f"missing snapshot: {verify_kpis.FILINGS}", file=sys.stderr)
        return 1

    filings = verify_kpis.load_filings()
    prices = verify_kpis.load_prices()

    cfg = ai_analyst.provider_config()
    can_call, why = ai_analyst.availability()
    print(f"provider   : {cfg['provider']}  model={cfg['model']}")
    print(f"key present: {cfg['has_key']}    LLM_ENABLED={cfg['enabled']}")
    print(f"prompt file: {ai_analyst.PROMPT_PATH}")
    print(f"can call   : {can_call}" + ("" if can_call else f"  ({why})"))
    print("-" * 70)

    tickers = ([args.ticker.upper()] if args.ticker
               else sorted(t for t in filings.ticker.dropna().unique()))

    failures = 0
    for tk in tickers:
        f = filings[filings.ticker == tk]
        p = prices[prices.ticker == tk]
        result = kpis.compute_all(f, p, years=args.years)
        if not result:
            print(f"!! {tk:6s} no annual filings — nothing to send")
            failures += 1
            continue

        evidence = ai_analyst.build_evidence(tk, result, args.years, context=None)
        prompt = ai_analyst.build_prompt(evidence, focus=args.focus)

        metrics = evidence["metrics"]
        # Counted on `value_field`, not on "the row has any column at all", so
        # this agrees with verify_kpis.py. AMZN's `debt_equity` is the case that
        # separates them: its rows carry a real `equity` figure every year and
        # never a `debt_to_equity`, because total liabilities are not tagged, so
        # there is nothing to divide. Counting rows would call that metric
        # present; it is not.
        with_data = sum(1 for m in metrics.values()
                        if any(m["value_field"] in row
                               for row in m["by_fiscal_year"]))
        gaps = sum(len(m["data_gaps"]) for m in metrics.values())
        years_seen = sorted({row["fiscal_year"]
                             for m in metrics.values()
                             for row in m["by_fiscal_year"]})
        flag = "OK" if with_data == len(ai_analyst.SLUG_ORDER) else "!!"
        print(f"{flag} {tk:6s} {with_data}/{len(ai_analyst.SLUG_ORDER)} metrics with data"
              f" · {len(prompt):,} char prompt · {gaps} gap note(s)"
              f" · FY{years_seen[0] if years_seen else '?'}"
              f"-FY{years_seen[-1] if years_seen else '?'}")

        if args.ticker:
            print("\n" + "=" * 70)
            print(prompt)
            print("=" * 70 + "\n")

    if args.call:
        if not can_call:
            print(f"\n--call requested but no call is possible: {why}")
            return 0 if failures == 0 else 1
        tk = tickers[0]
        f = filings[filings.ticker == tk]
        p = prices[prices.ticker == tk]
        evidence = ai_analyst.build_evidence(
            tk, kpis.compute_all(f, p, years=args.years), args.years)
        print(f"\ncalling {cfg['provider']} ({cfg['model']}) once for {tk} ...")
        if args.focus:
            print(f"emphasis: {args.focus!r}")
        try:
            out = ai_analyst.analyse(evidence, focus=args.focus)
        except ai_analyst.AnalystError as exc:
            print(f"CALL FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(out, indent=2))
        print(f"\ncontract OK: recommendation={out['recommendation']} "
              f"confidence={out['confidence']} "
              f"signals={len(out['signals'])} risks={len(out['key_risks'])}")

    print(f"\nchecked {len(tickers)} ticker(s)")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
