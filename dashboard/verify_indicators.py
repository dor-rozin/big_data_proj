#!/usr/bin/env python3
"""
Offline check of dashboard/indicators.py + woodies_chart.py against the parquet
snapshot. No Elasticsearch, no Docker, no browser — same contract as
verify_kpis.py and verify_ai.py.

    python3 verify_indicators.py [TICKER ...]

Four things are asserted, each one a failure that has actually happened:

  warm-up      every indicator reads true at the LEFT EDGE of the visible
               window. Skip the warm-up frame and the lines start blank.
  every bar    `_hide_gaps` hides empty grid slots; feed it timestamps that are
               not on a uniform grid and it hides real bars instead. This
               project's `ts` is midnight Eastern, so it shifts 04:00 <-> 05:00
               across DST and 126 bars once rendered as 24.
  axis collapse  every trace on x4, so the cursor spike is one continuous line.
  guide lines  every domain-referenced shape re-pointed to x4, or the CCI
               +/-100/+/-200 bands and the Stoch 80/20 guides stop rendering.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import indicators as I          # noqa: E402
import woodies_chart as W       # noqa: E402

REPO = Path(__file__).resolve().parent.parent
PRICES = REPO / "historical_data" / "market.prices.v1.historical" / "all.parquet"
WINDOW_DAYS = 182


def check(ticker: str, prices: pd.DataFrame) -> list[str]:
    """Returns a list of failure strings; empty means the ticker passed."""
    fails = []
    bars = W.bars_frame(prices[prices.ticker == ticker])
    if bars is None or len(bars) < 60:
        return [f"only {0 if bars is None else len(bars)} usable bars"]

    vis = bars[bars.ts >= bars.ts.max() - pd.Timedelta(days=WINDOW_DAYS)]["ts"]
    ind_df, df, short = W.prepare_frames(bars, vis, **I.DEFAULTS)
    fig, wdf = W.build_figure(ind_df, df, ticker, **I.DEFAULTS)

    # 1. warm-up — nothing NaN on the first visible bar
    k, d = I.stochastic_kd(ind_df, I.DEFAULTS["stoch_k"],
                           I.DEFAULTS["stoch_k_smooth"], I.DEFAULTS["stoch_d"])
    macd, sig, _ = I.macd_lines(ind_df, I.DEFAULTS["macd_fast"],
                                I.DEFAULTS["macd_slow"], I.DEFAULTS["macd_signal"])
    n = len(df)
    for name, ser in (("cci", wdf["cci"]), ("turbo", wdf["turbo"]),
                      ("%K", k.tail(n).reset_index(drop=True)),
                      ("%D", d.tail(n).reset_index(drop=True)),
                      ("macd", macd.tail(n).reset_index(drop=True)),
                      ("signal", sig.tail(n).reset_index(drop=True))):
        if pd.isna(ser.iloc[0]):
            fails.append(f"{name} NaN at left edge (warm-up short by {short})")

    # 2. every visible bar survives the rangebreaks
    rb = fig.layout.xaxis4.rangebreaks
    hidden = len(rb[0].values) if rb else 0
    span = (df["ts"].max() - df["ts"].min()).days + 1
    if span - hidden != n:
        fails.append(f"{span - hidden} bars drawn, expected {n} "
                     f"({hidden} slots hidden of {span})")

    # 3. every trace collapsed onto x4
    off = [t.name for t in fig.data if t.xaxis != "x4"]
    if off:
        fails.append(f"traces not on x4: {off}")

    # 4. every domain-referenced shape re-pointed to x4
    stray = {s.xref for s in fig.layout.shapes if s.xref != "x4 domain"}
    if stray:
        fails.append(f"shapes on empty axes (guides will vanish): {stray}")
    if len(fig.layout.shapes) != 8:
        fails.append(f"{len(fig.layout.shapes)} guide lines, expected 8")

    return fails


def main() -> int:
    if not PRICES.exists():
        print(f"missing {PRICES}")
        return 1
    prices = pd.read_parquet(PRICES)
    wanted = sys.argv[1:] or sorted(prices.ticker.unique())

    print(f"warmup_bars(defaults) = {I.warmup_bars(**I.DEFAULTS)}")
    print(f"window = last {WINDOW_DAYS} days")
    print("-" * 70)

    bad = 0
    for t in wanted:
        fails = check(t, prices)
        if fails:
            bad += 1
            print(f"!! {t:6} " + "; ".join(fails))
        else:
            print(f"OK {t:6} warm at left edge · all bars drawn · "
                  f"axes collapsed · 8 guides")

    print("-" * 70)
    print(f"checked {len(wanted)} ticker(s), {bad} failing")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
