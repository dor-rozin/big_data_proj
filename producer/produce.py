#!/usr/bin/env python3
"""Snapshot replay producer — the ingest stage of the pipeline.

Turns the on-disk snapshot files in `historical_data/` into a Kafka stream on
the three contract topics:

    historical_data/market.prices.v1.historical/all.parquet  ->  market.prices.v1
    historical_data/sec.filings.v1.historical/all.parquet    ->  sec.filings.v1
    historical_data/sec.text.v1.historical/all.parquet       ->  sec.text.v1

All streams are merged into one sequence ordered by **event time** before
anything is sent, so a filing (or its press release text) lands between the
price bars that surround it chronologically. Emitting all prices and then all
filings would leave the consumer's join with nothing sensible to do and make
the dashboard's time axis look wrong; the interleaving is the point of this
tool.

Replay speed changes *when* messages are sent, never the timestamps inside
them. `ts` and `filed_date` are always the original event times. `ingested_at`
is always the real wall-clock send time, so the gap between the two is visible
and explainable.

Two jobs, one tool
------------------
The snapshot is divided at a single instant -- by default the first price bar
plus `--backfill-days` (365) -- and each mode takes one side of it:

    --mode backfill   everything BEFORE the split, produced as fast as the
                      broker accepts. Run this once at startup so the topics
                      already hold a year of history, the way a real system
                      that has been running for a year would look.

    --mode live       everything AT OR AFTER the split, produced slowly, as if
                      it were arriving now. This is the stream the demo watches.

    --mode all        the whole snapshot in one pass. Default.

Both modes derive their window from the *same* split value, so they are exactly
complementary: no event is sent twice and none is skipped. That is the reason
this is one tool with a mode flag rather than two scripts.

    python produce.py --mode backfill                    # seed a year of history
    python produce.py --mode live --duration 300         # then stream the rest
    python produce.py --mode live --speed 500            # 500x event spacing
    python produce.py --mode backfill --dry-run          # validate, send nothing

There is no network dependency beyond Kafka: this works on a laptop with no
internet, in a classroom with bad wifi, and during the presentation.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from common import (
    DeliveryTracker,
    Progress,
    SCHEMA_VERSION,
    assert_valid,
    build_producer,
    flush_and_summarise,
    jsonable,
    load_validators,
    produce_message,
    utc_now_iso,
)

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
DEFAULT_SNAPSHOT_DIR = REPO_ROOT / "historical_data"

# Run from a dev checkout the schemas are a sibling of producer/; inside the
# container the Dockerfile copies them in next to this file.
DEFAULT_SCHEMA_DIR = HERE / "schemas" if (HERE / "schemas").is_dir() else REPO_ROOT / "schemas"

PRICES_TOPIC = os.getenv("PRICES_TOPIC", "market.prices.v1")
FILINGS_TOPIC = os.getenv("FILINGS_TOPIC", "sec.filings.v1")
TEXT_TOPIC = os.getenv("TEXT_TOPIC", "sec.text.v1")

# Fields each topic carries, in contract order. All three schemas set
# `additionalProperties: false`, so an extra parquet column is a hard failure
# rather than a passthrough — listing the fields explicitly is what keeps a
# stray column from ever reaching the wire.
PRICE_FIELDS = [
    "schema_version", "ticker", "ts", "open", "high", "low",
    "close", "volume", "interval", "ingested_at",
]
FILING_FIELDS = [
    "schema_version", "cik", "ticker", "accession_no", "form_type", "filed_date",
    "fiscal_period", "period_start", "period_end", "facts", "ingested_at",
]
TEXT_FIELDS = [
    "schema_version", "cik", "ticker", "accession_no", "form_type", "filed_date",
    "section", "source_document", "title", "text", "chunk_index", "chunk_total",
    "ingested_at",
]

_interrupted = False


def _handle_sigint(signum, frame) -> None:
    """Flag the run for a clean stop; the send loop flushes and summarises."""
    global _interrupted
    _interrupted = True
    print("\n[producer] interrupted - flushing outstanding messages...")


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #

def snapshot_path(snapshot_dir: Path, topic: str) -> Path:
    return snapshot_dir / f"{topic}.historical" / "all.parquet"


def read_snapshot(path: Path, label: str) -> pd.DataFrame:
    """Read one snapshot file, or exit with an actionable message if it is missing.

    Deliberately does not fall back to fetching from the network: a silent live
    fetch during a demo is worse than a clear error.
    """
    if not path.exists():
        sys.exit(
            f"[producer] no {label} snapshot at {path}\n"
            f"           Fetch it first:  python scripts/fetch_historical_data.py\n"
            f"                            python scripts/fetch_historical_filings.py\n"
            f"                            python scripts/fetch_historical_text.py"
        )
    return pd.read_parquet(path)


def price_events(df: pd.DataFrame) -> list[tuple[datetime, str, str, dict]]:
    """Build (event_time, topic, key, payload) tuples for every price bar.

    Keyed by ticker so every bar for one company lands on one partition and is
    strictly ordered, per schemas/README.md.
    """
    events = []
    for row in df.to_dict("records"):
        payload = {field: jsonable(row.get(field)) for field in PRICE_FIELDS}
        payload["schema_version"] = SCHEMA_VERSION
        # volume is an integer in the contract; parquet gives it back as a float
        # whenever the column contains a null.
        if payload["volume"] is not None:
            payload["volume"] = int(payload["volume"])
        events.append((parse_ts(payload["ts"]), PRICES_TOPIC, payload["ticker"], payload))
    return events


def filing_events(df: pd.DataFrame) -> list[tuple[datetime, str, str, dict]]:
    """Build (event_time, topic, key, payload) tuples for every filing.

    Keyed by cik, not ticker: the cik is the stable identifier across ticker
    changes, and it is what sec.text.v1 will join on.
    """
    events = []
    for row in df.to_dict("records"):
        payload = {field: jsonable(row.get(field)) for field in FILING_FIELDS}
        payload["schema_version"] = SCHEMA_VERSION
        # filed_date is a date, not a timestamp; order filings against price bars
        # at the start of the day they were accepted.
        events.append((parse_date(payload["filed_date"]), FILINGS_TOPIC, payload["cik"], payload))
    return events


def text_events(df: pd.DataFrame) -> list[tuple[datetime, str, str, dict]]:
    """Build (event_time, topic, key, payload) tuples for every press release.

    Keyed by cik, same as sec.filings.v1: they join on it, so both must land on
    the same partition. Ordered by filed_date, same as the parent filing.
    """
    events = []
    for row in df.to_dict("records"):
        payload = {field: jsonable(row.get(field)) for field in TEXT_FIELDS}
        payload["schema_version"] = SCHEMA_VERSION
        events.append((parse_date(payload["filed_date"]), TEXT_TOPIC, payload["cik"], payload))
    return events


def parse_ts(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def split_point(events: list, backfill_days: int, explicit: datetime | None) -> datetime:
    """The single instant that divides backfill from live.

    Both modes derive their window from this one value, so they are exactly
    complementary: every event lands in exactly one of them, with no gap and no
    overlap. Two independently configured producers would drift the moment
    somebody changed one and not the other.

    Anchored on the first *price* bar, not the first event: the filings snapshot
    reaches back to 1994, so anchoring on it would put the split three decades
    before any price data exists.
    """
    if explicit is not None:
        return explicit
    price_start = min((e[0] for e in events if e[1] == PRICES_TOPIC), default=None)
    if price_start is None:
        # No price bars (--prices-only inverted, or an empty snapshot): fall back
        # to the overall start so the split is still well defined.
        price_start = events[0][0]
    return price_start + timedelta(days=backfill_days)


def build_timeline(snapshot_dir: Path, include_filings: bool = True,
                   include_text: bool = True, since: datetime | None = None) -> list:
    """Merge all sources into one non-decreasing sequence of events by event time.

    Ties are broken by topic then key so two runs over the same snapshot produce
    byte-identical payloads in an identical order (apart from `ingested_at`).
    """
    prices = read_snapshot(snapshot_path(snapshot_dir, PRICES_TOPIC), "price")
    events = price_events(prices)
    price_start = min((e[0] for e in events), default=None)
    print(f"[producer] prices : {len(events):,} bars from {snapshot_path(snapshot_dir, PRICES_TOPIC)}")

    if include_filings:
        filings = read_snapshot(snapshot_path(snapshot_dir, FILINGS_TOPIC), "filing")
        filing_rows = filing_events(filings)
        print(f"[producer] filings: {len(filing_rows):,} filings from "
              f"{snapshot_path(snapshot_dir, FILINGS_TOPIC)}")
        filing_start = min((e[0] for e in filing_rows), default=None)
        events.extend(filing_rows)

        # The EDGAR snapshot reaches back to 1994 while the price snapshot covers
        # only the last two years. Harmless for --mode backfill (it is all sent
        # at once anyway) and irrelevant for --mode live (which starts at the
        # split), but under `--mode all --speed realtime` it means decades of
        # sparse filings followed by a burst of price bars. Say so rather than
        # letting the demo look broken.
        if since is None and price_start and filing_start and filing_start < price_start:
            gap_years = (price_start - filing_start).days / 365.25
            if gap_years > 1:
                print(f"[producer] note: filings start {gap_years:.0f} years before the first "
                      f"price bar ({filing_start.date()} vs {price_start.date()}). "
                      f"They all land in the backfill window.")

    if include_text:
        # The text snapshot reaches back to each company's IPO (SEC full-text
        # archives are cheap to keep on disk), but only the press releases that
        # land on or after the first price bar have any price bars to join
        # against. Clipping here, unconditionally, keeps a decade of
        # unjoinable 1990s press releases out of every replay regardless of
        # --since; the full archive is still on disk for anyone who wants it.
        text = read_snapshot(snapshot_path(snapshot_dir, TEXT_TOPIC), "press release")
        text_rows = text_events(text)
        total_text = len(text_rows)
        if price_start is not None:
            text_rows = [e for e in text_rows if e[0] >= price_start]
        events.extend(text_rows)
        print(f"[producer] text   : {len(text_rows):,} of {total_text:,} press releases from "
              f"{snapshot_path(snapshot_dir, TEXT_TOPIC)} land on or after the first price bar "
              f"({price_start.date() if price_start else 'n/a'}); the rest predate it and are skipped")

    if since is not None:
        before = len(events)
        events = [e for e in events if e[0] >= since]
        print(f"[producer] --since {since.date()}: kept {len(events):,} of {before:,} events")

    events.sort(key=lambda e: (e[0], e[1], e[2]))
    return events


# --------------------------------------------------------------------------- #
# Pacing
# --------------------------------------------------------------------------- #

def compression_factor(speed: str, duration: float, span_seconds: float) -> float:
    """How many seconds of event time pass per second of wall clock.

    `inf` means no waiting at all. Returning a single number here keeps the send
    loop free of speed-mode branching.
    """
    if speed == "instant":
        return float("inf")
    if speed == "realtime":
        if duration <= 0:
            sys.exit("[producer] --duration must be greater than 0 for --speed realtime")
        # Compress the entire historical span into `duration` wall-clock seconds,
        # preserving the relative spacing between events.
        return span_seconds / duration if span_seconds > 0 else float("inf")
    try:
        multiplier = float(speed)
    except ValueError:
        sys.exit(f"[producer] --speed must be 'instant', 'realtime', or a number (got {speed!r})")
    if multiplier <= 0:
        sys.exit("[producer] --speed multiplier must be greater than 0")
    return multiplier


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def replay(events: list, args: argparse.Namespace, producer, tracker: DeliveryTracker,
           validators: dict, sent_before: int) -> int:
    """Produce one pass over the timeline. Returns the number of messages sent."""
    span = (events[-1][0] - events[0][0]).total_seconds()
    factor = compression_factor(args.speed, args.duration, span)

    limit = args.limit or 0
    remaining = (limit - sent_before) if limit else len(events)
    total = min(len(events), remaining)

    progress = Progress(total)
    started = time.monotonic()
    sent = 0

    for event_time, topic, key, payload in events:
        if _interrupted or sent >= total:
            break

        if factor != float("inf"):
            # Wall-clock target for this event, relative to the run start.
            target = (event_time - events[0][0]).total_seconds() / factor
            delay = target - (time.monotonic() - started)
            if delay > 0:
                # Sleep in slices so Ctrl-C during a long gap is still responsive.
                deadline = time.monotonic() + delay
                while time.monotonic() < deadline and not _interrupted:
                    time.sleep(min(0.25, deadline - time.monotonic()))

        payload["ingested_at"] = utc_now_iso()
        if args.validate_all:
            assert_valid(validators, topic, payload)

        if not args.dry_run:
            produce_message(producer, tracker, topic, key, payload)
        sent += 1
        progress.update(sent)

    progress.update(sent, force=True)
    return sent


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--bootstrap",
                        default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                        help="Kafka bootstrap servers (default: %(default)s). Host processes "
                             "outside Docker want localhost:29092.")
    parser.add_argument("--snapshot-dir", type=Path,
                        default=Path(os.getenv("SNAPSHOT_DIR", str(DEFAULT_SNAPSHOT_DIR))),
                        help="Directory holding the snapshot parquet files (default: %(default)s)")
    parser.add_argument("--schema-dir", type=Path, default=DEFAULT_SCHEMA_DIR,
                        help="Directory holding the JSON Schemas used for the self-check.")
    parser.add_argument("--mode", choices=["backfill", "live", "all"],
                        default=os.getenv("REPLAY_MODE") or "all",
                        help="backfill = everything before the split, as fast as possible. "
                             "live = everything after it, paced as if arriving now. "
                             "all = the whole snapshot. Default: %(default)s")
    parser.add_argument("--backfill-days", type=int,
                        default=int(os.getenv("BACKFILL_DAYS") or 365),
                        help="Width of the backfill window, in days from the first price bar. "
                             "Default: %(default)s")
    parser.add_argument("--split-at", metavar="YYYY-MM-DD",
                        default=os.getenv("REPLAY_SPLIT_AT") or None,
                        help="Override the backfill/live boundary with an explicit date. "
                             "Both modes must be given the same value or they will overlap "
                             "or leave a gap.")
    # Default depends on --mode, so it is resolved after parsing rather than here.
    parser.add_argument("--speed", default=os.getenv("REPLAY_SPEED") or None,
                        help="instant | realtime | <multiplier>. "
                             "Defaults to instant for --mode backfill, realtime for --mode live.")
    parser.add_argument("--duration", type=float, default=float(os.getenv("REPLAY_DURATION", "300")),
                        help="Wall-clock seconds to compress the history into when "
                             "--speed realtime. Default: %(default)s")
    parser.add_argument("--limit", type=int, default=int(os.getenv("REPLAY_LIMIT", "0")),
                        help="Stop after N messages. 0 = no limit.")
    parser.add_argument("--loop", action="store_true",
                        default=os.getenv("REPLAY_LOOP", "false").lower() == "true",
                        help="Restart from the beginning on completion, for an unattended demo.")
    parser.add_argument("--since", metavar="YYYY-MM-DD",
                        default=os.getenv("REPLAY_SINCE") or None,
                        help="Drop events with an event time before this date. The filings "
                             "snapshot reaches back to 1994; clipping it to the price window "
                             "gives an evenly paced --speed realtime replay.")
    parser.add_argument("--prices-only", action="store_true",
                        help="Skip the filings and text topics. Useful when only the price path "
                             "is wired up.")
    parser.add_argument("--no-text", action="store_true",
                        help="Skip the text topic but keep filings. Useful when only the "
                             "sec.text.v1 snapshot is missing.")
    parser.add_argument("--validate-all", action="store_true",
                        help="Validate every message against its schema, not just one of each "
                             "type. Slower; use it when the snapshot format has changed.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Load, merge, and validate, but send nothing to Kafka.")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_sigint)

    since = parse_date(args.since) if args.since else None
    events = build_timeline(args.snapshot_dir,
                            include_filings=not args.prices_only,
                            include_text=not (args.prices_only or args.no_text),
                            since=since)
    if not events:
        sys.exit("[producer] the snapshot contains no events - nothing to replay.")
    print(f"[producer] timeline: {len(events):,} events, "
          f"{events[0][0].date()} -> {events[-1][0].date()}")

    # Split into backfill / live. Both sides come from one boundary value, so
    # running backfill and then live covers the snapshot exactly once.
    split = split_point(events, args.backfill_days,
                        parse_date(args.split_at) if args.split_at else None)
    before = [e for e in events if e[0] < split]
    after = [e for e in events if e[0] >= split]
    print(f"[producer] split at {split.date()}"
          + ("" if args.split_at else f" (first price bar + {args.backfill_days} days)"))
    print(f"[producer]   backfill side: {len(before):,} events"
          + (f" ({before[0][0].date()} -> {before[-1][0].date()})" if before else " (empty)"))
    print(f"[producer]   live side    : {len(after):,} events"
          + (f" ({after[0][0].date()} -> {after[-1][0].date()})" if after else " (empty)"))

    if args.mode == "backfill":
        events = before
    elif args.mode == "live":
        events = after

    if not events:
        sys.exit(f"[producer] --mode {args.mode} selected 0 events. Check --backfill-days "
                 f"/ --split-at against the timeline printed above.")
    print(f"[producer] mode: {args.mode} -> {len(events):,} events to send")

    # Speed default depends on the mode: a backfill wants to be over as fast as
    # possible, a live stream wants to be watchable.
    if args.speed is None:
        args.speed = "realtime" if args.mode == "live" else "instant"

    # Contract self-check before a single byte goes out: validate the first
    # message of each type. Drifting from the schema should fail here, not days
    # later inside somebody else's Spark job.
    validators = load_validators(args.schema_dir, [PRICES_TOPIC, FILINGS_TOPIC, TEXT_TOPIC])
    for topic in {PRICES_TOPIC, FILINGS_TOPIC, TEXT_TOPIC}:
        sample = next((p for _, t, _, p in events if t == topic), None)
        if sample is not None:
            assert_valid(validators, topic, {**sample, "ingested_at": utc_now_iso()})
    print(f"[producer] contract self-check passed ({len(validators)} schema(s) checked)")

    if args.dry_run:
        print("[producer] --dry-run: nothing will be sent to Kafka.")

    print(f"[producer] broker: {args.bootstrap}  speed: {args.speed}"
          + (f"  duration: {args.duration}s" if args.speed == "realtime" else ""))

    producer = None if args.dry_run else build_producer(args.bootstrap)
    tracker = DeliveryTracker()
    started_at = time.monotonic()

    total_sent = 0
    pass_number = 0
    while True:
        pass_number += 1
        if args.loop:
            print(f"\n[producer] pass {pass_number}")
        total_sent += replay(events, args, producer, tracker, validators, total_sent)
        if _interrupted:
            break
        if args.limit and total_sent >= args.limit:
            break
        if not args.loop:
            break

    if args.dry_run:
        print(f"\n[producer] dry run complete: {total_sent:,} messages would have been sent.")
        return 0

    return flush_and_summarise(producer, tracker, started_at)


if __name__ == "__main__":
    sys.exit(main())
