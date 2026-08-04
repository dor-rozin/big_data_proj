"""Bulk-load the older half of the historical price snapshot into Kafka.

Splits `historical_data/market.prices.v1.historical/all.parquet` (2 years of
daily bars per ticker) at the 1-year mark:

- **Older year** (snapshot start -> snapshot start + 365 days): produced to the
  `market.prices.v1` topic now, in bulk, so the Spark side has real data to
  build against immediately.
- **Newer year** (everything after the cutoff): NOT sent to Kafka. Written to
  `historical_data/market.prices.v1.historical/remaining_for_replay.parquet`
  instead, for a later producer (ticket 0007 - replay producer) to stream in
  slowly, at a simulated pace, as if it were arriving live.

This is a one-off bulk load, not the replay producer itself: no speed control,
no interleaving with filings, no backoff. It exists to unblock the consumer
side before that ticket is written.

Usage:
    python scripts/load_historical_prices_to_kafka.py [--dry-run]

Requires a reachable Kafka broker at $KAFKA_BOOTSTRAP (default
localhost:29092 - the PLAINTEXT_HOST listener in docker-compose.yml).
"""

import argparse
import json
import os
import sys

import pandas as pd
from confluent_kafka import Producer

IN_PATH = "historical_data/market.prices.v1.historical/all.parquet"
REMAINDER_PATH = "historical_data/market.prices.v1.historical/remaining_for_replay.parquet"
TOPIC = "market.prices.v1"
SPLIT_WINDOW_DAYS = 365


def load_split() -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    df = pd.read_parquet(IN_PATH)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)

    cutoff = df["ts"].min() + pd.Timedelta(days=SPLIT_WINDOW_DAYS)
    to_load = df[df["ts"] < cutoff].sort_values(["ts", "ticker"]).reset_index(drop=True)
    remaining = df[df["ts"] >= cutoff].sort_values(["ts", "ticker"]).reset_index(drop=True)
    return to_load, remaining, cutoff


def to_message(row: pd.Series) -> dict:
    d = row.to_dict()
    d["ts"] = row["ts"].strftime("%Y-%m-%dT%H:%M:%SZ")
    return d


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"  DELIVERY FAILED: {msg.key()} -> {err}", file=sys.stderr)


def produce_all(df: pd.DataFrame, bootstrap: str) -> int:
    producer = Producer({"bootstrap.servers": bootstrap})
    delivered = 0
    for _, row in df.iterrows():
        payload = to_message(row)
        while True:
            try:
                producer.produce(
                    TOPIC,
                    key=payload["ticker"].encode(),
                    value=json.dumps(payload).encode(),
                    headers={"schema_version": b"1"},
                    callback=delivery_report,
                )
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        delivered += 1
    producer.flush()
    return delivered


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print the split summary without producing to Kafka.")
    args = parser.parse_args()

    if not os.path.exists(IN_PATH):
        sys.exit(f"{IN_PATH} not found. Run scripts/fetch_historical_data.py first.")

    to_load, remaining, cutoff = load_split()
    print(f"Cutoff: {cutoff.date()} (snapshot start + {SPLIT_WINDOW_DAYS} days)")
    print(f"To load into Kafka now:  {len(to_load)} rows ({to_load['ts'].min().date()} -> {to_load['ts'].max().date()})")
    print(f"Held back for replay:    {len(remaining)} rows ({remaining['ts'].min().date()} -> {remaining['ts'].max().date()})")

    remaining_out = remaining.copy()
    remaining_out["ts"] = remaining_out["ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    remaining_out.to_parquet(REMAINDER_PATH, index=False)
    print(f"Wrote remainder -> {REMAINDER_PATH}")

    if args.dry_run:
        print("--dry-run: not producing to Kafka.")
        return

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
    print(f"Producing {len(to_load)} messages to topic '{TOPIC}' at {bootstrap}...")
    delivered = produce_all(to_load, bootstrap)
    print(f"Done. Delivered {delivered} messages.")


if __name__ == "__main__":
    main()
