#!/usr/bin/env python3
"""Show what is actually on the project's Kafka topics.

This is the "did the data land?" tool. For each topic it prints the partition
count, retention, and per-partition low/high watermark offsets, plus a total
message count. Output is meant to be read at a glance, not grepped.

    python scripts/describe_topics.py
    python scripts/describe_topics.py --bootstrap kafka:9092

A high watermark equal to its low watermark means an empty (or fully expired)
partition. Uneven counts across partitions are expected and correct: messages
are keyed by ticker/cik, so one company always lands on one partition.
"""

from __future__ import annotations

import argparse
import os
import sys

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource

TOPICS = [
    os.getenv("PRICES_TOPIC", "market.prices.v1"),
    os.getenv("FILINGS_TOPIC", "sec.filings.v1"),
    os.getenv("TEXT_TOPIC", "sec.text.v1"),
]

TIMEOUT = 15.0


def default_bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_HOST") or os.getenv("KAFKA_BOOTSTRAP") or "localhost:29092"


def retention_of(admin: AdminClient, topic: str) -> str:
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    try:
        config = admin.describe_configs([resource])[resource].result()
    except Exception:  # noqa: BLE001 - a missing config is cosmetic here
        return "?"
    value = config["retention.ms"].value if "retention.ms" in config else "?"
    return "infinite" if str(value) == "-1" else f"{value} ms"


def describe(admin: AdminClient, consumer: Consumer, topic: str) -> None:
    metadata = admin.list_topics(topic=topic, timeout=TIMEOUT)
    info = metadata.topics[topic]

    if info.error is not None:
        print(f"{topic}\n  does not exist ({info.error}). Run scripts/create_topics.py.\n")
        return

    print(f"{topic}")
    print(f"  partitions: {len(info.partitions)}    retention: {retention_of(admin, topic)}")

    total = 0
    for pid in sorted(info.partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=TIMEOUT)
        count = high - low
        total += count
        print(f"    partition {pid}: offsets {low}..{high}  ({count} messages)")
    print(f"  total: {total} messages\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=default_bootstrap(),
                        help="Kafka bootstrap servers (default: %(default)s)")
    args = parser.parse_args()

    print(f"Broker: {args.bootstrap}\n")
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    # A group id is required even though we never join a group or commit; this
    # consumer exists only to read watermark offsets.
    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": "describe-topics",
        "enable.auto.commit": False,
    })

    try:
        admin.list_topics(timeout=TIMEOUT)
    except Exception as exc:  # noqa: BLE001
        print(f"Could not reach the broker at {args.bootstrap}: {exc}", file=sys.stderr)
        return 1

    try:
        for topic in TOPICS:
            describe(admin, consumer, topic)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
