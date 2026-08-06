#!/usr/bin/env python3
"""Create the project's Kafka topics with explicit, replayable config.

Kafka will auto-create a topic on first write, with one partition and seven-day
retention. Both defaults are wrong here: partition count cannot be lowered after
creation, and seven-day retention means a topic filled on Monday is empty the
following Tuesday, which looks exactly like a consumer bug.

This script is idempotent. A topic that already exists is never deleted or
recreated; its config is read back and any drift from the values below is
reported as a warning. It reports drift, it does not fight the operator.

    python scripts/create_topics.py                 # uses $KAFKA_BOOTSTRAP_HOST
    python scripts/create_topics.py --bootstrap kafka:9092

Exits 0 on success (including "already exists"), 1 if a topic could not be
created or the broker was unreachable.
"""

from __future__ import annotations

import argparse
import os
import sys

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
REPLICATION = int(os.getenv("TOPIC_REPLICATION", "1"))
RETENTION_MS = os.getenv("TOPIC_RETENTION_MS", "-1")

# Declarative topic config. Keys are read from the environment so the topic
# names live in exactly one place (.env) across the producer, Spark, and here.
#
#   partitions=3    -- so the consumer half can demonstrate parallel consumption.
#   retention.ms=-1 -- infinite, so replay-from-earliest always works.
#   cleanup.policy=delete -- these are event logs, not compacted state. Stating
#     it explicitly documents the decision; compaction plus a cik key would
#     collapse a company's filing history to its latest filing.
TOPICS = {
    os.getenv("PRICES_TOPIC", "market.prices.v1"): {
        "key": "ticker",
        "note": "OHLCV price bars",
    },
    os.getenv("FILINGS_TOPIC", "sec.filings.v1"): {
        "key": "cik",
        "note": "SEC filings, normalised XBRL facts",
    },
    os.getenv("TEXT_TOPIC", "sec.text.v1"): {
        "key": "cik",
        "note": "SEC filing narrative text (8-K press releases, ticket 0010)",
    },
}

TOPIC_CONFIG = {
    "retention.ms": RETENTION_MS,
    "cleanup.policy": "delete",
}

CONNECT_TIMEOUT = 15.0


def default_bootstrap() -> str:
    """Prefer the host listener: this script is usually run from a dev venv."""
    return os.getenv("KAFKA_BOOTSTRAP_HOST") or os.getenv("KAFKA_BOOTSTRAP") or "localhost:29092"


def existing_topics(admin: AdminClient) -> set[str]:
    metadata = admin.list_topics(timeout=CONNECT_TIMEOUT)
    return set(metadata.topics)


def create_missing(admin: AdminClient, missing: list[str]) -> int:
    """Create the named topics. Returns the number of failures."""
    new_topics = [
        NewTopic(
            name,
            num_partitions=PARTITIONS,
            replication_factor=REPLICATION,
            config=dict(TOPIC_CONFIG),
        )
        for name in missing
    ]
    failures = 0
    for name, future in admin.create_topics(new_topics, request_timeout=CONNECT_TIMEOUT).items():
        try:
            future.result()
            print(f"CREATED  {name}  partitions={PARTITIONS} replication={REPLICATION} "
                  f"retention.ms={RETENTION_MS}")
        except Exception as exc:  # noqa: BLE001 - the broker error text is what matters
            print(f"FAILED   {name}: {exc}", file=sys.stderr)
            failures += 1
    return failures


def report_drift(admin: AdminClient, name: str) -> None:
    """Compare an existing topic against the expected config and print the result."""
    metadata = admin.list_topics(topic=name, timeout=CONNECT_TIMEOUT)
    actual_partitions = len(metadata.topics[name].partitions)

    resource = ConfigResource(ConfigResource.Type.TOPIC, name)
    try:
        config = admin.describe_configs([resource])[resource].result()
    except Exception as exc:  # noqa: BLE001
        print(f"EXISTS   {name}  (could not read config: {exc})")
        return

    drift = []
    if actual_partitions != PARTITIONS:
        drift.append(f"partitions expected={PARTITIONS} actual={actual_partitions}")
    for key, expected in TOPIC_CONFIG.items():
        actual = config[key].value if key in config else "<unset>"
        if str(actual) != str(expected):
            drift.append(f"{key} expected={expected} actual={actual}")

    if drift:
        print(f"EXISTS   {name}  CONFIG DRIFT:")
        for line in drift:
            print(f"           - {line}")
    else:
        print(f"EXISTS   {name}  config matches")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=default_bootstrap(),
                        help="Kafka bootstrap servers (default: %(default)s)")
    args = parser.parse_args()

    print(f"Broker: {args.bootstrap}")
    admin = AdminClient({"bootstrap.servers": args.bootstrap})

    try:
        present = existing_topics(admin)
    except Exception as exc:  # noqa: BLE001
        print(f"Could not reach the broker at {args.bootstrap}: {exc}", file=sys.stderr)
        print("Is the stack up? Try: docker compose up -d kafka", file=sys.stderr)
        return 1

    missing = [name for name in TOPICS if name not in present]
    failures = create_missing(admin, missing) if missing else 0
    for name in TOPICS:
        if name in present:
            report_drift(admin, name)

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
