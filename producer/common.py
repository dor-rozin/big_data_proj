"""Shared producer plumbing: client factory, delivery accounting, validation.

Everything in here is deliberately independent of *what* is being produced, so
the snapshot replay producer in `produce.py` and any later producer (mock, live
WebSocket) share one implementation of the parts that are easy to get subtly
wrong: backpressure, delivery accounting, and flushing before exit.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import KafkaException, Producer

# Every message on every topic carries this as a Kafka header, so a consumer can
# route on the contract version without deserialising the payload first.
SCHEMA_VERSION = 1
SCHEMA_VERSION_HEADER = ("schema_version", str(SCHEMA_VERSION).encode())


def utc_now_iso() -> str:
    """ISO 8601 UTC with a trailing Z, matching the pattern every schema enforces.

    Note `datetime.now(timezone.utc)`, never `datetime.now()`: a naive local
    timestamp passes a `format: date-time` check and then silently shifts every
    event by the machine's UTC offset.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def jsonable(value: Any) -> Any:
    """Convert pandas/numpy scalars to plain Python, and NaN/NaT to None.

    Parquet round-trips give back numpy scalars and float NaN. `json.dumps`
    renders NaN as the bare token `NaN`, which is invalid JSON that most
    consumers accept on read and then choke on later. The schemas say a missing
    value is `null`, so that is what goes on the wire.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    # numpy scalars expose .item(); pandas NA-likes are not equal to themselves.
    item = value.item() if hasattr(value, "item") else value
    if isinstance(item, float) and item != item:  # NaN
        return None
    return item


# --------------------------------------------------------------------------- #
# Schema validation
# --------------------------------------------------------------------------- #

def load_validators(schema_dir: Path, topics: list[str]) -> dict[str, Any]:
    """Build one JSON Schema validator per topic, keyed by topic name.

    Returns an empty dict (and warns) if jsonschema is unavailable, so a missing
    dev dependency degrades the self-check rather than blocking the producer.
    """
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        print("[producer] jsonschema not installed - skipping contract self-check.",
              file=sys.stderr)
        return {}

    validators = {}
    for topic in topics:
        path = schema_dir / f"{topic}.schema.json"
        if not path.exists():
            print(f"[producer] no schema at {path} - skipping self-check for {topic}.",
                  file=sys.stderr)
            continue
        validators[topic] = Draft202012Validator(json.loads(path.read_text()))
    return validators


def assert_valid(validators: dict[str, Any], topic: str, message: dict) -> None:
    """Fail loudly, before anything is sent, if a message has drifted from the contract.

    Producing an invalid message is worse than not producing at all: it lands on
    a topic with infinite retention and breaks a consumer days later, far from
    the change that caused it.
    """
    validator = validators.get(topic)
    if validator is None:
        return
    errors = sorted(validator.iter_errors(message), key=lambda e: list(e.absolute_path))
    if not errors:
        return
    lines = [f"Message does not match the {topic} contract:"]
    for error in errors:
        location = ".".join(str(p) for p in error.absolute_path) or "<root>"
        lines.append(f"  {location}: {error.message}")
    lines.append(f"  offending message: {json.dumps(message, default=str)[:500]}")
    raise SystemExit("\n".join(lines))


# --------------------------------------------------------------------------- #
# Producing
# --------------------------------------------------------------------------- #

class DeliveryTracker:
    """Counts delivery outcomes from the producer's async callback thread.

    `confluent-kafka` acknowledges deliveries asynchronously, so a script that
    exits without flushing drops its tail silently. Attempted vs delivered is
    the only way to know that did not happen.
    """

    def __init__(self) -> None:
        self.attempted = 0
        self.delivered = 0
        self.failed = 0
        self.first_error: str | None = None

    def callback(self, err, msg) -> None:
        if err is None:
            self.delivered += 1
            return
        self.failed += 1
        if self.first_error is None:
            key = msg.key().decode() if msg and msg.key() else "<no key>"
            self.first_error = f"{key}: {err}"


def build_producer(bootstrap: str, queue_max_messages: int = 100_000) -> Producer:
    """Create a Kafka producer configured for at-least-once bulk loading.

    - `acks=all` + `enable.idempotence` means a retry after a partial failure
      does not duplicate the message. Team design is at-least-once delivery with
      an idempotent document id on the Elasticsearch side; idempotence here just
      removes the cheapest source of duplicates.
    - `linger.ms=20` batches small messages. At ~1 KB a message this is the
      difference between a replay taking seconds and taking minutes.
    """
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "lz4",
        "queue.buffering.max.messages": queue_max_messages,
    })


def produce_message(producer: Producer, tracker: DeliveryTracker, topic: str,
                    key: str, payload: dict) -> None:
    """Produce one message, blocking on backpressure instead of dropping it.

    When the local queue fills, `produce()` raises BufferError. The fix is to
    poll (which drains delivery callbacks and frees queue slots) and retry the
    *same* message. A loop that catches BufferError and moves on loses messages
    with no error anywhere.
    """
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    while True:
        try:
            producer.produce(
                topic,
                key=key.encode(),
                value=encoded,
                headers=[SCHEMA_VERSION_HEADER],
                on_delivery=tracker.callback,
            )
            break
        except BufferError:
            producer.poll(0.5)
        except KafkaException as exc:
            tracker.failed += 1
            if tracker.first_error is None:
                tracker.first_error = f"{key}: {exc}"
            break
    tracker.attempted += 1
    producer.poll(0)


def flush_and_summarise(producer: Producer, tracker: DeliveryTracker,
                        started_at: float) -> int:
    """Flush outstanding messages, print the run summary, return an exit code."""
    remaining = producer.flush(timeout=60)
    elapsed = max(time.monotonic() - started_at, 1e-6)

    print()
    print("---- summary ----------------------------------------------")
    print(f"  attempted : {tracker.attempted}")
    print(f"  delivered : {tracker.delivered}")
    print(f"  failed    : {tracker.failed}")
    print(f"  elapsed   : {elapsed:.1f}s")
    print(f"  rate      : {tracker.delivered / elapsed:,.0f} msg/s")
    if remaining:
        print(f"  WARNING   : {remaining} message(s) still queued after a 60s flush.")
    if tracker.first_error:
        print(f"  first error: {tracker.first_error}")

    if tracker.failed or remaining or tracker.delivered != tracker.attempted:
        return 1
    return 0


class Progress:
    """Single updating status line. One log line per message is unreadable at 5k messages."""

    def __init__(self, total: int, interval: float = 0.5) -> None:
        self.total = total
        self.interval = interval
        self.started = time.monotonic()
        self._last = 0.0

    def update(self, sent: int, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last < self.interval:
            return
        self._last = now
        elapsed = now - self.started
        rate = sent / elapsed if elapsed > 0 else 0
        pct = (100 * sent / self.total) if self.total else 0
        eta = (self.total - sent) / rate if rate > 0 and self.total else 0
        sys.stdout.write(
            f"\r  {sent:,}/{self.total:,} ({pct:5.1f}%)  "
            f"{rate:,.0f} msg/s  elapsed {elapsed:5.1f}s  eta {eta:5.1f}s   "
        )
        sys.stdout.flush()
