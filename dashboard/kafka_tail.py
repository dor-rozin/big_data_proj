"""Background Kafka consumer for the live-tick widget.

Deliberately bypasses Elasticsearch and Spark entirely: the "Refresh data"
button and the KPI/price-chart views go through the batch pipeline on purpose
(idempotent, reprocessable, auditable), but that path has real per-run latency
(JVM startup) that makes it unsuitable for a widget meant to update every few
seconds. This module tails `market.prices.v1` directly, so a bar Finnhub (or
the replay producer) sent 2 seconds ago is visible 2 seconds later, with no
Spark run in between.

Design: one background thread per Streamlit server process (started via
`st.cache_resource`, so a page rerun reuses it rather than starting a second
consumer), reading from `auto.offset.reset=latest`. This means the tail only
sees messages produced AFTER the dashboard container started (or after this
module was first imported) -- it does NOT replay the historical backlog, which
would be tens of thousands of messages for a topic like this. That is the
correct behaviour for "what's happening right now", not a limitation to route
around: enable the widget, then start a producer, and bars appear within
seconds; enable it after a producer has been streaming a while and it shows
nothing until the next bar completes.
"""

from __future__ import annotations

import json
import os
import threading
import uuid

from confluent_kafka import Consumer

PRICES_TOPIC = os.getenv("PRICES_TOPIC", "market.prices.v1")


class TailState:
    """Thread-safe `ticker -> latest bar` map, updated by the background thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latest: dict[str, dict] = {}

    def update(self, ticker: str, bar: dict) -> None:
        with self._lock:
            self._latest[ticker] = bar

    def get(self, ticker: str) -> dict | None:
        with self._lock:
            return self._latest.get(ticker)


def _consume_forever(state: TailState, bootstrap: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        # Random, uncommitted group per process: this consumer must never
        # affect or be affected by any other consumer group's offsets, and
        # must always start from "whatever arrives next", not resume a stale
        # position from a previous dashboard container.
        "group.id": f"dashboard-live-tick-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([PRICES_TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                bar = json.loads(msg.value())
            except (ValueError, TypeError):
                continue
            ticker = bar.get("ticker")
            if ticker:
                state.update(ticker, bar)
    finally:
        consumer.close()


def start(bootstrap: str) -> TailState:
    """Start the background consumer once; safe to call on every rerun."""
    state = TailState()
    thread = threading.Thread(target=_consume_forever, args=(state, bootstrap),
                              daemon=True)
    thread.start()
    return state
