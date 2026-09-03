"""Finnhub WebSocket -> market.prices.v1: real trades, aggregated into bars.

The replay producer (produce.py) makes the demo reliable by replaying history;
this one makes it *live*. It opens `wss://ws.finnhub.io`, subscribes to a set
of symbols, and turns the trade stream into OHLCV bars on the same topic and
schema the replay producer writes to -- nothing downstream (Spark, ES,
dashboard) has to know which producer wrote a given message.

Market-hours independence: Finnhub's free trade socket only emits when trades
actually happen. US equities trade 16:30-23:00 Israel time; outside that
window an equities run connects cleanly and then sits in silence. Crypto
symbols (e.g. BINANCE:BTCUSDT) trade continuously on the same free socket, so
a presentation at any hour should lead with a crypto symbol list:

    python -m producer.live_producer --symbols AAPL,MSFT,NVDA --bar-interval 1m
    python -m producer.live_producer --symbols BINANCE:BTCUSDT,BINANCE:ETHUSDT --bar-interval 1m

Inbound frames (documented here because it is easy to misremember): trades
arrive as `{"type":"trade","data":[{"s":sym,"p":price,"t":unix_ms,"v":volume,
"c":[conditions]}, ...]}`. `{"type":"ping"}` frames are keepalives, not trades.

Bar interval is currently pinned to schema-valid widths (1m/5m/1h/1d) --
sub-minute bars need a schema change to `market.prices.v1`'s `interval` enum,
which is a frozen contract and out of scope here (see CLAUDE.md).

Windows with no trades emit nothing -- a zero-volume placeholder bar would
corrupt any downstream average, and "no trade happened" is a different fact
from "price stayed flat".
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

import websocket

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.common import (
    DeliveryTracker,
    assert_valid,
    build_producer,
    flush_and_summarise,
    load_validators,
    produce_message,
    utc_now_iso,
)

FINNHUB_WS_URL = "wss://ws.finnhub.io"
FREE_TIER_SYMBOL_CAP = 50
BAR_INTERVAL_SECONDS = {"1m": 60, "5m": 300, "1h": 3600, "1d": 86400}
RECONNECT_BACKOFF = (1, 2, 4, 8, 16, 30)  # seconds, capped at 30
DEFAULT_SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas"
PRICES_TOPIC = os.getenv("PRICES_TOPIC", "market.prices.v1")


def normalize_ticker(symbol: str) -> str:
    """Finnhub crypto symbols look like BINANCE:BTCUSDT; the schema wants BTCUSDT.

    Equities symbols (AAPL) pass through unchanged. Keeping the mapping to one
    function means the raw Finnhub symbol never leaks onto the wire.
    """
    return symbol.split(":", 1)[1] if ":" in symbol else symbol


class BarAggregator:
    """Tumbling per-symbol OHLCV window, keyed on trade timestamp, not arrival time."""

    def __init__(self, interval_seconds: int, interval_label: str) -> None:
        self.interval_seconds = interval_seconds
        self.interval_label = interval_label
        self._windows: dict[str, dict] = {}  # ticker -> in-progress window state

    def _window_start(self, trade_ts_ms: int) -> int:
        trade_ts_s = trade_ts_ms // 1000
        return trade_ts_s - (trade_ts_s % self.interval_seconds)

    def add_trade(self, symbol: str, price: float, volume: float, trade_ts_ms: int):
        """Feed one trade in. Returns a completed bar dict if this trade closed
        a prior window for this symbol, else None. A trade exactly on a
        boundary starts the *later* window (window_start uses floor division,
        so ts == boundary always belongs to the window beginning at ts)."""
        ticker = normalize_ticker(symbol)
        window_start = self._window_start(trade_ts_ms)
        win = self._windows.get(ticker)

        completed = None
        if win is not None and win["window_start"] != window_start:
            completed = self._finalize(ticker, win)
            win = None

        if win is None:
            win = {
                "window_start": window_start,
                "open": price, "open_ts": trade_ts_ms,
                "close": price, "close_ts": trade_ts_ms,
                "high": price, "low": price,
                "volume": 0.0,
            }
            self._windows[ticker] = win

        if trade_ts_ms < win["open_ts"]:
            win["open"], win["open_ts"] = price, trade_ts_ms
        if trade_ts_ms >= win["close_ts"]:
            win["close"], win["close_ts"] = price, trade_ts_ms
        win["high"] = max(win["high"], price)
        win["low"] = min(win["low"], price)
        win["volume"] += volume

        return completed

    def _finalize(self, ticker: str, win: dict) -> dict:
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(win["window_start"]))
        return {
            "schema_version": 1,
            "ticker": ticker,
            "ts": ts,
            "open": win["open"],
            "high": win["high"],
            "low": win["low"],
            "close": win["close"],
            # round, not truncate: crypto trade sizes are fractional, and the
            # schema's volume field is an integer -- truncation would silently
            # zero out any window with sub-1-unit total volume.
            "volume": round(win["volume"]),
            "interval": self.interval_label,
            "ingested_at": utc_now_iso(),
        }

    def flush_all(self) -> list[dict]:
        """Finalize every in-flight window (Ctrl-C, reconnect). Clears state."""
        bars = [self._finalize(ticker, win) for ticker, win in self._windows.items()]
        self._windows.clear()
        return bars


class LiveProducer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.symbols = args.symbols
        self.interval_seconds = BAR_INTERVAL_SECONDS[args.bar_interval]
        self.aggregator = BarAggregator(self.interval_seconds, args.bar_interval)
        self.kafka_producer = build_producer(args.bootstrap)
        self.tracker = DeliveryTracker()
        self.validators = load_validators(args.schema_dir, [PRICES_TOPIC])
        self.started_at = time.monotonic()
        self.trades_this_window = 0
        self.bars_emitted = 0
        self.last_bar_ts: str | None = None
        self.connected = False
        self.reconnect_attempt = 0
        self._stop = threading.Event()
        self._ws: websocket.WebSocketApp | None = None

    # -- WebSocket callbacks -------------------------------------------------

    def _on_open(self, ws) -> None:
        self.connected = True
        self.reconnect_attempt = 0
        for symbol in self.symbols:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        print(f"\n[live] connected, subscribed to {len(self.symbols)} symbol(s)")

    def _on_message(self, ws, raw: str) -> None:
        msg = json.loads(raw)
        if msg.get("type") == "ping":
            return
        if msg.get("type") != "trade":
            return
        for trade in msg.get("data", []):
            self.trades_this_window += 1
            bar = self.aggregator.add_trade(
                trade["s"], trade["p"], trade.get("v", 0), trade["t"]
            )
            if bar is not None:
                self._emit(bar)

    def _on_error(self, ws, error) -> None:
        print(f"\n[live] socket error: {error}", file=sys.stderr)

    def _on_close(self, ws, status_code, msg) -> None:
        self.connected = False
        print(f"\n[live] disconnected (code={status_code})")

    def _emit(self, bar: dict) -> None:
        assert_valid(self.validators, PRICES_TOPIC, bar)
        produce_message(self.kafka_producer, self.tracker, PRICES_TOPIC, bar["ticker"], bar)
        self.bars_emitted += 1
        self.last_bar_ts = bar["ts"]

    # -- status line ----------------------------------------------------------

    def _status_line(self) -> None:
        state = "connected" if self.connected else "reconnecting"
        last = self.last_bar_ts or "-"
        sys.stdout.write(
            f"\r  [{state}] symbols={len(self.symbols)}  "
            f"trades(window)={self.trades_this_window}  "
            f"bars emitted={self.bars_emitted}  last bar={last}   "
        )
        sys.stdout.flush()
        self.trades_this_window = 0

    def _status_loop(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self.interval_seconds)
            if not self._stop.is_set():
                self._status_line()

    # -- run loop with reconnect ------------------------------------------------

    def run(self) -> int:
        status_thread = threading.Thread(target=self._status_loop, daemon=True)
        status_thread.start()

        deadline = time.monotonic() + self.args.duration if self.args.duration else None
        while not self._stop.is_set():
            if deadline and time.monotonic() >= deadline:
                break
            self._ws = websocket.WebSocketApp(
                f"{FINNHUB_WS_URL}?token={self.args.api_key}",
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            remaining = (deadline - time.monotonic()) if deadline else None
            self._run_socket_with_timeout(remaining)

            if self._stop.is_set() or (deadline and time.monotonic() >= deadline):
                break

            backoff = RECONNECT_BACKOFF[min(self.reconnect_attempt, len(RECONNECT_BACKOFF) - 1)]
            self.reconnect_attempt += 1
            print(f"\n[live] reconnecting in {backoff}s (attempt {self.reconnect_attempt})...")
            # A completed-but-unflushed window survives a reconnect on purpose;
            # only Ctrl-C / final shutdown flushes in-flight windows.
            time.sleep(backoff)

        self._stop.set()
        for bar in self.aggregator.flush_all():
            self._emit(bar)
        return flush_and_summarise(self.kafka_producer, self.tracker, self.started_at)

    def _run_socket_with_timeout(self, remaining: float | None) -> None:
        """run_forever blocks until closed; run it in a thread so --duration can cut it off."""
        t = threading.Thread(target=self._ws.run_forever, kwargs={"reconnect": 0}, daemon=True)
        t.start()
        t.join(timeout=remaining)
        if t.is_alive():
            self._ws.close()
            t.join(timeout=5)

    def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            self._ws.close()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--symbols", required=True,
                         help="Comma-separated Finnhub symbols, e.g. AAPL,MSFT or "
                              "BINANCE:BTCUSDT,BINANCE:ETHUSDT for market-hours independence.")
    parser.add_argument("--bar-interval", choices=sorted(BAR_INTERVAL_SECONDS), default="1m",
                         help="Bar width. Pinned to schema-valid values (default 1m); "
                              "sub-minute bars need a market.prices.v1 schema change first.")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:29092"))
    parser.add_argument("--schema-dir", type=Path, default=DEFAULT_SCHEMA_DIR)
    parser.add_argument("--duration", type=float, default=None,
                         help="Exit cleanly after N seconds. Default: run until Ctrl-C.")
    args = parser.parse_args(argv)

    args.symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if not args.symbols:
        sys.exit("[live] --symbols must name at least one symbol.")
    if len(args.symbols) > FREE_TIER_SYMBOL_CAP:
        sys.exit(f"[live] {len(args.symbols)} symbols requested, but Finnhub's free tier "
                  f"caps the trade socket at {FREE_TIER_SYMBOL_CAP}. Trim --symbols.")

    args.api_key = os.getenv("FINNHUB_API_KEY")
    if not args.api_key:
        sys.exit("[live] FINNHUB_API_KEY is not set. Get a free key at "
                  "https://finnhub.io/register and put it in .env.")
    return args


def main() -> int:
    args = parse_args()
    producer = LiveProducer(args)

    def handle_sigint(signum, frame):
        print("\n[live] Ctrl-C received, flushing current window and exiting...")
        producer.stop()

    signal.signal(signal.SIGINT, handle_sigint)
    return producer.run()


if __name__ == "__main__":
    sys.exit(main())
