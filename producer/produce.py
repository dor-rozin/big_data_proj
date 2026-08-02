"""
Producer (INGEST stage) -- owned by Person A.

Pulls two kinds of data from yfinance and publishes each record as a JSON
message to Kafka:
  * daily OHLCV price bars  -> PRICES_TOPIC
  * recent news headlines   -> NEWS_TOPIC

yfinance is free and needs no API key. This is the "E" (extract) of the ETL.
"""
import json
import os
import time

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---- config from environment (see .env) -----------------------------------
TICKERS = [t.strip() for t in os.getenv("TICKERS", "AAPL,MSFT,GOOGL").split(",") if t.strip()]
PRICE_PERIOD = os.getenv("PRICE_PERIOD", "2y")
PRICE_INTERVAL = os.getenv("PRICE_INTERVAL", "1d")
PRICES_TOPIC = os.getenv("PRICES_TOPIC", "prices")
NEWS_TOPIC = os.getenv("NEWS_TOPIC", "news")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")


def connect_producer(retries=10, delay=5):
    """Kafka may still be starting when we run; retry until the broker answers."""
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready (attempt {attempt}/{retries}), waiting {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after several attempts.")


def send_prices(producer, ticker):
    """Download price history and publish one JSON message per trading day."""
    df = yf.Ticker(ticker).history(period=PRICE_PERIOD, interval=PRICE_INTERVAL)
    if df.empty:
        print(f"[producer] no price data for {ticker}")
        return 0
    df = df.reset_index()  # move the Date index into a column
    count = 0
    for row in df.itertuples(index=False):
        record = {
            "ticker": ticker,
            "date": str(row.Date.date()) if hasattr(row.Date, "date") else str(row.Date),
            "open": float(row.Open),
            "high": float(row.High),
            "low": float(row.Low),
            "close": float(row.Close),
            "volume": int(row.Volume),
        }
        producer.send(PRICES_TOPIC, key=ticker, value=record)
        count += 1
    print(f"[producer] {ticker}: sent {count} price records")
    return count


def send_news(producer, ticker):
    """Publish recent news headlines for a ticker (free-text field for NLP)."""
    try:
        items = yf.Ticker(ticker).news or []
    except Exception as exc:  # yfinance news can be flaky; don't crash the run
        print(f"[producer] news fetch failed for {ticker}: {exc}")
        return 0
    count = 0
    for item in items:
        # yfinance has used two shapes over time; handle both defensively.
        content = item.get("content", item)
        title = content.get("title") or item.get("title")
        if not title:
            continue
        provider = content.get("provider") or {}
        publisher = provider.get("displayName") if isinstance(provider, dict) else item.get("publisher")
        pub_time = (
            content.get("pubDate")
            or content.get("displayTime")
            or item.get("providerPublishTime")
        )
        record = {
            "ticker": ticker,
            "title": title,
            "publisher": publisher,
            "published": str(pub_time) if pub_time is not None else None,
        }
        producer.send(NEWS_TOPIC, key=ticker, value=record)
        count += 1
    print(f"[producer] {ticker}: sent {count} news records")
    return count


def main():
    producer = connect_producer()
    total_prices = total_news = 0
    for ticker in TICKERS:
        total_prices += send_prices(producer, ticker)
        total_news += send_news(producer, ticker)
    producer.flush()
    producer.close()
    print(f"[producer] DONE. prices={total_prices} news={total_news} "
          f"across {len(TICKERS)} tickers.")


if __name__ == "__main__":
    main()
