"""
Streamlit dashboard (RESULTS stage) -- owned by Person C.

Reads the enriched tables from Elasticsearch and presents the findings:
  * price line with MLlib-flagged anomalies marked
  * news headlines with sentiment
  * do sentiment swings line up with the price anomalies?
"""
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from elasticsearch import Elasticsearch

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
PRICES_INDEX = os.getenv("PRICES_INDEX", "stock_prices")
NEWS_INDEX = os.getenv("NEWS_INDEX", "stock_news")


@st.cache_resource
def get_es():
    return Elasticsearch(ES_HOST)


@st.cache_data(ttl=60)
def load_index(index):
    """Pull all docs from an index into a DataFrame (small demo dataset)."""
    es = get_es()
    if not es.indices.exists(index=index):
        return pd.DataFrame()
    resp = es.search(index=index, query={"match_all": {}}, size=10000)
    rows = [hit["_source"] for hit in resp["hits"]["hits"]]
    return pd.DataFrame(rows)


st.set_page_config(page_title="Stock Anomalies & News Sentiment", layout="wide")
st.title("📈 Stock Anomaly Detection & News Sentiment")

prices = load_index(PRICES_INDEX)
news = load_index(NEWS_INDEX)

if prices.empty:
    st.warning("No price data in Elasticsearch yet. Run the producer and the "
               "Spark pipeline first, then reload.")
    st.stop()

prices["date"] = pd.to_datetime(prices["date"])
tickers = sorted(prices["ticker"].unique())
ticker = st.sidebar.selectbox("Ticker", tickers)

pdf = prices[prices["ticker"] == ticker].sort_values("date")
anomalies = pdf[pdf["is_anomaly"] == True]  # noqa: E712

# ---- KPIs -----------------------------------------------------------------
c1, c2, c3 = st.columns(3)
c1.metric("Trading days", len(pdf))
c2.metric("Anomalies flagged", len(anomalies))
c3.metric("Avg daily return", f"{pdf['daily_return'].mean() * 100:.2f}%")

# ---- price chart with anomalies -------------------------------------------
st.subheader(f"{ticker} — closing price with anomalies")
fig = go.Figure()
fig.add_trace(go.Scatter(x=pdf["date"], y=pdf["close"], mode="lines", name="Close"))
fig.add_trace(go.Scatter(
    x=anomalies["date"], y=anomalies["close"], mode="markers",
    name="Anomaly", marker=dict(color="red", size=9, symbol="x"),
))
fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
st.plotly_chart(fig, use_container_width=True)

# ---- news sentiment -------------------------------------------------------
st.subheader("News sentiment")
if news.empty:
    st.info("No news in Elasticsearch for this run.")
else:
    ndf = news[news["ticker"] == ticker].copy()
    if ndf.empty:
        st.info(f"No news headlines for {ticker}.")
    else:
        avg = ndf["sentiment"].mean()
        st.metric("Average headline sentiment", f"{avg:+.3f}")
        st.dataframe(
            ndf[["title", "publisher", "sentiment", "sentiment_label"]]
            .sort_values("sentiment"),
            use_container_width=True, hide_index=True,
        )

# ---- the interesting cross-view -------------------------------------------
with st.expander("What are the flagged anomaly days?"):
    st.dataframe(
        anomalies[["date", "close", "daily_return", "volume_change",
                   "volatility_10d", "anomaly_score"]]
        .sort_values("anomaly_score", ascending=False),
        use_container_width=True, hide_index=True,
    )
