"""
Live-ish candlestick dashboard. Polls Postgres every few seconds.
"""

import os
import time

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "flashcrash")
PG_USER = os.getenv("POSTGRES_USER", "fcs")
PG_PWD = os.getenv("POSTGRES_PASSWORD", "fcs")

st.set_page_config(page_title="Flash Crash Sim", layout="wide")
st.title("Flash Crash Simulator — Live Market View")

refresh_sec = st.sidebar.slider("Refresh (sec)", 1, 10, 3)
lookback_min = st.sidebar.slider("Lookback (minutes)", 1, 30, 5)
symbol = st.sidebar.selectbox("Symbol", ["ACME"])


@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PWD
    )


def fetch_candles(conn, symbol, lookback_min):
    q = """
        SELECT ts_bucket, open, high, low, close, volume, tick_count
        FROM candles_1s
        WHERE symbol = %s
          AND ts_bucket >= now() - (%s || ' minutes')::interval
        ORDER BY ts_bucket
    """
    return pd.read_sql(q, conn, params=(symbol, str(lookback_min)))


def fetch_tick_stats(conn, symbol, lookback_min):
    q = """
        SELECT
            count(*)                                  AS total_ticks,
            count(*) FILTER (WHERE order_type='CANCEL') AS cancels,
            count(*) FILTER (WHERE order_type='LIMIT')  AS limits,
            count(*) FILTER (WHERE order_type='MARKET') AS markets,
            avg(price)::float                         AS avg_price
        FROM ticks
        WHERE symbol = %s
          AND ts >= now() - (%s || ' minutes')::interval
    """
    with conn.cursor() as cur:
        cur.execute(q, (symbol, str(lookback_min)))
        row = cur.fetchone()
    return {
        "total_ticks": row[0] or 0,
        "cancels": row[1] or 0,
        "limits": row[2] or 0,
        "markets": row[3] or 0,
        "avg_price": row[4] or 0.0,
    }


placeholder = st.empty()

while True:
    try:
        conn = get_conn()
        candles = fetch_candles(conn, symbol, lookback_min)
        stats = fetch_tick_stats(conn, symbol, lookback_min)
    except Exception as e:
        st.error(f"DB error: {e}")
        time.sleep(refresh_sec)
        continue

    with placeholder.container():
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Ticks", f"{stats['total_ticks']:,}")
        c2.metric("Limits", f"{stats['limits']:,}")
        c3.metric("Markets", f"{stats['markets']:,}")
        c4.metric("Cancels", f"{stats['cancels']:,}")
        cancel_ratio = (stats['cancels'] / stats['total_ticks']) if stats['total_ticks'] else 0
        c5.metric("Cancel %", f"{cancel_ratio*100:.1f}%")

        if candles.empty:
            st.info("No candles yet. Make sure producer and consumer are running.")
        else:
            fig = go.Figure(data=[go.Candlestick(
                x=candles["ts_bucket"],
                open=candles["open"],
                high=candles["high"],
                low=candles["low"],
                close=candles["close"],
                name=symbol,
            )])
            fig.update_layout(
                height=500,
                xaxis_rangeslider_visible=False,
                margin=dict(l=10, r=10, t=30, b=10),
                title=f"{symbol} — 1s candles",
            )
            st.plotly_chart(fig, use_container_width=True)

            vol_fig = go.Figure(data=[go.Bar(x=candles["ts_bucket"], y=candles["volume"])])
            vol_fig.update_layout(height=200, margin=dict(l=10, r=10, t=30, b=10), title="Volume")
            st.plotly_chart(vol_fig, use_container_width=True)

    time.sleep(refresh_sec)
