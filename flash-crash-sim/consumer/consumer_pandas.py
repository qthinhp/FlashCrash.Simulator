"""
Pandas consumer. Drop-in replacement for consumer.py.

Same micro-batch shape as the Spark version:
  1. Pull up to N messages from RabbitMQ with pika.
  2. Build a pandas DataFrame from the batch.
  3. Insert raw ticks into `ticks`.
  4. Aggregate into 1-second OHLC candles, UPSERT into `candles_1s`.
  5. Sleep, repeat.

If you later need Spark for larger backtests, run it as an offline batch over
the `ticks` table — that's where Spark actually pays off.
"""

import json
import os
import time
from datetime import datetime

import pandas as pd
import pika
import psycopg2
from psycopg2.extras import execute_values

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_QUEUE = os.getenv("RABBITMQ_QUEUE", "ticks.queue")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "flashcrash")
PG_USER = os.getenv("POSTGRES_USER", "fcs")
PG_PWD = os.getenv("POSTGRES_PASSWORD", "fcs")

BATCH_MAX = int(os.getenv("BATCH_MAX", "1000"))
BATCH_INTERVAL_SEC = float(os.getenv("BATCH_INTERVAL_SEC", "2.0"))


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PWD
    )


def drain_rabbit(channel, max_msgs):
    msgs, tags = [], []
    for _ in range(max_msgs):
        method, _props, body = channel.basic_get(queue=RABBIT_QUEUE, auto_ack=False)
        if method is None:
            break
        try:
            msgs.append(json.loads(body.decode("utf-8")))
            tags.append(method.delivery_tag)
        except Exception as e:
            print(f"bad message, discarding: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)
    return msgs, tags


def insert_ticks(conn, rows):
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ticks (ts, symbol, side, price, size, order_type, order_id)
            VALUES %s
            """,
            rows,
            page_size=500,
        )
    conn.commit()


def upsert_candles(conn, candles):
    if not candles:
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO candles_1s (ts_bucket, symbol, open, high, low, close, volume, tick_count)
            VALUES %s
            ON CONFLICT (ts_bucket, symbol) DO UPDATE SET
                high       = GREATEST(candles_1s.high, EXCLUDED.high),
                low        = LEAST(candles_1s.low, EXCLUDED.low),
                close      = EXCLUDED.close,
                volume     = candles_1s.volume     + EXCLUDED.volume,
                tick_count = candles_1s.tick_count + EXCLUDED.tick_count
            """,
            candles,
            page_size=500,
        )
    conn.commit()


def aggregate_candles(msgs):
    """Group ticks into 1s OHLC candles. Exclude CANCELs (no execution price)."""
    df = pd.DataFrame(msgs)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df[df["order_type"] != "CANCEL"].copy()
    if df.empty:
        return []
    df["ts_bucket"] = df["ts"].dt.floor("s")
    df = df.sort_values("ts")

    grouped = df.groupby(["ts_bucket", "symbol"], as_index=False).agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
        tick_count=("price", "count"),
    )
    return [
        (r["ts_bucket"].to_pydatetime(), r["symbol"],
         float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
         int(r["volume"]), int(r["tick_count"]))
        for _, r in grouped.iterrows()
    ]


def main():
    print(f"Connecting to RabbitMQ at {RABBIT_HOST}...")
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, heartbeat=30, blocked_connection_timeout=30
    )
    rabbit = pika.BlockingConnection(params)
    channel = rabbit.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

    print("Connecting to Postgres...")
    pg = get_pg_conn()
    print("Consumer ready. Draining...")

    try:
        while True:
            t0 = time.time()
            msgs, tags = drain_rabbit(channel, BATCH_MAX)

            if not msgs:
                time.sleep(BATCH_INTERVAL_SEC)
                continue

            # Parse timestamps
            for m in msgs:
                # ISO strings come from the C# producer
                if isinstance(m["ts"], str):
                    m["ts"] = datetime.fromisoformat(m["ts"].replace("Z", "+00:00"))

            tick_rows = [
                (m["ts"], m["symbol"], m["side"], m["price"], m["size"],
                 m["order_type"], m["order_id"])
                for m in msgs
            ]
            insert_ticks(pg, tick_rows)

            candles = aggregate_candles(msgs)
            upsert_candles(pg, candles)

            for tag in tags:
                channel.basic_ack(delivery_tag=tag)

            elapsed = time.time() - t0
            print(f"batch: msgs={len(msgs)} candles={len(candles)} in {elapsed:.2f}s")

            remaining = BATCH_INTERVAL_SEC - elapsed
            if remaining > 0:
                time.sleep(remaining)

    except KeyboardInterrupt:
        print("Interrupted, shutting down.")
    finally:
        try:
            rabbit.close()
        except Exception:
            pass
        pg.close()


if __name__ == "__main__":
    main()
