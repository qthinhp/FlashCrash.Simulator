"""
Micro-batch consumer:
  1. Pull up to N messages from RabbitMQ with pika (non-blocking get).
  2. Build a Spark DataFrame from the batch.
  3. Insert raw ticks into `ticks`.
  4. Aggregate into 1-second OHLC candles, UPSERT into `candles_1s`.
  5. Sleep, repeat.

Not real Structured Streaming — Rabbit has no native Spark source. This is an
honest micro-batch bridge. Rename to "streaming" in casual speech at your peril.
"""

import json
import os
import time
from datetime import datetime, timezone

import pika
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType, DoubleType,
)

RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_QUEUE = os.getenv("RABBITMQ_QUEUE", "ticks.queue")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "flashcrash")
PG_USER = os.getenv("POSTGRES_USER", "fcs")
PG_PWD = os.getenv("POSTGRES_PASSWORD", "fcs")

BATCH_MAX = int(os.getenv("BATCH_MAX", "1000"))
BATCH_INTERVAL_SEC = float(os.getenv("BATCH_INTERVAL_SEC", "2.0"))

TICK_SCHEMA = StructType([
    StructField("ts", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("side", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("size", IntegerType(), False),
    StructField("order_type", StringType(), False),
    StructField("order_id", StringType(), False),
])


def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PWD
    )


def drain_rabbit(channel, max_msgs):
    """Non-blocking basic_get until queue empty or max reached."""
    msgs = []
    delivery_tags = []
    for _ in range(max_msgs):
        method, _props, body = channel.basic_get(queue=RABBIT_QUEUE, auto_ack=False)
        if method is None:
            break
        try:
            msgs.append(json.loads(body.decode("utf-8")))
            delivery_tags.append(method.delivery_tag)
        except Exception as e:
            print(f"bad message, discarding: {e}")
            channel.basic_nack(method.delivery_tag, requeue=False)
    return msgs, delivery_tags


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
                high = GREATEST(candles_1s.high, EXCLUDED.high),
                low  = LEAST(candles_1s.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = candles_1s.volume + EXCLUDED.volume,
                tick_count = candles_1s.tick_count + EXCLUDED.tick_count
            """,
            candles,
            page_size=500,
        )
    conn.commit()


def main():
    print("Starting Spark session...")
    spark = (
        SparkSession.builder.appName("FlashCrashConsumer")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Connecting to RabbitMQ at {RABBIT_HOST}...")
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, heartbeat=30, blocked_connection_timeout=30
    )
    rabbit = pika.BlockingConnection(params)
    channel = rabbit.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

    pg = get_pg_conn()
    print("Consumer ready. Draining...")

    try:
        while True:
            t0 = time.time()
            msgs, tags = drain_rabbit(channel, BATCH_MAX)

            if not msgs:
                time.sleep(BATCH_INTERVAL_SEC)
                continue

            # Normalize timestamps to datetime for Spark
            for m in msgs:
                m["ts"] = datetime.fromisoformat(m["ts"].replace("Z", "+00:00"))

            df = spark.createDataFrame(msgs, schema=TICK_SCHEMA)

            # Raw tick rows for insert
            tick_rows = [
                (m["ts"], m["symbol"], m["side"], m["price"], m["size"],
                 m["order_type"], m["order_id"])
                for m in msgs
            ]
            insert_ticks(pg, tick_rows)

            # 1-second OHLC aggregation (exclude CANCELs — no execution price)
            agg = (
                df.filter(F.col("order_type") != "CANCEL")
                  .withColumn("ts_bucket", F.date_trunc("second", F.col("ts")))
                  .groupBy("ts_bucket", "symbol")
                  .agg(
                      F.first("price").alias("open"),
                      F.max("price").alias("high"),
                      F.min("price").alias("low"),
                      F.last("price").alias("close"),
                      F.sum("size").alias("volume"),
                      F.count("*").alias("tick_count"),
                  )
            )
            candle_rows = [
                (r["ts_bucket"], r["symbol"], float(r["open"]), float(r["high"]),
                 float(r["low"]), float(r["close"]), int(r["volume"]), int(r["tick_count"]))
                for r in agg.collect()
            ]
            upsert_candles(pg, candle_rows)

            # Ack everything only after DB commit succeeded
            for tag in tags:
                channel.basic_ack(delivery_tag=tag)

            elapsed = time.time() - t0
            print(f"batch: msgs={len(msgs)} candles={len(candle_rows)} in {elapsed:.2f}s")

            # Pace the loop
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
        spark.stop()


if __name__ == "__main__":
    main()
