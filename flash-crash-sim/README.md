# Flash Crash Simulator — MVP

End-to-end pipeline: **C# producer → RabbitMQ → PySpark consumer → Postgres → Streamlit dashboard.**

This is the MVP skeleton. No spoofing detection yet; that comes next.

## Prereqs

- Docker + Docker Compose
- .NET 8 SDK (for the producer)
- Python 3.10+ with Java 11/17 available on PATH (for PySpark)

## Run it

### 1. Start infrastructure
```bash
cd flash-crash-sim
docker compose up -d
# RabbitMQ UI: http://localhost:15672  (guest / guest)
# Postgres:   localhost:5432  (fcs / fcs / flashcrash)
```

Wait ~10 seconds for both to be healthy.

### 2. Start the consumer (PySpark)
```bash
cd consumer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python consumer.py
```

You should see `Consumer ready. Draining...` and then `batch: msgs=0 ...` until the producer starts.

### 3. Start the producer (C#)
In a new terminal:
```bash
cd producer
dotnet run
```

You should see `sent=500 price=...` messages.

### 4. Start the dashboard
In a new terminal:
```bash
cd dashboard
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

Open http://localhost:8501.

## Architecture

```
 ┌──────────────┐   AMQP    ┌────────────┐  basic_get   ┌────────────────┐
 │ C# Producer  │──────────▶│ RabbitMQ   │─────────────▶│ PySpark        │
 │ (random walk)│           │ ticks.queue│              │ consumer       │
 └──────────────┘           └────────────┘              │ (micro-batch)  │
                                                        └───────┬────────┘
                                                                │ JDBC/psycopg2
                                                                ▼
                                                        ┌────────────────┐
                                                        │ PostgreSQL     │
                                                        │ ticks,         │
                                                        │ candles_1s     │
                                                        └───────┬────────┘
                                                                │ SELECT
                                                                ▼
                                                        ┌────────────────┐
                                                        │ Streamlit +    │
                                                        │ Plotly         │
                                                        └────────────────┘
```

## Honest caveats

- **Not real Structured Streaming.** Spark has no native RabbitMQ source, so
  the consumer uses `pika.basic_get` to drain the queue in micro-batches and
  hands each batch to Spark. It's stream-*ish*.
- **Resume bullet rewrite** (fair to what this code actually does):
  *"Built an event-driven pipeline: C# produces simulated order ticks into
  RabbitMQ; a PySpark micro-batch consumer aggregates 1s OHLC candles and
  lands them in Postgres; Streamlit + Plotly renders the live book."*

## What's next

1. Spoofing pattern generator in the producer (bursts of large LIMITs followed
   by rapid CANCELs).
2. Detection rules in the consumer (cancel-to-fill ratio, order-size z-score).
3. Flash Crash scenario script (replay 2010-05-06 price arc).
