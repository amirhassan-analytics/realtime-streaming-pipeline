# Real-Time Streaming Pipeline — Kafka + Spark Structured Streaming

Live event pipeline: a **producer** simulates financial market ticks
(symbol, price, volume) and sends them to **Apache Kafka**; a
**Spark Structured Streaming** job reads the topic, validates and
aggregates the events in 1-minute windows, and loads the results into
**PostgreSQL** — ready for a **Power BI** dashboard on top.

## What it proves

- **Streaming:** Kafka producer/consumer, Spark Structured Streaming
  with watermarking and windowed aggregations
- **Exactly-once thinking:** checkpointing, idempotent upserts into
  PostgreSQL (`ON CONFLICT DO UPDATE`)
- **Data quality:** malformed events are quarantined, never loaded
- **End-to-end:** event → Kafka → Spark → warehouse → dashboard

## Architecture

```
producer.py (simulated market ticks, 1/sec)
      │  Kafka topic: market_ticks
      ▼
streaming.py (Spark Structured Streaming)
      │  validate, watermark 5 min, window 1 min
      ▼
PostgreSQL — fact_ticks (raw) + mart_minute_bars (aggregated)
      │
      ▼
Power BI (see dashboard/README.md)
```

## Quick start

```bash
# 1. Start Kafka + PostgreSQL
docker compose up -d zookeeper kafka postgres

# 2. Create the topic
docker compose exec kafka kafka-topics.sh --create \
  --topic market_ticks --bootstrap-server kafka:9092 \
  --partitions 1 --replication-factor 1

# 3. Start the producer (in a second terminal)
pip install -r requirements.txt
python -m src.producer

# 4. Start the streaming job (in a third terminal)
python -m src.streaming

# 5. Watch the data land
docker compose exec postgres psql -U stream -d stream -c \
  "SELECT * FROM mart_minute_bars ORDER BY window_start DESC LIMIT 5;"
```

Or run everything with Docker Compose (producer + streaming as services):

```bash
docker compose up --build
```

## Project structure

```
├── README.md
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── src/
│   ├── config.py
│   ├── producer.py
│   ├── streaming.py
│   ├── data_quality.py
│   └── __init__.py
├── sql/
│   └── schema.sql
├── dashboard/
│   └── README.md
├── tests/
│   └── test_producer.py
└── .github/workflows/ci.yml
```

## Tech

Python · Apache Kafka · Spark Structured Streaming · PostgreSQL ·
Docker · Power BI
