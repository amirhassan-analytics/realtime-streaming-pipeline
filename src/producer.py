"""Producer: simulate financial market ticks and send them to Kafka.

Each tick is a JSON event:
  {"symbol": "AAPL", "price": 211.35, "volume": 120, "ts": "2026-11-12T15:00:01"}
Prices follow a small random walk from the starting price in config.
Run: python -m src.producer
"""
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from .config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, PRODUCER_INTERVAL_SECONDS, SYMBOLS


def make_tick(symbol: str, price: float) -> dict:
    return {
        "symbol": symbol,
        "price": round(price, 2),
        "volume": random.randint(10, 500),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    prices = dict(SYMBOLS)
    print(f"[producer] sending ticks to {KAFKA_TOPIC} on {KAFKA_BOOTSTRAP}")
    try:
        while True:
            for symbol in prices:
                prices[symbol] *= 1 + random.uniform(-0.002, 0.002)
                tick = make_tick(symbol, prices[symbol])
                producer.send(KAFKA_TOPIC, value=tick)
                print(f"[producer] {tick}")
            producer.flush()
            time.sleep(PRODUCER_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("[producer] stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
