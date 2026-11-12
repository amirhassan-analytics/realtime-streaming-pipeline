"""Central settings for the real-time streaming pipeline."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market_ticks")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://stream:stream@localhost:5432/stream",
)

# Simulated market symbols and their starting prices.
SYMBOLS = {
    "AAPL": 210.0,
    "MSFT": 430.0,
    "NVDA": 135.0,
    "AMZN": 195.0,
    "TSLA": 250.0,
}

PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))
WINDOW_MINUTES = 1
WATERMARK_MINUTES = 5
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
