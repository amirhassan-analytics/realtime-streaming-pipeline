"""Unit tests for the tick producer and data-quality rules."""
from src.data_quality import is_valid_tick
from src.producer import make_tick


def test_make_tick_shape():
    tick = make_tick("AAPL", 211.35)
    assert tick["symbol"] == "AAPL"
    assert tick["price"] == 211.35
    assert isinstance(tick["volume"], int) and tick["volume"] >= 10
    assert tick["ts"]


def test_valid_tick_passes():
    ok, _ = is_valid_tick({"symbol": "MSFT", "price": 430.0, "volume": 100, "ts": "2026-11-12T15:00:01"})
    assert ok


def test_invalid_price_fails():
    ok, reason = is_valid_tick({"symbol": "MSFT", "price": -5, "volume": 100, "ts": "2026-11-12T15:00:01"})
    assert not ok and reason == "invalid price"


def test_missing_symbol_fails():
    ok, _ = is_valid_tick({"price": 430.0, "volume": 100, "ts": "2026-11-12T15:00:01"})
    assert not ok
