"""Data quality rules for a tick event — shared by producer tests and
the streaming job's validation logic."""
from typing import Any


def is_valid_tick(tick: dict[str, Any]) -> tuple[bool, str]:
    if not tick.get("symbol"):
        return False, "missing symbol"
    price = tick.get("price")
    if price is None or not isinstance(price, (int, float)) or price <= 0:
        return False, "invalid price"
    volume = tick.get("volume")
    if volume is None or not isinstance(volume, int) or volume < 0:
        return False, "invalid volume"
    if not tick.get("ts"):
        return False, "missing timestamp"
    return True, "ok"
