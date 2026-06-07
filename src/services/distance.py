from __future__ import annotations

import math
from typing import Any


def as_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    radius_m = 6371000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return radius_m * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def distance_meters(user_lat: Any, user_lng: Any, store_lat: Any, store_lng: Any) -> float | None:
    lat1 = as_float(user_lat)
    lng1 = as_float(user_lng)
    lat2 = as_float(store_lat)
    lng2 = as_float(store_lng)
    if lat1 is None or lng1 is None or lat2 is None or lng2 is None:
        return None
    return haversine_m(lat1, lng1, lat2, lng2)


def distance_km(value_m: float | None) -> float | None:
    if value_m is None:
        return None
    return round(value_m / 1000.0, 3)
