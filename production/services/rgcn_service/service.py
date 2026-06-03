from __future__ import annotations

import pandas as pd

from services.rgcn_service.model_loader import load_active_model
from services.query_parser.parser import normalize_query
from services.distance import as_float, distance_km, distance_meters
from settings import Settings


def _split_tokens(value: str) -> set[str]:
    return {normalize_query(part.strip()) for part in str(value or "").split("|") if part.strip()}


def _price_bucket(max_price: int | None) -> str | None:
    if max_price is None:
        return None
    if max_price <= 50000:
        return "under_50k"
    if max_price <= 100000:
        return "50k_100k"
    return "over_100k"


class RGCNService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.model_config = load_active_model(settings.paths.models_root)
        processed_dirs = [path for path in settings.paths.processed_root.iterdir() if path.is_dir()]
        self.processed_dir = sorted(processed_dirs)[-1] if processed_dirs else None
        self.scenarios = pd.read_csv(self.processed_dir / "scenario_features.csv").fillna("") if self.processed_dir and (self.processed_dir / "scenario_features.csv").exists() else pd.DataFrame()

    def recommend(self, query: str, rules: dict, top_k: int = 5) -> list[dict]:
        if self.scenarios.empty:
            return []
        food_tokens = _split_tokens(rules.get("food") or query)
        preferred_aspects = _split_tokens("|".join(rules.get("priority", [])))
        price_range = _price_bucket(rules.get("max_price"))
        query_lat = rules.get("query_lat")
        query_lng = rules.get("query_lng")
        tolerance_m = float(rules.get("distance_tolerance_m") or 1500.0)
        wants_distance = "distance" in (rules.get("priority") or [])

        rows = []
        for _, row in self.scenarios.iterrows():
            score = 0.0
            term_tokens = _split_tokens(row.get("term", ""))
            aspect_tokens = _split_tokens(row.get("preferred_aspects", ""))
            if food_tokens and food_tokens & term_tokens:
                score += 1.2
            if price_range and str(row.get("desired_price_range_id", "")) == price_range:
                score += 0.5
            if preferred_aspects and preferred_aspects & aspect_tokens:
                score += 0.4
            distance_m = distance_meters(query_lat, query_lng, row.get("latitude"), row.get("longitude"))
            if distance_m is None:
                distance_m = as_float(row.get("distance_m"))
            if wants_distance and distance_m is not None:
                score += max(0.0, 1.0 - min(distance_m / max(tolerance_m, 1.0), 1.0))
            score += float(row.get("relevance_weight", 0) or 0)
            score += min(float(row.get("rating", 0) or 0) / 10.0, 0.5)
            if score <= 0:
                continue
            rows.append(
                {
                    "restaurant_id": str(row.get("store_id", "")).strip(),
                    "name": str(row.get("store_name", "")).strip(),
                    "rgcn_score": round(score, 6),
                    "distance_m": round(distance_m, 1) if distance_m is not None else "",
                    "distance_km": distance_km(distance_m),
                    "latitude": as_float(row.get("latitude")),
                    "longitude": as_float(row.get("longitude")),
                    "reason": str(row.get("reason", "")),
                }
            )
        deduped: dict[str, dict] = {}
        for row in sorted(rows, key=lambda item: item["rgcn_score"], reverse=True):
            deduped.setdefault(row["restaurant_id"], row)
        return list(deduped.values())[:top_k]
