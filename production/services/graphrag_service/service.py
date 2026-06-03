from __future__ import annotations

import ast
import json
from pathlib import Path

import pandas as pd

from services.graphrag_service.context_builder import build_context
from services.graphrag_service.retriever import LocalVectorRetriever
from services.distance import as_float, distance_km, distance_meters
from settings import Settings


def _parse_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except Exception:
            return [text]
    return [item.strip() for item in text.split("|") if item.strip()]


class GraphRAGService:
    def __init__(self, settings: Settings):
        active_index = json.loads((settings.paths.data_lake_root / "vector_index" / "ACTIVE_INDEX.json").read_text(encoding="utf-8"))
        self.retriever = LocalVectorRetriever(Path(active_index["index_path"]))
        processed_dirs = [path for path in settings.paths.processed_root.iterdir() if path.is_dir()]
        processed_dir = sorted(processed_dirs)[-1]
        self.restaurants = pd.read_csv(processed_dir / "canonical_restaurants.csv").fillna("")
        self.menu_items = pd.read_csv(processed_dir / "menu_items_enriched.csv").fillna("") if (processed_dir / "menu_items_enriched.csv").exists() else pd.DataFrame()
        self.text_units = pd.read_csv(processed_dir / "text_units.csv").fillna("") if (processed_dir / "text_units.csv").exists() else pd.DataFrame()
        self.entities = pd.read_csv(processed_dir / "extracted_entities.csv").fillna("") if (processed_dir / "extracted_entities.csv").exists() else pd.DataFrame()
        self.community_reports = pd.read_csv(processed_dir / "community_reports.csv").fillna("") if (processed_dir / "community_reports.csv").exists() else pd.DataFrame()

    def _restaurant_payload(self, restaurant_id: str) -> dict:
        matched = self.restaurants[self.restaurants["restaurant_id"].astype(str) == restaurant_id]
        if matched.empty:
            return {}
        row = matched.iloc[0]
        menu_rows = self.menu_items[self.menu_items["store_key"].astype(str) == restaurant_id] if "store_key" in self.menu_items.columns else pd.DataFrame()
        text_rows = self.text_units[self.text_units["store_key"].astype(str) == restaurant_id] if "store_key" in self.text_units.columns else pd.DataFrame()
        entity_rows = self.entities[self.entities["store_key"].astype(str) == restaurant_id] if "store_key" in self.entities.columns else pd.DataFrame()
        evidence = []
        for _, menu_row in menu_rows.head(3).iterrows():
            evidence.append(
                {
                    "source": "menu",
                    "source_record_id": str(menu_row.get("menu_item_id", menu_row.get("restaurant_item_id", ""))),
                    "field": "menu_items",
                    "value": str(menu_row.get("item_name", "")),
                }
            )
        for _, text_row in text_rows.head(3).iterrows():
            evidence.append(
                {
                    "source": "review",
                    "source_record_id": str(text_row.get("text_unit_id", "")),
                    "field": "text_units",
                    "value": str(text_row.get("chunk_text", ""))[:220],
                }
            )
        return {
            "restaurant_id": restaurant_id,
            "name": row["name"],
            "matched_items": [
                {"name": str(menu_row.get("item_name", "")), "price": menu_row.get("price")}
                for _, menu_row in menu_rows.head(5).iterrows()
            ],
            "categories": _parse_list(row.get("categories", "")),
            "dish_families": _parse_list(menu_rows["dish_family"].tolist() if "dish_family" in menu_rows.columns else []),
            "review_count": float(row.get("review_count", 0) or 0),
            "rating": float(row.get("rating", 0) or 0),
            "latitude": as_float(row.get("latitude")),
            "longitude": as_float(row.get("longitude")),
            "distance_m": None,
            "distance_km": as_float(row.get("distance_km")),
            "evidence": evidence,
            "extracted_entities": [
                {"name": str(ent.get("name", "")), "type": str(ent.get("entity_type", ""))}
                for _, ent in entity_rows.head(5).iterrows()
            ],
        }

    def recommend(self, query: str, rules: dict, top_k: int = 5) -> list[dict]:
        candidates = self.retriever.search(query, top_k=max(top_k * 10, 40))
        grouped: dict[str, dict] = {}
        query_lat = rules.get("query_lat")
        query_lng = rules.get("query_lng")
        tolerance_m = float(rules.get("distance_tolerance_m") or 1500.0)
        wants_distance = "distance" in (rules.get("priority") or [])
        for candidate in candidates:
            restaurant_id = str(candidate.get("restaurant_id", "")).strip()
            if not restaurant_id:
                continue
            payload = grouped.get(restaurant_id) or self._restaurant_payload(restaurant_id)
            if not payload:
                continue
            payload.setdefault("graphrag_score", 0.0)
            payload["graphrag_score"] += float(candidate.get("graphrag_score", 0.0))
            distance_m = distance_meters(query_lat, query_lng, payload.get("latitude"), payload.get("longitude"))
            if distance_m is not None:
                payload["distance_m"] = round(distance_m, 1)
                payload["distance_km"] = distance_km(distance_m)
                if wants_distance:
                    distance_score = max(0.0, 1.0 - min(distance_m / max(tolerance_m, 1.0), 1.0))
                    payload["rule_score"] = payload.get("rule_score", 0.0) + distance_score
            if rules.get("food"):
                food = str(rules["food"])
                payload["rule_score"] = payload.get("rule_score", 0.0) + (
                    1.0
                    if any(food in str(item.get("name", "")).lower() for item in payload["matched_items"])
                    else 0.0
                )
            else:
                payload["rule_score"] = payload.get("rule_score", 0.0)
            payload["popularity_score"] = min(payload.get("review_count", 0.0) / 1000.0, 1.0)
            if rules.get("max_price"):
                matched_prices = [float(item.get("price") or 0) for item in payload["matched_items"] if item.get("price")]
                if matched_prices and min(matched_prices) <= float(rules["max_price"]):
                    payload["rule_score"] += 0.5
            grouped[restaurant_id] = payload
        ranked = list(grouped.values())
        ranked.sort(key=lambda item: (float(item.get("graphrag_score", 0.0)) + float(item.get("rule_score", 0.0))), reverse=True)
        build_context(query, ranked)
        return ranked[:top_k]
