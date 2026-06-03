from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import ensure_dir, load_json, dump_json, utc_now_iso
from pipelines.storage.minio_client import MinioStorageAdapter
from services.distance import as_float, distance_meters


SCENARIO_COLUMNS = [
    "query_node_id",
    "user_id",
    "time_query",
    "query_lat",
    "query_lng",
    "area_id",
    "query_location_hint",
    "time_slot_id",
    "term",
    "desired_price_range_id",
    "preferred_aspects",
    "raw_preferred_aspects",
    "distance_tolerance_m",
    "store_node_id",
    "store_id",
    "store_name",
    "rank",
    "relevance_score",
    "relevance_weight",
    "distance_m",
    "rating",
    "review_count",
    "median_price",
    "latitude",
    "longitude",
    "is_closed",
    "reason",
    "raw_query",
    "normalized_query",
    "semantic_chunks_json",
    "label_source",
    "clicked",
    "created_at",
]


def _today_dir(root: Path) -> Path:
    return ensure_dir(root / datetime.now(UTC).strftime("%Y-%m-%d"))


def _append_csv(path: Path, row: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    exists = path.exists() and path.stat().st_size > 0
    if exists:
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            if reader.fieldnames != SCENARIO_COLUMNS:
                existing_rows = list(reader)
                with path.open("w", encoding="utf-8-sig", newline="") as output:
                    writer = csv.DictWriter(output, fieldnames=SCENARIO_COLUMNS, extrasaction="ignore")
                    writer.writeheader()
                    for existing_row in existing_rows:
                        writer.writerow({column: existing_row.get(column, "") for column in SCENARIO_COLUMNS})
    with path.open("a", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=SCENARIO_COLUMNS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow({column: row.get(column, "") for column in SCENARIO_COLUMNS})


def _price_range(max_price: Any) -> str:
    try:
        value = float(max_price)
    except (TypeError, ValueError):
        return ""
    if value <= 50000:
        return "under_50k"
    if value <= 100000:
        return "50k_100k"
    return "over_100k"


def _term_from_rules(rules: dict[str, Any], raw_query: str) -> str:
    food = rules.get("food")
    if food:
        return str(food)
    return "|".join(part for part in raw_query.lower().replace(",", " ").split() if len(part) >= 3)[:120]


def _result_lat_lng(result: dict[str, Any]) -> tuple[float | None, float | None]:
    return as_float(result.get("latitude") or result.get("lat")), as_float(result.get("longitude") or result.get("lng"))


def build_scenario_row(
    *,
    session_id: str,
    raw_query: str,
    rules: dict[str, Any],
    result: dict[str, Any],
    rank: int,
    normalized_query: str = "",
    clicked: bool = False,
    label_source: str = "shown_candidate",
) -> dict[str, Any]:
    restaurant_id = str(result.get("restaurant_id", ""))
    query_lat = as_float(rules.get("query_lat"))
    query_lng = as_float(rules.get("query_lng"))
    store_lat, store_lng = _result_lat_lng(result)
    distance_m = as_float(result.get("distance_m"))
    if distance_m is None:
        distance_m = distance_meters(query_lat, query_lng, store_lat, store_lng)
    tolerance_m = rules.get("distance_tolerance_m") or ""
    semantic_chunks = {
        "food": rules.get("food"),
        "location": rules.get("location"),
        "priority": rules.get("priority", []),
        "max_price": rules.get("max_price"),
        "cuisine": rules.get("cuisine"),
        "time_constraint": rules.get("time_constraint"),
        "normalized_query": normalized_query or rules.get("normalized_query"),
        "query_lat": query_lat,
        "query_lng": query_lng,
        "distance_tolerance_m": tolerance_m,
    }
    return {
        "query_node_id": f"query:{session_id}",
        "user_id": "",
        "time_query": utc_now_iso(),
        "query_lat": query_lat if query_lat is not None else "",
        "query_lng": query_lng if query_lng is not None else "",
        "area_id": str(rules.get("location") or ""),
        "query_location_hint": str(rules.get("location") or ""),
        "time_slot_id": str(rules.get("time_constraint") or ""),
        "term": _term_from_rules(rules, raw_query),
        "desired_price_range_id": _price_range(rules.get("max_price")),
        "preferred_aspects": "|".join(str(item) for item in (rules.get("priority") or [])),
        "raw_preferred_aspects": "|".join(str(item) for item in (rules.get("priority") or [])),
        "distance_tolerance_m": tolerance_m,
        "store_node_id": f"store:{restaurant_id}" if restaurant_id else "",
        "store_id": restaurant_id,
        "store_name": str(result.get("name", "")),
        "rank": rank,
        "relevance_score": 1.0 if clicked else "",
        "relevance_weight": 1.0 if clicked else "",
        "distance_m": round(distance_m, 1) if distance_m is not None else "",
        "rating": result.get("rating", ""),
        "review_count": result.get("review_count", ""),
        "median_price": "",
        "latitude": store_lat if store_lat is not None else "",
        "longitude": store_lng if store_lng is not None else "",
        "is_closed": "",
        "reason": str(result.get("reason") or result.get("evidence") or "")[:500],
        "raw_query": raw_query,
        "normalized_query": normalized_query or rules.get("normalized_query", ""),
        "semantic_chunks_json": json.dumps(semantic_chunks, ensure_ascii=False, sort_keys=True),
        "label_source": label_source,
        "clicked": "true" if clicked else "false",
        "created_at": utc_now_iso(),
    }


class ScenarioLogger:
    def __init__(self, data_lake_root: Path):
        self.root = data_lake_root / "user_scenarios"
        self.minio = MinioStorageAdapter()

    def _sync_to_minio(self, path: Path) -> None:
        day = path.parent.name
        key = f"user_scenarios/{day}/{path.name}"
        try:
            self.minio.put_file(path, key)
        except Exception:
            pass

    def log_shown_candidates(
        self,
        *,
        session_id: str,
        raw_query: str,
        rules: dict[str, Any],
        results: list[dict[str, Any]],
        normalized_query: str = "",
    ) -> None:
        day_dir = _today_dir(self.root)
        context_path = day_dir / "session_context" / f"{session_id}.json"
        context = {
            "session_id": session_id,
            "raw_query": raw_query,
            "normalized_query": normalized_query or rules.get("normalized_query", ""),
            "rules": rules,
            "results": results,
            "created_at": utc_now_iso(),
        }
        dump_json(context_path, context)
        for rank, result in enumerate(results, start=1):
            row = build_scenario_row(
                session_id=session_id,
                raw_query=raw_query,
                normalized_query=normalized_query,
                rules=rules,
                result=result,
                rank=rank,
            )
            _append_csv(day_dir / "pending_scenarios.csv", row)
        self._sync_to_minio(day_dir / "pending_scenarios.csv")

    def label_click(self, *, session_id: str, restaurant_id: str, rank_position: int | None = None) -> dict[str, Any] | None:
        day_dir = _today_dir(self.root)
        context = load_json(day_dir / "session_context" / f"{session_id}.json", {})
        if not context:
            return None
        results = context.get("results") or []
        clicked_result = next((row for row in results if str(row.get("restaurant_id")) == str(restaurant_id)), None)
        if clicked_result is None:
            clicked_result = {"restaurant_id": restaurant_id, "name": ""}
        rank = rank_position or next((idx for idx, row in enumerate(results, start=1) if str(row.get("restaurant_id")) == str(restaurant_id)), 1)
        row = build_scenario_row(
            session_id=session_id,
            raw_query=context.get("raw_query", ""),
            normalized_query=context.get("normalized_query", ""),
            rules=context.get("rules", {}),
            result=clicked_result,
            rank=rank,
            clicked=True,
            label_source="restaurant_detail_click",
        )
        _append_csv(day_dir / "labeled_scenarios.csv", row)
        self._sync_to_minio(day_dir / "labeled_scenarios.csv")
        return row
