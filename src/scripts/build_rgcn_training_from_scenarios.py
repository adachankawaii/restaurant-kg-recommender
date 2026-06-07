from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


OUTPUT_FIELDS = [
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
]


ASPECT_ALIASES = {
    "taste": "food_quality",
    "value_for_money": "price",
    "staff_service": "service",
    "fast_delivery": "speed",
    "cleanliness": "cleanliness",
    "nearby": "location",
    "high_rating": "food_quality",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def normalized_aspects(value: str) -> str:
    out = []
    for item in str(value or "").split("|"):
        item = item.strip()
        if not item:
            continue
        mapped = ASPECT_ALIASES.get(item, item)
        if mapped not in out:
            out.append(mapped)
    return "|".join(out)


def safe_json_list(value: str) -> list[dict[str, Any]]:
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except json.JSONDecodeError:
        return []


def relevance_for_rank(rank: int, score: float | None = None) -> tuple[float, float]:
    base = max(0.2, 1.0 - 0.12 * max(rank - 1, 0))
    if score is not None:
        base = 0.65 * base + 0.35 * max(0.0, min(float(score), 1.0))
    return round(base, 6), round(base, 6)


def area_from_hint(hint: str) -> str:
    text = str(hint or "").lower()
    if any(token in text for token in ["bách khoa", "bach khoa", "lê thanh nghị", "le thanh nghi", "ktx"]):
        return "Ha Noi:Quan Hai Ba Trung"
    if any(token in text for token in ["đống đa", "dong da", "xã đàn", "xa dan"]):
        return "Ha Noi:Quan Dong Da"
    return ""


def rows_from_nested(path: Path) -> list[dict[str, str]]:
    rows = []
    for index, row in enumerate(read_csv(path), start=1):
        user_id = row.get("user_id") or f"u{index:03d}"
        query_node_id = f"query:{user_id}"
        top_items = safe_json_list(row.get("top_k", ""))
        for fallback_rank, item in enumerate(top_items, start=1):
            store_id = str(item.get("restaurant_id") or "").strip()
            if not store_id:
                continue
            rank = int(item.get("rank") or fallback_rank)
            score = item.get("score")
            try:
                score_value = float(score) if score is not None else None
            except (TypeError, ValueError):
                score_value = None
            rel_score, rel_weight = relevance_for_rank(rank, score_value)
            rows.append(
                {
                    "query_node_id": query_node_id,
                    "user_id": user_id,
                    "time_query": row.get("time_query", ""),
                    "query_lat": row.get("query_lat", ""),
                    "query_lng": row.get("query_lng", ""),
                    "area_id": area_from_hint(row.get("query_location_hint", "")),
                    "query_location_hint": row.get("query_location_hint", ""),
                    "time_slot_id": row.get("time_slot_id", ""),
                    "term": row.get("term", ""),
                    "desired_price_range_id": row.get("desired_price_range_id", ""),
                    "preferred_aspects": normalized_aspects(row.get("preferred_aspects", "")),
                    "raw_preferred_aspects": row.get("preferred_aspects", ""),
                    "distance_tolerance_m": row.get("distance_tolerance_m", ""),
                    "store_node_id": f"store:{store_id}",
                    "store_id": store_id,
                    "store_name": str(item.get("restaurant_name") or ""),
                    "rank": str(rank),
                    "relevance_score": str(rel_score),
                    "relevance_weight": str(rel_weight),
                    "distance_m": str(item.get("distance_m") or ""),
                    "rating": str(item.get("rating") or ""),
                    "review_count": str(item.get("review_count") or ""),
                    "median_price": str(item.get("price_min") or ""),
                    "latitude": "",
                    "longitude": "",
                    "is_closed": str(str(item.get("open_status", "")).lower() == "closed").lower(),
                    "reason": str(item.get("reason") or ""),
                }
            )
    return rows


def rows_from_phase2(path: Path) -> list[dict[str, str]]:
    rows = []
    for row in read_csv(path):
        out = {field: row.get(field, "") for field in OUTPUT_FIELDS}
        out["preferred_aspects"] = normalized_aspects(out.get("preferred_aspects") or out.get("raw_preferred_aspects", ""))
        if out.get("store_id") and not out.get("store_node_id"):
            out["store_node_id"] = f"store:{out['store_id']}"
        if not out.get("relevance_weight"):
            try:
                rank = int(float(out.get("rank") or "1"))
            except ValueError:
                rank = 1
            _, out["relevance_weight"] = relevance_for_rank(rank)
        rows.append(out)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nested", type=Path, required=True)
    parser.add_argument("--phase2", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    merged = rows_from_phase2(args.phase2) + rows_from_nested(args.nested)
    seen = set()
    deduped = []
    for row in merged:
        key = (row.get("query_node_id"), row.get("store_node_id"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(deduped)
    print({"output": str(args.output), "rows": len(deduped), "queries": len({row["query_node_id"] for row in deduped})})


if __name__ == "__main__":
    main()
