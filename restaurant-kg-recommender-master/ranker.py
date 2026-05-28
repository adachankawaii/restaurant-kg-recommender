from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Optional

from ingest import distance_score, haversine_km, normalize_dish_family, slugify_vn, to_float


def safe_float(x: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if x is None:
            return default
        x = float(x)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def rrf(rank: int, k: int = 60) -> float:
    return 1.0 / (k + rank)


def add_user_distance_to_record(
    rec: dict,
    user_lat: Optional[float],
    user_lng: Optional[float],
    decay_km: float = 3.0,
) -> dict:
    if user_lat is None or user_lng is None:
        rec.setdefault("distance_km", None)
        rec.setdefault("distance_score", 0.0)
        return rec
    dist = haversine_km(user_lat, user_lng, rec.get("lat"), rec.get("lng"))
    rec["distance_km"] = dist
    rec["distance_score"] = distance_score(dist, decay_km)
    return rec


def aggregate_text_unit_evidence(text_unit_hits: list[dict]) -> dict[str, dict]:
    bucket = defaultdict(lambda: {"evidence": [], "text_unit_vec_score_max": 0.0})
    for row in text_unit_hits:
        b = bucket[row["store_key"]]
        b["evidence"].append(row.get("feedback"))
        b["text_unit_vec_score_max"] = max(b["text_unit_vec_score_max"], float(row.get("vec_score") or 0.0))
    return bucket


def infer_geo_intent(query: str, intent: Optional[dict] = None) -> str:
    intent = intent or {}
    if intent.get("geo_intent") in {"nearest", "nearby", "normal"}:
        geo = intent["geo_intent"]
    else:
        geo = "normal"
    slug = slugify_vn(query)
    if any(t in slug for t in ["gan-nhat", "closest", "nearest"]):
        return "nearest"
    if any(t in slug for t in ["quanh-day", "xung-quanh", "gan-day", "gan", "nearby", "around"]):
        return "nearby" if geo != "nearest" else geo
    return geo


def _slug_contains_any(values: Any, needle: str) -> bool:
    if values is None or not needle:
        return False
    if isinstance(values, str):
        values = [values]
    needle_slug = slugify_vn(needle)
    for value in values or []:
        value_slug = slugify_vn(value)
        if needle_slug in value_slug or value_slug in needle_slug:
            return True
    return False


def _requires_direct_evidence(query: str, intent: dict) -> bool:
    slug = slugify_vn(query)
    terms = ["ngon", "sach", "review", "danh-gia", "phuc-vu", "dong-goi", "nhanh", "chat-luong", "evidence"]
    return bool(intent.get("required_attributes") or intent.get("sentiment_pref") or any(t in slug for t in terms))


def candidate_constraint_errors(candidate: dict, intent: dict, query: str, max_distance_km: Optional[float] = None) -> list[str]:
    errors: list[str] = []
    dish = intent.get("dish_name")
    if dish:
        family = normalize_dish_family(dish) or dish
        if not (_slug_contains_any(candidate.get("dish_families"), family) or _slug_contains_any(candidate.get("top_menu_items"), dish)):
            errors.append(f"dish_family_mismatch:{family}")
    price_band = intent.get("price_band")
    if price_band and candidate.get("price_band") != price_band:
        errors.append(f"price_band_mismatch:{candidate.get('price_band')}!={price_band}")
    max_distance = intent.get("max_distance_km") or max_distance_km
    if max_distance is not None:
        dist = candidate.get("distance_km")
        if dist is None or float(dist) > float(max_distance):
            errors.append(f"distance_mismatch:{dist}>{max_distance}")
    if intent.get("min_rating") is not None:
        rating = candidate.get("rating")
        if rating is None or float(rating) < float(intent["min_rating"]):
            errors.append(f"rating_mismatch:{rating}<{intent['min_rating']}")
    if _requires_direct_evidence(query, intent):
        has_evidence = bool(candidate.get("evidence")) or bool(candidate.get("community_report"))
        attrs = [a for a in (candidate.get("attributes") or []) if a and a.get("score") is not None and float(a.get("score") or 0) > 0]
        if not has_evidence and not attrs:
            errors.append("missing_evidence")
    return errors


def validate_post_fusion(candidates: list[dict], intent: dict, query: str, max_distance_km: Optional[float] = None) -> list[dict]:
    valid = []
    for candidate in candidates:
        errors = candidate_constraint_errors(candidate, intent, query, max_distance_km=max_distance_km)
        candidate["constraint_errors"] = errors
        candidate["constraint_valid"] = not errors
        if not errors:
            valid.append(candidate)
    return valid


def rerank_candidates(
    query: str,
    graph_hits: list[dict],
    neighbor_hits: list[dict],
    restaurant_vector_hits: list[dict],
    text_unit_hits: list[dict],
    intent: Optional[dict] = None,
    summary_by_key: Optional[dict[str, dict]] = None,
    rrf_k: int = 60,
    rating_weight: float = 0.10,
    distance_weight: float = 0.20,
    max_distance_km: Optional[float] = None,
) -> list[dict]:
    intent = intent or {}
    geo_intent = infer_geo_intent(query, intent)
    intent["geo_intent"] = geo_intent
    summary_by_key = summary_by_key or {}
    by_store: dict[str, dict] = {}

    def ensure(rec: dict) -> dict:
        sid = rec["store_key"]
        if sid not in by_store:
            meta = summary_by_key.get(sid, {})
            by_store[sid] = {
                "store_key": sid,
                "name": rec.get("name") or meta.get("name"),
                "address": rec.get("address") or meta.get("address"),
                "district": rec.get("district") or meta.get("district"),
                "city": rec.get("city") or meta.get("city"),
                "rating": rec.get("rating") if rec.get("rating") is not None else meta.get("rating"),
                "lat": rec.get("lat") if rec.get("lat") is not None else meta.get("lat"),
                "lng": rec.get("lng") if rec.get("lng") is not None else meta.get("lng"),
                "distance_km": rec.get("distance_km") if rec.get("distance_km") is not None else meta.get("distance_km"),
                "distance_score": rec.get("distance_score", meta.get("distance_score", 0.0)),
                "price_band": rec.get("price_band") or meta.get("price_band"),
                "top_menu_items": rec.get("top_menu_items") or meta.get("top_menu_items"),
                "dish_families": rec.get("dish_families") or meta.get("dish_families"),
                "categories": rec.get("categories") or meta.get("categories"),
                "cuisines": rec.get("cuisines") or meta.get("cuisines"),
                "attributes": rec.get("attributes") or meta.get("attributes") or [],
                "extracted_entities": rec.get("extracted_entities") or meta.get("extracted_entities") or [],
                "menu_price_min": rec.get("menu_price_min") or meta.get("menu_price_min"),
                "menu_price_max": rec.get("menu_price_max") or meta.get("menu_price_max"),
                "menu_price_median": rec.get("menu_price_median") or meta.get("menu_price_median"),
                "rrf_score": 0.0,
                "graph_rank_score": 0.0,
                "neighbor_score": 0.0,
                "restaurant_vec_score": 0.0,
                "text_unit_vec_score": 0.0,
                "community_report": rec.get("community_report"),
                "evidence": [],
                "source_flags": set(),
            }
        else:
            cur = by_store[sid]
            cur["community_report"] = cur.get("community_report") or rec.get("community_report")
            cur["distance_score"] = max(float(cur.get("distance_score") or 0.0), float(rec.get("distance_score") or 0.0))
            if cur.get("distance_km") is None and rec.get("distance_km") is not None:
                cur["distance_km"] = rec.get("distance_km")
        return by_store[sid]

    for rank, row in enumerate(graph_hits, start=1):
        c = ensure(row)
        c["graph_rank_score"] = max(c["graph_rank_score"], rrf(rank, rrf_k))
        c["rrf_score"] += rrf(rank, rrf_k)
        c["source_flags"].add("graph_filter")

    for rank, row in enumerate(neighbor_hits, start=1):
        c = ensure(row)
        c["neighbor_score"] = max(c["neighbor_score"], float(row.get("sim") or 0.0))
        c["rrf_score"] += rrf(rank, rrf_k)
        c["source_flags"].add("graph_neighbor")

    for rank, row in enumerate(restaurant_vector_hits, start=1):
        c = ensure(row)
        c["restaurant_vec_score"] = max(c["restaurant_vec_score"], float(row.get("vec_score") or 0.0))
        c["rrf_score"] += rrf(rank, rrf_k)
        c["source_flags"].add("restaurant_vector")

    for sid, info in aggregate_text_unit_evidence(text_unit_hits).items():
        c = ensure({"store_key": sid})
        c["text_unit_vec_score"] = max(c["text_unit_vec_score"], info["text_unit_vec_score_max"])
        c["evidence"] = [x for x in info["evidence"] if x][:3]
        c["rrf_score"] += rrf(1, rrf_k) * info["text_unit_vec_score_max"]
        c["source_flags"].add("text_unit_vector")

    max_rrf = max([c["rrf_score"] for c in by_store.values()] or [1.0])
    max_rrf = max(max_rrf, 1e-9)
    has_distance = any(c.get("distance_km") is not None for c in by_store.values())
    if not has_distance:
        dist_weight = 0.0
    elif geo_intent == "nearest":
        dist_weight = max(distance_weight, 0.45)
    elif geo_intent == "nearby":
        dist_weight = max(distance_weight, 0.35)
    else:
        dist_weight = min(distance_weight, 0.08)
    retrieval_weight = max(0.0, 1.0 - rating_weight - dist_weight)

    results = []
    for c in by_store.values():
        rrf_norm = c["rrf_score"] / max_rrf
        rating_norm = float(c.get("rating") or 0.0) / 5.0
        dist_norm = float(c.get("distance_score") or 0.0)
        c["final_score"] = retrieval_weight * rrf_norm + rating_weight * rating_norm + dist_weight * dist_norm
        c["geo_intent"] = geo_intent
        c["score_components"] = {
            "retrieval": round(retrieval_weight * rrf_norm, 4),
            "rating": round(rating_weight * rating_norm, 4),
            "distance": round(dist_weight * dist_norm, 4),
        }
        c["source_flags"] = sorted(c["source_flags"])
        for key in ["restaurant_vec_score", "text_unit_vec_score", "graph_rank_score", "neighbor_score", "rrf_score", "final_score", "rating", "distance_km", "distance_score"]:
            c[key] = safe_float(c.get(key), default=None if key in {"rating", "distance_km"} else 0.0)
        results.append(c)

    results = validate_post_fusion(results, intent, query, max_distance_km=max_distance_km)
    if geo_intent == "nearest":
        return sorted(results, key=lambda x: (x.get("distance_km") is None, safe_float(x.get("distance_km"), 1e9) or 1e9, -(safe_float(x.get("final_score")) or 0.0)))
    return sorted(results, key=lambda x: safe_float(x.get("final_score")) or 0.0, reverse=True)
