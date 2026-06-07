from __future__ import annotations

import time
from uuid import uuid4

from fastapi import APIRouter

from apps.api.deps import (
    get_event_logger,
    get_geo_test_result_cache,
    get_graphrag_service,
    get_query_parser,
    get_rgcn_service,
    get_settings,
)
from apps.api.schemas import RecommendRequest
from services.ranking_service.fusion import fuse_results
from services.query_parser.parser import normalize_query

router = APIRouter()


def _sanitize(value):
    if isinstance(value, dict):
        return {str(key): _sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _public_rules(rules: dict) -> dict:
    return {key: value for key, value in rules.items() if key != "normalized_query"}


def _hard_feature_rules(req: RecommendRequest) -> dict:
    rules = {}
    if req.manual_rules:
        rules.update(req.manual_rules)
    if req.hard_features:
        rules.update(req.hard_features)
    if req.user_lat is not None and req.user_lng is not None:
        rules["query_lat"] = req.user_lat
        rules["query_lng"] = req.user_lng
    rules["distance_tolerance_m"] = req.distance_tolerance_m or rules.get("distance_tolerance_m") or 1500.0
    return rules


@router.post("/recommend")
def recommend(req: RecommendRequest):
    started = time.perf_counter()
    settings = get_settings()
    logger = get_event_logger()
    session_id = req.session_id or str(uuid4())

    normalized_query = normalize_query(req.query)
    if req.mode == "hard_feature":
        final_rules = _hard_feature_rules(req)
        rgcn_service = get_rgcn_service()
        rgcn_results = rgcn_service.recommend(req.query, final_rules, top_k=req.top_k)
        weights = {"graphrag": 0.0, "rgcn": 1.0, "rule": 0.0, "popularity": 0.0}
        fused = fuse_results([], rgcn_results, "rgcn", weights)[: req.top_k]
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        logger.log_event(
            {
                "session_id": session_id,
                "user_id": None,
                "event_type": "query_created",
                "raw_query": req.query,
                "normalized_query": normalized_query,
                "manual_rules_json": req.manual_rules or {},
                "inferred_rules_json": {},
                "final_rules_json": final_rules,
                "query_lat": final_rules.get("query_lat"),
                "query_lng": final_rules.get("query_lng"),
                "distance_tolerance_m": final_rules.get("distance_tolerance_m"),
                "rule_source": "hard_feature",
                "parse_confidence": 1.0,
                "algorithm_requested": req.algorithm,
                "algorithm_used": "rgcn_hard_feature",
                "graphrag_mode": "disabled",
                "rgcn_model_loaded": rgcn_service.ranker is not None,
                "ranking_weights_json": weights,
                "results_shown_json": [row["restaurant_id"] for row in fused],
                "clicked_restaurant_id": None,
                "feedback_value": None,
                "latency_ms": latency_ms,
            }
        )
        logger.log_event(
            {
                "session_id": session_id,
                "event_type": "result_shown",
                "results_shown_json": [row["restaurant_id"] for row in fused],
            }
        )
        logger.log_shown_scenarios(
            session_id=session_id,
            raw_query=req.query,
            normalized_query=normalized_query,
            rules=final_rules,
            results=fused,
        )
        return _sanitize({
            "session_id": session_id,
            "query": req.query,
            "inferred_rules": {},
            "final_rules": _public_rules(final_rules),
            "algorithm_used": "rgcn_hard_feature",
            "graphrag_mode": "disabled",
            "rgcn_model_loaded": rgcn_service.ranker is not None,
            "ranking_weights": weights,
            "results": fused,
            "latency_ms": latency_ms,
        })

    cache_hit = get_geo_test_result_cache().recommend(req.query, top_k=req.top_k)
    parser = get_query_parser()
    if cache_hit is not None:
        inferred_rules, cached_results = cache_hit
        final_rules = parser.merge_rules(inferred_rules, req.manual_rules)
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        logger.log_event(
            {
                "session_id": session_id,
                "user_id": None,
                "event_type": "query_created",
                "raw_query": req.query,
                "normalized_query": normalized_query,
                "manual_rules_json": req.manual_rules or {},
                "inferred_rules_json": inferred_rules,
                "final_rules_json": final_rules,
                "query_lat": req.user_lat,
                "query_lng": req.user_lng,
                "distance_tolerance_m": req.distance_tolerance_m,
                "rule_source": "geo_test_cache",
                "parse_confidence": inferred_rules.get("match_score", 0.0),
                "algorithm_requested": req.algorithm,
                "algorithm_used": "geo_test_cache",
                "graphrag_mode": "geo_test_cache",
                "rgcn_model_loaded": False,
                "ranking_weights_json": {"graphrag": 1.0, "rgcn": 0.0, "rule": 0.0, "popularity": 0.0},
                "results_shown_json": [row["restaurant_id"] for row in cached_results],
                "clicked_restaurant_id": None,
                "feedback_value": None,
                "latency_ms": latency_ms,
            }
        )
        logger.log_event(
            {
                "session_id": session_id,
                "event_type": "result_shown",
                "results_shown_json": [row["restaurant_id"] for row in cached_results],
            }
        )
        logger.log_shown_scenarios(
            session_id=session_id,
            raw_query=req.query,
            normalized_query=normalized_query,
            rules=final_rules,
            results=cached_results,
        )
        return _sanitize({
            "session_id": session_id,
            "query": req.query,
            "inferred_rules": _public_rules(inferred_rules),
            "final_rules": _public_rules(final_rules),
            "algorithm_used": "geo_test_cache",
            "graphrag_mode": "geo_test_cache",
            "rgcn_model_loaded": False,
            "ranking_weights": {"graphrag": 1.0, "rgcn": 0.0, "rule": 0.0, "popularity": 0.0},
            "results": cached_results,
            "latency_ms": latency_ms,
        })

    inferred_rules = parser.parse(req.query)
    final_rules = parser.merge_rules(inferred_rules, req.manual_rules)
    if req.user_lat is not None and req.user_lng is not None:
        final_rules["query_lat"] = req.user_lat
        final_rules["query_lng"] = req.user_lng
        final_rules["distance_tolerance_m"] = req.distance_tolerance_m or 1500.0

    graph_results = get_graphrag_service().recommend(req.query, final_rules, top_k=req.top_k)
    graphrag_mode = next((row.get("graphrag_mode") for row in graph_results if row.get("graphrag_mode")), "unknown")
    weights = dict(settings.config["ranking"]["weights"])
    weights["rgcn"] = 0.0
    rgcn_results = []
    fused = fuse_results(graph_results, rgcn_results, req.algorithm, weights)[: req.top_k]

    logger.log_event(
        {
            "session_id": session_id,
            "user_id": None,
            "event_type": "query_created",
            "raw_query": req.query,
            "normalized_query": normalized_query,
            "manual_rules_json": req.manual_rules or {},
            "inferred_rules_json": inferred_rules,
            "final_rules_json": final_rules,
            "query_lat": req.user_lat,
            "query_lng": req.user_lng,
            "distance_tolerance_m": req.distance_tolerance_m,
            "rule_source": "mixed" if req.manual_rules else "nl_parser",
            "parse_confidence": inferred_rules.get("confidence", 0.0),
            "algorithm_requested": req.algorithm,
            "algorithm_used": req.algorithm,
            "graphrag_mode": graphrag_mode,
            "rgcn_model_loaded": False,
            "ranking_weights_json": weights,
            "results_shown_json": [row["restaurant_id"] for row in fused],
            "clicked_restaurant_id": None,
            "feedback_value": None,
            "latency_ms": None,
        }
    )
    logger.log_event(
        {
            "session_id": session_id,
            "event_type": "result_shown",
            "results_shown_json": [row["restaurant_id"] for row in fused],
        }
    )
    logger.log_shown_scenarios(
        session_id=session_id,
        raw_query=req.query,
        normalized_query=normalized_query,
        rules=final_rules,
        results=fused,
    )

    latency_ms = round((time.perf_counter() - started) * 1000, 3)
    return _sanitize({
        "session_id": session_id,
        "query": req.query,
        "inferred_rules": _public_rules(inferred_rules),
        "final_rules": _public_rules(final_rules),
        "algorithm_used": req.algorithm,
        "graphrag_mode": graphrag_mode,
        "rgcn_model_loaded": False,
        "ranking_weights": weights,
        "results": fused,
        "latency_ms": latency_ms,
    })
