from __future__ import annotations

import time
from uuid import uuid4

from fastapi import APIRouter

from apps.api.deps import get_event_logger, get_graphrag_service, get_query_parser, get_rgcn_service, get_settings
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


@router.post("/recommend")
def recommend(req: RecommendRequest):
    started = time.perf_counter()
    settings = get_settings()
    parser = get_query_parser()
    logger = get_event_logger()
    session_id = req.session_id or str(uuid4())

    normalized_query = normalize_query(req.query)
    inferred_rules = parser.parse(normalized_query)
    final_rules = parser.merge_rules(inferred_rules, req.manual_rules)
    if req.user_lat is not None and req.user_lng is not None:
        final_rules["query_lat"] = req.user_lat
        final_rules["query_lng"] = req.user_lng
        final_rules["distance_tolerance_m"] = req.distance_tolerance_m or 1500.0

    graph_results = get_graphrag_service().recommend(req.query, final_rules, top_k=req.top_k)
    rgcn_results = get_rgcn_service().recommend(normalized_query, final_rules, top_k=req.top_k)
    weights = settings.config["ranking"]["weights"]
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
            "algorithm_used": req.algorithm if req.algorithm != "rgcn" or rgcn_results else "graphrag",
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
        "algorithm_used": req.algorithm if req.algorithm != "rgcn" or rgcn_results else "graphrag",
        "results": fused,
        "latency_ms": latency_ms,
    })
