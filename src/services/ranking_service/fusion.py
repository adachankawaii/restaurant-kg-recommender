from __future__ import annotations


def _normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    high = max(values)
    low = min(values)
    if high == low:
        return [1.0 if high > 0 else 0.0 for _ in values]
    return [(value - low) / (high - low) for value in values]


def _is_missing(value) -> bool:
    return value is None or value == "" or value == "N/A"


def _graph_only_rows(graphrag_results: list[dict], weights: dict | None = None) -> list[dict]:
    graph_scores = _normalize([row.get("graphrag_score", 0.0) for row in graphrag_results])
    rule_scores = _normalize([row.get("rule_score", 0.0) for row in graphrag_results])
    popularity_scores = _normalize([row.get("popularity_score", 0.0) for row in graphrag_results])
    graph_weight = float((weights or {}).get("graphrag", 1.0))
    rule_weight = float((weights or {}).get("rule", 0.0))
    popularity_weight = float((weights or {}).get("popularity", 0.0))
    rows = []
    for index, row in enumerate(graphrag_results):
        final_score = (
            graph_weight * graph_scores[index]
            + rule_weight * rule_scores[index]
            + popularity_weight * popularity_scores[index]
        )
        rows.append(
            {
                **row,
                "scores": {
                    "final": round(final_score, 6),
                    "graphrag": round(graph_scores[index], 6),
                    "rgcn": 0.0,
                    "rule": round(rule_scores[index], 6),
                    "popularity": round(popularity_scores[index], 6),
                },
            }
        )
    rows.sort(key=lambda item: item["scores"]["final"], reverse=True)
    return rows


def fuse_results(graphrag_results: list[dict], rgcn_results: list[dict], algorithm: str, weights: dict) -> list[dict]:
    if algorithm == "rgcn":
        if not rgcn_results:
            return _graph_only_rows(graphrag_results, {**weights, "rgcn": 0.0})
        else:
            return [
                {
                    **row,
                    "scores": {
                        "final": row.get("rgcn_score", 0.0),
                        "graphrag": 0.0,
                        "rgcn": row.get("rgcn_score", 0.0),
                        "rule": 0.0,
                        "popularity": 0.0,
                    },
                }
                for row in rgcn_results
            ]

    if algorithm == "graphrag":
        return _graph_only_rows(graphrag_results, {"graphrag": 1.0, "rule": 0.0, "popularity": 0.0})

    effective_weights = dict(weights)
    if not rgcn_results:
        effective_weights["rgcn"] = 0.0

    union: dict[str, dict] = {row["restaurant_id"]: dict(row) for row in graphrag_results}
    for row in rgcn_results:
        union.setdefault(row["restaurant_id"], {"restaurant_id": row["restaurant_id"], "name": row.get("restaurant_id"), "matched_items": [], "evidence": []})
        target = union[row["restaurant_id"]]
        for key, value in row.items():
            if key == "restaurant_id":
                continue
            if key in {"distance_m", "distance_km", "latitude", "longitude"} and not _is_missing(target.get(key)) and _is_missing(value):
                continue
            target[key] = value

    merged_rows = list(union.values())
    graph_scores = _normalize([row.get("graphrag_score", 0.0) for row in merged_rows])
    rule_scores = _normalize([row.get("rule_score", 0.0) for row in merged_rows])
    popularity_scores = _normalize([row.get("popularity_score", 0.0) for row in merged_rows])
    rgcn_scores = _normalize([row.get("rgcn_score", 0.0) for row in merged_rows])

    fused = []
    for index, row in enumerate(merged_rows):
        final_score = (
            effective_weights["graphrag"] * graph_scores[index]
            + effective_weights["rgcn"] * rgcn_scores[index]
            + effective_weights["rule"] * rule_scores[index]
            + effective_weights["popularity"] * popularity_scores[index]
        )
        fused.append(
            {
                **row,
                "scores": {
                    "final": round(final_score, 6),
                    "graphrag": round(graph_scores[index], 6),
                    "rgcn": round(rgcn_scores[index], 6),
                    "rule": round(rule_scores[index], 6),
                    "popularity": round(popularity_scores[index], 6),
                },
            }
        )
    fused.sort(key=lambda item: item["scores"]["final"], reverse=True)
    return fused
