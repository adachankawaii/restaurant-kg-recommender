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


def fuse_results(graphrag_results: list[dict], rgcn_results: list[dict], algorithm: str, weights: dict) -> list[dict]:
    if algorithm == "rgcn":
        if not rgcn_results:
            algorithm = "graphrag"
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

    if algorithm == "graphrag" or not rgcn_results:
        for row in graphrag_results:
            row["scores"] = {
                "final": row.get("graphrag_score", 0.0),
                "graphrag": row.get("graphrag_score", 0.0),
                "rgcn": 0.0,
                "rule": row.get("rule_score", 0.0),
                "popularity": row.get("popularity_score", 0.0),
            }
        return graphrag_results

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
            weights["graphrag"] * graph_scores[index]
            + weights["rgcn"] * rgcn_scores[index]
            + weights["rule"] * rule_scores[index]
            + weights["popularity"] * popularity_scores[index]
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
