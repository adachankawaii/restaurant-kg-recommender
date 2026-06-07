from __future__ import annotations


def infer_rgcn_score(query: str, rules: dict, restaurant_id: str) -> float:
    score = 0.0
    if rules.get("food"):
        score += 0.1
    if restaurant_id:
        score += 0.1
    return score
