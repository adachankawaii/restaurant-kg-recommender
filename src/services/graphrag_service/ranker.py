from __future__ import annotations


def apply_rule_score(item: dict, rules: dict) -> float:
    score = 0.0
    if rules.get("food") and rules["food"] in str(item.get("text", "")).lower():
        score += 1.0
    return score
