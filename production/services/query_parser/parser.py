from __future__ import annotations

import re
import unicodedata

from .rules import FOOD_KEYWORDS, LOCATION_KEYWORDS


def _vietnamese_signal(value: str) -> int:
    return sum(
        1
        for char in value
        if "\u00c0" <= char <= "\u1ef9"
        or char in {"\u0103", "\u0111", "\u0129", "\u0169", "\u01a1", "\u01b0"}
    )


def _mojibake_signal(value: str) -> int:
    markers = ("Ã", "Ä", "Æ", "Â", "áº", "á»", "â€")
    return sum(value.count(marker) for marker in markers)


def _repair_mojibake(value: str) -> str:
    best = value
    best_score = _vietnamese_signal(value)
    best_mojibake = _mojibake_signal(value)
    for encoding in ("cp1252", "latin1"):
        try:
            candidate = value.encode(encoding).decode("utf-8")
        except UnicodeError:
            continue
        score = _vietnamese_signal(candidate)
        mojibake = _mojibake_signal(candidate)
        if mojibake < best_mojibake or (mojibake == best_mojibake and score > best_score):
            best = candidate
            best_score = score
            best_mojibake = mojibake
    return best


def normalize_query(value: str) -> str:
    value = _repair_mojibake(str(value or ""))
    value = unicodedata.normalize("NFKC", value).lower()
    value = value.replace("\u0111", "d").replace("\u0110", "d")
    value = "".join(
        char for char in unicodedata.normalize("NFKD", value) if not unicodedata.combining(char)
    )
    value = re.sub(r"[^a-z0-9<>=|\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


class QueryParser:
    def parse(self, raw_query: str) -> dict:
        query = normalize_query(raw_query)
        food = next((item for item in FOOD_KEYWORDS if item in query), None)
        location = next((item for item in LOCATION_KEYWORDS if item in query), None)

        price_match = re.search(r"(duoi|<=|khoang)\s*(\d+)\s*k", query)
        max_price = int(price_match.group(2)) * 1000 if price_match else None

        priority = []
        if "rating cao" in query or "danh gia cao" in query:
            priority.append("rating")
        if "gan" in query:
            priority.append("distance")
        if "re" in query:
            priority.append("price")

        confidence = 0.35
        if food:
            confidence += 0.2
        if location:
            confidence += 0.2
        if max_price:
            confidence += 0.1

        return {
            "food": food,
            "location": location,
            "max_price": max_price,
            "priority": priority,
            "cuisine": None,
            "time_constraint": None,
            "confidence": round(min(confidence, 0.95), 2),
            "normalized_query": query,
        }

    def merge_rules(self, inferred_rules: dict, manual_rules: dict | None) -> dict:
        final_rules = dict(inferred_rules)
        for key, value in (manual_rules or {}).items():
            if value is not None:
                final_rules[key] = value
        return final_rules
