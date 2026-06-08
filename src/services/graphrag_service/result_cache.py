from __future__ import annotations

import ast
import csv
import json
import math
import os
import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any


STOPWORDS = {
    "a",
    "an",
    "co",
    "cua",
    "di",
    "duoc",
    "gan",
    "hay",
    "la",
    "minh",
    "muon",
    "o",
    "quan",
    "that",
    "tim",
    "toi",
    "va",
}


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFKC", str(value or "")).lower()
    value = value.replace("đ", "d")
    value = "".join(char for char in unicodedata.normalize("NFKD", value) if not unicodedata.combining(char))
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _tokens(value: str) -> set[str]:
    return {token for token in _normalize(value).split() if len(token) > 1 and token not in STOPWORDS}


def _char_ngrams(value: str, n: int = 3) -> set[str]:
    text = _normalize(value).replace(" ", "")
    if len(text) <= n:
        return {text} if text else set()
    return {text[i : i + n] for i in range(len(text) - n + 1)}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _cosine(left: Counter[str], right: Counter[str]) -> float:
    if not left or not right:
        return 0.0
    common = set(left) & set(right)
    dot = sum(left[key] * right[key] for key in common)
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


def _containment(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / min(len(left), len(right))


def _parse_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
        return parsed if isinstance(parsed, list) else [parsed]
    except (SyntaxError, ValueError):
        return [part.strip() for part in text.split("|") if part.strip()]


def _parse_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _intent_text(intent: dict[str, Any]) -> str:
    values: list[str] = []
    for key in ("query_type", "district", "dish_name", "price_band", "geo_intent", "sentiment_pref"):
        if intent.get(key):
            values.append(str(intent[key]))
    for key in ("cuisines", "categories", "entity_terms", "required_attributes"):
        values.extend(str(item) for item in _parse_list(intent.get(key)))
    return " ".join(values)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class GeoTestResultCache:
    def __init__(self, csv_path: Path, threshold: float = 0.18):
        self.csv_path = csv_path
        self.threshold = threshold
        self.groups: dict[str, list[dict[str, str]]] = {}
        self.query_features: dict[str, tuple[set[str], set[str], Counter[str]]] = {}
        self._load()

    @classmethod
    def from_production_root(cls, production_root: Path) -> "GeoTestResultCache":
        configured = os.getenv("GEO_TEST_RESULTS_PATH", "").strip()
        candidates = [
            Path(configured) if configured else None,
            production_root / "graphrag_50_geo_test_results_long.csv",
            production_root.parent.parent / "graphrag_50_geo_test_results_long.csv",
            production_root.parent.parent / "food_demo_app" / "graphrag_50_geo_test_results_long(2).csv",
        ]
        for candidate in candidates:
            if candidate and candidate.exists():
                return cls(candidate, threshold=float(os.getenv("GEO_TEST_MATCH_THRESHOLD", "0.18")))
        return cls(Path("__missing_geo_test_results__.csv"))

    def _load(self) -> None:
        if not self.csv_path.exists():
            return
        with self.csv_path.open("r", encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                query = str(row.get("query", "")).strip()
                if not query:
                    continue
                self.groups.setdefault(query, []).append(row)
        for query, rows in self.groups.items():
            rows.sort(key=lambda row: int(float(row.get("rank") or 999)))
            intent = _parse_json(rows[0].get("intent_json"))
            feature_text = " ".join(
                [
                    query,
                    _intent_text(intent),
                ]
            )
            tokens = _tokens(feature_text)
            self.query_features[query] = (tokens, _char_ngrams(feature_text), Counter(tokens))

    def _score(self, query: str, candidate: str) -> float:
        query_tokens = _tokens(query)
        candidate_tokens, candidate_chars, candidate_counts = self.query_features[candidate]
        token_score = _jaccard(query_tokens, candidate_tokens)
        containment = _containment(query_tokens, candidate_tokens)
        char_score = _jaccard(_char_ngrams(query), candidate_chars)
        cosine_score = _cosine(Counter(query_tokens), candidate_counts)
        return 0.30 * token_score + 0.30 * containment + 0.25 * cosine_score + 0.15 * char_score

    def match(self, query: str) -> tuple[str, float] | None:
        if not self.groups:
            return None
        query_tokens = _tokens(query)
        best_query = ""
        best_score = 0.0
        for candidate in self.groups:
            score = self._score(query, candidate)
            if score > best_score:
                best_query = candidate
                best_score = score
        candidate_tokens = self.query_features[best_query][0] if best_query else set()
        overlap = len(query_tokens & candidate_tokens)
        strong_match = overlap >= 2 or (overlap >= 1 and (len(query_tokens) <= 2 or best_score >= 0.32))
        if best_query and best_score >= self.threshold and strong_match:
            return best_query, round(best_score, 6)
        return None

    def recommend(self, query: str, top_k: int = 5) -> tuple[dict, list[dict]] | None:
        match = self.match(query)
        if match is None:
            return None
        matched_query, score = match
        rows = self.groups[matched_query][:top_k]
        results = [self._row_to_result(row) for row in rows]
        intent = {}
        if rows and rows[0].get("intent_json"):
            intent = _parse_json(rows[0]["intent_json"])
        intent.update(
            {
                "cache_hit": True,
                "cache_source": str(self.csv_path),
                "matched_query": matched_query,
                "match_score": score,
            }
        )
        return intent, results

    def _row_to_result(self, row: dict[str, str]) -> dict:
        final_score = _safe_float(row.get("final_score"))
        graph_score = _safe_float(row.get("final_score_before_ce"), final_score)
        distance_km = _safe_float(row.get("distance_km"), 0.0)
        evidence_values = _parse_list(row.get("evidence"))
        result = {
            "restaurant_id": str(row.get("store_key", "")),
            "name": row.get("name", ""),
            "address": row.get("address", ""),
            "district": row.get("district", ""),
            "city": row.get("city", ""),
            "price_band": row.get("price_band", ""),
            "matched_items": [],
            "categories": [str(item) for item in _parse_list(row.get("categories"))],
            "dish_families": [str(item) for item in _parse_list(row.get("dish_families"))],
            "review_count": 0.0,
            "rating": _safe_float(row.get("rating")),
            "latitude": None,
            "longitude": None,
            "distance_m": round(distance_km * 1000.0, 1) if distance_km else None,
            "distance_km": distance_km or None,
            "community_id": row.get("community_id", ""),
            "community_report": row.get("community_report", ""),
            "evidence_count": int(_safe_float(row.get("evidence_count"), 0.0)),
            "evidence": [
                {"source": "geo_test_cache", "field": "evidence", "value": str(item)}
                for item in evidence_values[:3]
            ],
            "extracted_entities": [],
            "graphrag_score": graph_score,
            "rule_score": _safe_float(row.get("ce_score")),
            "popularity_score": _safe_float(row.get("rating")) / 5.0,
            "graphrag_reason": "Lấy nhanh từ graphrag_50_geo_test_results_long.csv do query khớp với bộ test.",
            "graphrag_mode": "geo_test_cache",
            "scores": {
                "final": round(final_score, 6),
                "graphrag": round(graph_score, 6),
                "rgcn": 0.0,
                "rule": _safe_float(row.get("ce_score")),
                "popularity": _safe_float(row.get("rating")) / 5.0,
            },
        }
        result["graphrag_reason"] = "Fast path from graphrag_50_geo_test_results_long.csv because the query matched the cached test set."
        return result
