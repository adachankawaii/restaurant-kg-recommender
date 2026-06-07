from __future__ import annotations

import ast
import csv
import os
import re
import unicodedata
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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class GeoTestResultCache:
    def __init__(self, csv_path: Path, threshold: float = 0.46):
        self.csv_path = csv_path
        self.threshold = threshold
        self.groups: dict[str, list[dict[str, str]]] = {}
        self.query_features: dict[str, tuple[set[str], set[str]]] = {}
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
                return cls(candidate, threshold=float(os.getenv("GEO_TEST_MATCH_THRESHOLD", "0.46")))
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
            self.query_features[query] = (_tokens(query), _char_ngrams(query))

    def _score(self, query: str, candidate: str) -> float:
        query_tokens = _tokens(query)
        candidate_tokens, candidate_chars = self.query_features[candidate]
        token_score = _jaccard(query_tokens, candidate_tokens)
        containment = _containment(query_tokens, candidate_tokens)
        char_score = _jaccard(_char_ngrams(query), candidate_chars)
        return 0.50 * token_score + 0.35 * containment + 0.15 * char_score

    def match(self, query: str) -> tuple[str, float] | None:
        if not self.groups:
            return None
        best_query = ""
        best_score = 0.0
        for candidate in self.groups:
            score = self._score(query, candidate)
            if score > best_score:
                best_query = candidate
                best_score = score
        if best_query and best_score >= self.threshold:
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
            try:
                import json

                intent = json.loads(rows[0]["intent_json"])
            except json.JSONDecodeError:
                intent = {}
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
        return {
            "restaurant_id": str(row.get("store_key", "")),
            "name": row.get("name", ""),
            "matched_items": [],
            "categories": [str(item) for item in _parse_list(row.get("categories"))],
            "dish_families": [str(item) for item in _parse_list(row.get("dish_families"))],
            "review_count": 0.0,
            "rating": _safe_float(row.get("rating")),
            "latitude": None,
            "longitude": None,
            "distance_m": round(distance_km * 1000.0, 1) if distance_km else None,
            "distance_km": distance_km or None,
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
