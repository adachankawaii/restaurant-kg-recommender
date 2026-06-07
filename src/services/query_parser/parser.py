from __future__ import annotations

import json
import os
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
    def __init__(self):
        self._client = None
        self._model = None
        self._llm_enabled = os.getenv("USE_LLM_QUERY_PARSER", "").strip().lower() in {"1", "true", "yes", "y", "on"}
        self._llm_available = self._llm_enabled and bool(_openai_api_key())

    def _parse_with_llm(self, raw_query: str) -> dict | None:
        if not self._llm_available:
            return None
        try:
            from openai import OpenAI

            if self._client is None:
                kwargs = {"api_key": _openai_api_key()}
                if os.getenv("OPENAI_BASE_URL"):
                    kwargs["base_url"] = os.getenv("OPENAI_BASE_URL")
                self._client = OpenAI(**kwargs)
                self._model = _openai_model_name()
            prompt = (
                "Bạn là bộ phân tích intent cho hệ gợi ý quán ăn GraphRAG. "
                "Trích xuất truy vấn thành JSON theo schema RestaurantIntent của notebook graph_rag_new. "
                "Các field: query_type, district, cuisines, categories, dish_name, min_rating, "
                "max_distance_km, price_band, geo_intent, entity_terms, required_attributes, "
                "sentiment_pref, top_k. "
                "required_attributes hợp lệ: food_quality, service, cleanliness, packaging, price, space, speed. "
                "Nếu query nhắc tiện ích/ngữ cảnh/mốc địa lý/chất lượng ngoài schema cứng, đưa vào entity_terms. "
                "Nếu nói gần nhất thì geo_intent=nearest; nếu gần đây/quanh tôi thì geo_intent=nearby."
            )
            response = self._client.chat.completions.create(
                model=self._model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": raw_query},
                ],
            )
            parsed = json.loads(response.choices[0].message.content or "{}")
            if not isinstance(parsed, dict):
                return None
            parsed.setdefault("query_type", "search")
            parsed.setdefault("district", None)
            parsed.setdefault("cuisines", [])
            parsed.setdefault("categories", [])
            parsed.setdefault("dish_name", None)
            parsed.setdefault("min_rating", None)
            parsed.setdefault("max_distance_km", None)
            parsed.setdefault("price_band", None)
            parsed.setdefault("geo_intent", "normal")
            parsed.setdefault("entity_terms", [])
            parsed.setdefault("required_attributes", [])
            parsed.setdefault("sentiment_pref", None)
            parsed.setdefault("top_k", 5)
            parsed["normalized_query"] = normalize_query(raw_query)
            parsed["confidence"] = float(parsed.get("confidence", 0.85) or 0.85)
            return parsed
        except Exception:
            self._llm_available = False
            return None

    def _parse_rule_based(self, raw_query: str) -> dict:
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

    def parse(self, raw_query: str) -> dict:
        if self._llm_enabled:
            llm_intent = self._parse_with_llm(raw_query)
            if llm_intent is not None:
                return llm_intent
        return self._parse_rule_based(raw_query)

    def merge_rules(self, inferred_rules: dict, manual_rules: dict | None) -> dict:
        final_rules = dict(inferred_rules)
        for key, value in (manual_rules or {}).items():
            if value is not None:
                final_rules[key] = value
        return final_rules


def _openai_api_key() -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        return api_key
    if os.getenv("LLM_PROVIDER", "").strip().lower() == "openai":
        return os.getenv("LLM_API_KEY", "").strip()
    return ""


def _openai_model_name() -> str:
    model = os.getenv("OPENAI_MODEL", "").strip()
    if model:
        return model
    llm_model = os.getenv("LLM_MODEL", "").strip()
    if llm_model and llm_model.lower() not in {"local-or-api-model-name", "local", "none"}:
        return llm_model
    return "gpt-4.1-mini"
