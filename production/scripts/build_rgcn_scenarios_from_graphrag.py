from __future__ import annotations

import argparse
import ast
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

PRODUCTION_ROOT = Path(__file__).resolve().parents[1]
ROOT = PRODUCTION_ROOT.parent
for path in (ROOT, PRODUCTION_ROOT):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from common import ensure_dir, utc_now_iso  # noqa: E402


ZERO = "0"

TERM_STOPWORDS = {
    "an",
    "mon",
    "quan",
    "tim",
    "goi",
    "y",
    "gan",
    "toi",
    "day",
    "quanh",
    "khu",
    "nay",
    "comment",
    "review",
    "that",
    "nhieu",
    "rating",
    "on",
    "tot",
    "cao",
    "gia",
    "re",
    "mem",
    "sinh",
    "vien",
    "duoi",
    "khong",
    "qua",
    "dat",
    "hop",
    "ly",
    "phu",
    "voi",
    "bua",
    "sang",
    "trua",
    "toi",
    "mot",
    "minh",
    "nhom",
    "nguoi",
}

BASE_FOOD_TERMS = {
    "com",
    "com ga",
    "com rang",
    "com tam",
    "com van phong",
    "pho",
    "pho bo",
    "pho ga",
    "pho xao",
    "bun",
    "bun cha",
    "bun bo",
    "banh cuon",
    "tra sua",
    "ga",
    "ga ran",
}

ASPECT_RULES = [
    ("cleanliness", (r"\bsach\b", r"\bve sinh\b", r"\bkhong bi che ve sinh\b")),
    ("packaging", (r"\bdong goi\b", r"\bgoi tot\b")),
    ("speed", (r"\bgiao nhanh\b", r"\ban nhanh\b", r"\bnhanh\b")),
    ("staff_service", (r"\bphuc vu\b", r"\bho tro\b", r"\bnhan vien\b")),
    ("value_for_money", (r"\bgia\b", r"\bre\b", r"\bmem\b", r"\bsinh vien\b", r"\bduoi \d+k\b", r"\bkhong qua dat\b", r"\bhop ly\b", r"\bdang tien\b")),
    ("taste", (r"\bngon\b", r"\bkhen ngon\b", r"\bcomment tot\b", r"\bcomment on\b", r"\bkhong te\b", r"\bday dan\b", r"\bphan an\b")),
    ("high_rating", (r"\brating\b", r"\bdanh gia\b")),
]

RAW_ASPECT = {
    "taste": "food_quality",
    "value_for_money": "price",
    "staff_service": "service",
    "cleanliness": "cleanliness",
    "packaging": "packaging",
    "speed": "speed",
    "high_rating": "rating",
}


def _strip_accents(value: str) -> str:
    value = value.replace("đ", "d").replace("Đ", "D")
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _norm(value: Any) -> str:
    text = _strip_accents(str(value or "").lower())
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _split_serialized_list(value: Any) -> list[str]:
    if value is None or str(value).strip() == "":
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
    except (SyntaxError, ValueError):
        pass
    return [part.strip() for part in text.split("|") if part.strip()]


def _tokens(text: str) -> set[str]:
    return {token for token in _norm(text).split() if token and token not in TERM_STOPWORDS}


def _phrase_in_text(phrase: str, text: str) -> bool:
    if not phrase:
        return False
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text))


def _feature_vocab(source: pd.DataFrame, existing_scenarios: Path) -> set[str]:
    vocab = set(BASE_FOOD_TERMS)
    for column in ("dish_families", "categories"):
        for value in source.get(column, pd.Series(dtype=str)).tolist():
            for item in _split_serialized_list(value):
                clean = _norm(item)
                if len(clean.split()) == 1 and clean not in BASE_FOOD_TERMS:
                    continue
                if clean and len(clean) >= 3:
                    vocab.add(clean)
    if existing_scenarios.exists():
        old = pd.read_csv(existing_scenarios).fillna("")
        for value in old.get("term", pd.Series(dtype=str)).tolist():
            for item in str(value).split("|"):
                clean = _norm(item)
                if clean and clean != ZERO:
                    vocab.add(clean)
    return vocab


def _query_terms(query: str, group: pd.DataFrame, vocab: set[str]) -> str:
    text = _norm(query)
    query_tokens = _tokens(query)
    matched: list[str] = []
    direct_matches: list[str] = []

    for phrase in sorted(vocab, key=lambda item: (-len(item.split()), -len(item), item)):
        phrase_tokens = {token for token in phrase.split() if token not in TERM_STOPWORDS}
        if not phrase_tokens:
            continue
        if _phrase_in_text(phrase, text):
            matched.append(phrase)
            direct_matches.append(phrase)

    direct_specific = [item for item in direct_matches if len(item.split()) >= 2]
    direct_generic = {item for item in direct_matches if item in {"com", "pho", "bun", "ga"}}

    # Positive-store features refine only explicit query terms. A specific query
    # like "com ga" may add "com ga roti"; it must not add every "com ..." item.
    for _, row in group.sort_values("rank").iterrows():
        for item in _split_serialized_list(row.get("dish_families")) + _split_serialized_list(row.get("categories")):
            clean = _norm(item)
            item_tokens = {token for token in clean.split() if token not in TERM_STOPWORDS}
            if clean and item_tokens and item_tokens.intersection(query_tokens):
                specific_match = any(term in clean or clean in term for term in direct_specific)
                generic_match = bool(direct_generic.intersection(item_tokens)) and not direct_specific
                if specific_match or generic_match:
                    matched.append(clean)

    out = []
    seen = set()
    for item in matched:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
        if len(out) >= 20:
            break
    return "|".join(out) if out else ZERO


def _aspects(query: str) -> tuple[str, str]:
    text = _norm(query)
    aspects: list[str] = []
    for aspect, patterns in ASPECT_RULES:
        if any(re.search(pattern, text) for pattern in patterns):
            aspects.append(aspect)
    unique = []
    for aspect in aspects:
        if aspect not in unique:
            unique.append(aspect)
    raw = [RAW_ASPECT.get(aspect, aspect) for aspect in unique]
    return ("|".join(unique) if unique else ZERO, "|".join(raw) if raw else ZERO)


def _price_range(query: str) -> str:
    text = _norm(query)
    if re.search(r"\bduoi\s*50k\b|\bsinh vien\b|\bgia re\b|\bgia mem\b", text):
        return "under_50k"
    if re.search(r"\b50\s*100k\b|\b50k\s*100k\b|\b100k\b|\b70k\b|\btam trung\b|\bgia vua phai\b", text):
        return "50k_100k"
    if re.search(r"\btren\s*100k\b|\bpremium\b|\bcao cap\b", text):
        return "over_100k"
    return ZERO


def _time_slot(query: str) -> str:
    text = _norm(query)
    if re.search(r"\bsang som\b|\ban sang\b|\bbua sang\b|\bsang\b", text):
        return "breakfast"
    if re.search(r"\ban trua\b|\bbua trua\b|\bcom trua\b|\btrua\b", text):
        return "lunch"
    if re.search(r"\ban toi\b|\bbua toi\b|\bgio toi\b|\bbuoi toi\b|\btoi nay\b|\b22h\b", text):
        return "dinner"
    return ZERO


def _area(query: str) -> str:
    text = _norm(query)
    areas = {
        "bach khoa": "bach khoa",
        "hust": "hust",
        "dong da": "Quan Dong Da",
        "hai ba trung": "Quan Hai Ba Trung",
        "bach mai": "bach mai",
        "giai phong": "giai phong",
    }
    for needle, area in areas.items():
        if _phrase_in_text(needle, text):
            return area
    return ZERO


def _weight(rank: Any) -> float:
    try:
        rank_int = int(float(rank))
    except (TypeError, ValueError):
        rank_int = 5
    return round(1.0 / max(rank_int, 1), 6)


def build(input_path: Path, existing_scenarios: Path, output_path: Path) -> Path:
    source = pd.read_csv(input_path).fillna("")
    vocab = _feature_vocab(source, existing_scenarios)
    rows = []
    for (test_id, query), group in source.groupby(["test_id", "query"], sort=False):
        preferred, raw_preferred = _aspects(str(query))
        price_range = _price_range(str(query))
        term = _query_terms(str(query), group, vocab)
        area = _area(str(query))
        time_slot = _time_slot(str(query))
        query_node_id = f"query:graphrag_{str(test_id).lower()}"
        for _, row in group.sort_values("rank").iterrows():
            store_id = str(row.get("store_key", "")).strip()
            if not store_id:
                continue
            rows.append(
                {
                    "query_node_id": query_node_id,
                    "user_id": f"graphrag_{str(test_id).lower()}",
                    "time_query": str(row.get("saved_at") or utc_now_iso()),
                    "query_lat": row.get("user_lat", ""),
                    "query_lng": row.get("user_lng", ""),
                    "area_id": area,
                    "query_location_hint": area,
                    "time_slot_id": time_slot,
                    "term": term,
                    "desired_price_range_id": price_range,
                    "preferred_aspects": preferred,
                    "raw_preferred_aspects": raw_preferred,
                    "distance_tolerance_m": 0,
                    "store_node_id": f"store:{store_id}",
                    "store_id": store_id,
                    "store_name": row.get("name", ""),
                    "rank": row.get("rank", ""),
                    "relevance_score": row.get("final_score", ""),
                    "relevance_weight": _weight(row.get("rank")),
                    "distance_m": float(row.get("distance_km") or 0) * 1000 if str(row.get("distance_km", "")).strip() else "",
                    "rating": row.get("rating", ""),
                    "review_count": "",
                    "median_price": "",
                    "latitude": "",
                    "longitude": "",
                    "is_closed": "false",
                    "reason": "deterministic_query_features_from_graphrag_long_csv",
                }
            )
    ensure_dir(output_path.parent)
    pd.DataFrame(rows).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("production/graphrag_50_geo_test_results_long.csv"))
    parser.add_argument("--existing-scenarios", type=Path, default=Path("rgcn_pipeline/data/user_scenarios_phase2_top5.csv"))
    parser.add_argument("--output", type=Path, default=Path("rgcn_pipeline/data/graphrag_50_geo_rgcn_scenarios.csv"))
    args = parser.parse_args()
    print(build(args.input, args.existing_scenarios, args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
