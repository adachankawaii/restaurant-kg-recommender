from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_graphrag_cache_root(repo_root: Path, preferred: Path | None = None) -> Path:
    """Find the notebook GraphRAG cache, preferring a populated cache directory."""
    candidates = [
        preferred,
        repo_root / ".cache" / "graphrag",
        repo_root / "cache" / ".cache" / "graphrag",
    ]
    for candidate in candidates:
        if candidate and candidate.exists() and any(candidate.iterdir()):
            return candidate
    return (preferred or repo_root / ".cache" / "graphrag")


def has_graph_extraction_checkpoint(cache_root: Path) -> bool:
    checkpoint = cache_root / "llm_graph_extraction_checkpoint.json"
    return checkpoint.exists() and checkpoint.stat().st_size > 2


def _review_cache_key(review_text: str) -> str:
    payload = {
        "backend": "llm",
        "checkpoint_version": "v1",
        "review_text": (review_text or "").strip().lower(),
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def chunk_review_text(text: str, chunk_size: int = 400, overlap: int = 80) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        if end >= len(text):
            chunks.append(text[start:])
            break
        boundary = max(
            text.rfind(".", start, end),
            text.rfind("!", start, end),
            text.rfind("?", start, end),
            text.rfind("\n", start, end),
        )
        if boundary > start + overlap:
            end = boundary + 1
        chunks.append(text[start:end])
        start = end - overlap
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def enrich_feedback_with_cached_aspects(feedback_df: pd.DataFrame, cache_root: Path) -> pd.DataFrame:
    checkpoint = _load_json(cache_root / "llm_aspect_sentiment_checkpoint.json")
    df = feedback_df.copy()
    aspect_scores = []
    sentiments = []
    confidences = []
    for _, row in df.iterrows():
        key = _review_cache_key(str(row.get("feedback", "")))
        record = checkpoint.get(key, {})
        scores = record.get("aspect_scores", {}) if isinstance(record, dict) else {}
        aspect_scores.append(scores)
        if scores:
            avg = sum(float(v) for v in scores.values()) / max(len(scores), 1)
            sentiments.append("positive" if avg > 0.2 else "negative" if avg < -0.2 else "neutral")
            confidences.append(round(max(abs(float(v)) for v in scores.values()), 4))
        else:
            sentiments.append("neutral")
            confidences.append(0.0)
    df["aspect_scores"] = aspect_scores
    df["sentiment"] = sentiments
    df["confidence"] = confidences
    return df


def enrich_feedback_cache_first(feedback_df: pd.DataFrame, cache_root: Path, config) -> pd.DataFrame:
    checkpoint_path = cache_root / "llm_aspect_sentiment_checkpoint.json"
    if checkpoint_path.exists() and checkpoint_path.stat().st_size > 2:
        return enrich_feedback_with_cached_aspects(feedback_df, cache_root)

    from aspect_sentiment import create_aspect_sentiment_service, score_feedback_dataframe

    service = create_aspect_sentiment_service(config)
    return score_feedback_dataframe(feedback_df, service)


def apply_cached_dish_families(menu_items_df: pd.DataFrame, cache_root: Path) -> pd.DataFrame:
    cache = _load_json(cache_root / "llm_dish_family_cache.json")
    df = menu_items_df.copy()

    def build_key(category_name: str, item_name: str, item_details: str = "") -> str:
        payload = {
            "category_name": str(category_name or "").strip().lower(),
            "item_name": str(item_name or "").strip().lower(),
            "item_details": str(item_details or "").strip().lower(),
        }
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    dish_families = []
    for _, row in df.iterrows():
        key = build_key(row.get("category_name", ""), row.get("item_name", ""), row.get("item_details", ""))
        family = None
        if key in cache and isinstance(cache[key], dict):
            family = cache[key].get("dish_family")
        dish_families.append(family)
    df["dish_family"] = dish_families
    return df


def build_text_units_with_chunking(feedback_df: pd.DataFrame, name_map: dict[str, str], chunk_size: int = 400, overlap: int = 80) -> pd.DataFrame:
    rows = []
    for _, row in feedback_df.iterrows():
        review_text = str(row["feedback"])
        aspect_str = ", ".join(f"{key}={float(value):+.2f}" for key, value in (row.get("aspect_scores") or {}).items())
        header = (
            f"Tên quán: {name_map.get(str(row['store_key']), row['store_name'])}\n"
            f"Nguồn: {row['source']}\n"
            f"Rating người dùng: {row['rating']}\n"
            f"Sentiment tổng hợp: {row['sentiment']}\n"
            f"Aspect sentiment: {aspect_str}\n"
            f"Nội dung review: "
        )
        chunks = chunk_review_text(review_text, chunk_size=chunk_size, overlap=overlap) or ["(empty review)"]
        for index, chunk in enumerate(chunks):
            chunk_id = f"{row['review_id']}_{index}" if len(chunks) > 1 else row["review_id"]
            rows.append(
                {
                    "text_unit_id": "tu_" + chunk_id,
                    "review_id": row["review_id"],
                    "store_key": row["store_key"],
                    "store_name": row["store_name"],
                    "rating": row["rating"],
                    "rated_at": row.get("rated_at"),
                    "sentiment": row["sentiment"],
                    "aspect_scores": row.get("aspect_scores", {}),
                    "source": row["source"],
                    "feedback": review_text,
                    "chunk_text": header + chunk,
                    "chunk_index": index,
                    "n_chunks": len(chunks),
                }
            )
    return pd.DataFrame(rows)


def extract_entities_from_checkpoint(text_units_df: pd.DataFrame, cache_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    checkpoint = _load_json(cache_root / "llm_graph_extraction_checkpoint.json")
    entity_rows = []
    relation_rows = []
    seen_ids = set(text_units_df["text_unit_id"].astype(str).tolist())
    for record in checkpoint.values():
        if not isinstance(record, dict):
            continue
        if str(record.get("text_unit_id", "")) not in seen_ids:
            continue
        entity_rows.extend(record.get("entities", []))
        relation_rows.extend(record.get("relations", []))
    entity_df = pd.DataFrame(entity_rows).drop_duplicates() if entity_rows else pd.DataFrame()
    relation_df = pd.DataFrame(relation_rows).drop_duplicates() if relation_rows else pd.DataFrame()
    return entity_df, relation_df


def extract_entities_cache_first(text_units_df: pd.DataFrame, cache_root: Path, config) -> tuple[pd.DataFrame, pd.DataFrame]:
    if has_graph_extraction_checkpoint(cache_root):
        entity_df, relation_df = extract_entities_from_checkpoint(text_units_df, cache_root)
        if not entity_df.empty or not relation_df.empty:
            return entity_df, relation_df

    from llm_graph_extraction import LLMGraphExtractor

    extractor = LLMGraphExtractor(config)
    return extractor.extract_text_units(text_units_df)


def community_reports_to_df(cache_root: Path) -> pd.DataFrame:
    cache = _load_json(cache_root / "community_reports.json")
    rows = []
    for key, value in cache.items():
        if not isinstance(value, dict):
            continue
        report = value.get("report", {})
        rows.append(
            {
                "cache_key": key,
                "title": report.get("title", ""),
                "summary": report.get("summary", ""),
                "key_strengths": report.get("key_strengths", []),
                "common_issues": report.get("common_issues", []),
                "recommended_for": report.get("recommended_for", []),
            }
        )
    return pd.DataFrame(rows)
