from __future__ import annotations

from typing import Any, Literal, Optional

import pandas as pd
from pydantic import BaseModel, Field

from cache import JsonDiskCache, stable_hash
from config import AppConfig
from ingest import normalize_text, slugify_vn


ENTITY_TYPES = {
    "dish",
    "food_descriptor",
    "amenity",
    "location_landmark",
    "occasion",
    "audience",
    "service_quality",
    "space_quality",
    "price_value",
    "delivery_packaging",
    "problem",
}

RELATION_TYPES = {
    "MENTIONS",
    "HAS_POSITIVE_SIGNAL",
    "HAS_NEGATIVE_SIGNAL",
    "GOOD_FOR",
    "NEAR",
    "HAS_AMENITY",
    "HAS_PROBLEM",
    "DESCRIBES_FOOD",
}


class ExtractedEntity(BaseModel):
    name: str
    type: str
    sentiment: Literal["positive", "neutral", "negative"] = "neutral"
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence: str = ""


class ExtractedRelation(BaseModel):
    source_entity: str
    target_entity: str
    relation_type: str
    sentiment: Literal["positive", "neutral", "negative"] = "neutral"
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence: str = ""


class ReviewGraphExtraction(BaseModel):
    entities: list[ExtractedEntity] = Field(default_factory=list)
    relations: list[ExtractedRelation] = Field(default_factory=list)


def canonical_entity_type(value: Any) -> str:
    slug = slugify_vn(value).replace("-", "_")
    aliases = {
        "mon_an": "dish",
        "dish_family": "dish",
        "tien_ich": "amenity",
        "dia_diem": "location_landmark",
        "landmark": "location_landmark",
        "ngu_canh": "occasion",
        "doi_tuong": "audience",
        "van_de": "problem",
        "do_an": "food_descriptor",
        "chat_luong_mon": "food_descriptor",
        "phuc_vu": "service_quality",
        "khong_gian": "space_quality",
        "gia": "price_value",
        "dong_goi": "delivery_packaging",
    }
    out = aliases.get(slug, slug)
    return out if out in ENTITY_TYPES else "problem"


def canonical_relation_type(value: Any) -> str:
    slug = slugify_vn(value).replace("-", "_").upper()
    aliases = {
        "POSITIVE": "HAS_POSITIVE_SIGNAL",
        "NEGATIVE": "HAS_NEGATIVE_SIGNAL",
        "GOOD_FOR_GROUP": "GOOD_FOR",
        "NEAR_TO": "NEAR",
        "HAS_PARKING": "HAS_AMENITY",
        "MENTIONS_ENTITY": "MENTIONS",
    }
    out = aliases.get(slug, slug)
    return out if out in RELATION_TYPES else "MENTIONS"


def entity_key(entity_type: str, name: str) -> str:
    return f"{canonical_entity_type(entity_type)}:{slugify_vn(name)}"


def _coerce_extraction(raw: Any) -> ReviewGraphExtraction:
    if isinstance(raw, ReviewGraphExtraction):
        return raw
    if hasattr(raw, "model_dump"):
        raw = raw.model_dump()
    if not isinstance(raw, dict):
        return ReviewGraphExtraction()
    return ReviewGraphExtraction.model_validate(raw)


def normalize_extraction(
    raw: Any,
    store_key: str,
    review_id: str,
    text_unit_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    extraction = _coerce_extraction(raw)
    entity_rows: list[dict] = []
    name_to_key: dict[str, str] = {}

    for entity in extraction.entities:
        name = normalize_text(entity.name)
        if not name:
            continue
        etype = canonical_entity_type(entity.type)
        key = entity_key(etype, name)
        name_to_key[slugify_vn(entity.name)] = key
        entity_rows.append({
            "entity_key": key,
            "name": name,
            "entity_type": etype,
            "store_key": str(store_key),
            "review_id": str(review_id),
            "text_unit_id": str(text_unit_id),
            "sentiment": entity.sentiment,
            "confidence": round(float(entity.confidence), 4),
            "evidence": entity.evidence,
        })

    relation_rows: list[dict] = []
    for rel in extraction.relations:
        source_slug = slugify_vn(rel.source_entity)
        target_slug = slugify_vn(rel.target_entity)
        source_key = name_to_key.get(source_slug)
        target_key = name_to_key.get(target_slug)
        if not source_key or not target_key or source_key == target_key:
            continue
        relation_rows.append({
            "relation_key": stable_hash({
                "text_unit_id": text_unit_id,
                "source": source_key,
                "target": target_key,
                "relation_type": canonical_relation_type(rel.relation_type),
            }),
            "source_entity_key": source_key,
            "target_entity_key": target_key,
            "relation_type": canonical_relation_type(rel.relation_type),
            "store_key": str(store_key),
            "review_id": str(review_id),
            "text_unit_id": str(text_unit_id),
            "sentiment": rel.sentiment,
            "confidence": round(float(rel.confidence), 4),
            "evidence": rel.evidence,
        })

    return pd.DataFrame(entity_rows), pd.DataFrame(relation_rows)


class LLMGraphExtractor:
    prompt_version = "restaurant_review_graph_v1"

    def __init__(self, config: AppConfig, llm=None):
        from langchain.prompts import ChatPromptTemplate
        from llm import get_llm

        self.config = config
        self.cache = JsonDiskCache(config.cache_dir, "llm_graph_extraction")
        llm = llm or get_llm(config)
        self.chain = ChatPromptTemplate.from_messages([
            ("system", """Bạn trích xuất entity và relation từ review quán ăn Việt Nam để xây knowledge graph.
Chỉ dùng thông tin có trong review, không suy diễn.

Entity type hợp lệ:
- dish: món cụ thể hoặc nhóm món được nhắc
- food_descriptor: đặc tính món/texture/vị như khô, cứng, mềm, cay, ngấy, tanh, đậm vị
- amenity: tiện ích như chỗ để xe, wifi, điều hòa
- location_landmark: mốc/khu vực như gần Hồ Tây, gần trường, trong ngõ
- occasion: ăn trưa, hẹn hò, tụ tập, takeaway
- audience: sinh viên, nhóm bạn, gia đình
- service_quality, space_quality, price_value, delivery_packaging, problem

Relation type hợp lệ:
MENTIONS, HAS_POSITIVE_SIGNAL, HAS_NEGATIVE_SIGNAL, GOOD_FOR, NEAR, HAS_AMENITY, HAS_PROBLEM, DESCRIBES_FOOD.

Giữ evidence ngắn, trích cụm từ trong review. Confidence 0-1."""),
            ("human", "Review: {review}"),
        ]) | llm.with_structured_output(ReviewGraphExtraction)

    def extract_review(self, review: str, store_key: str, review_id: str, text_unit_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        normalized = normalize_text(review)
        payload = {
            "model": self.config.llm_graph_extraction_model_id,
            "prompt_version": self.prompt_version,
            "review": normalized,
        }
        cache_key = stable_hash(payload)
        cached = self.cache.get(cache_key)
        if cached is None:
            cached = self.chain.invoke({"review": normalized})
            if hasattr(cached, "model_dump"):
                cached = cached.model_dump()
            self.cache.set(cache_key, cached)
        return normalize_extraction(cached, store_key=store_key, review_id=review_id, text_unit_id=text_unit_id)

    def extract_text_units(self, text_units: pd.DataFrame, text_col: str = "chunk_text") -> tuple[pd.DataFrame, pd.DataFrame]:
        entity_frames = []
        relation_frames = []
        for _, row in text_units.iterrows():
            text = row.get(text_col) or row.get("feedback") or ""
            entities, relations = self.extract_review(
                str(text),
                store_key=str(row.get("store_key")),
                review_id=str(row.get("review_id")),
                text_unit_id=str(row.get("text_unit_id")),
            )
            if not entities.empty:
                entity_frames.append(entities)
            if not relations.empty:
                relation_frames.append(relations)
        self.cache.save()
        entity_df = pd.concat(entity_frames, ignore_index=True) if entity_frames else pd.DataFrame()
        relation_df = pd.concat(relation_frames, ignore_index=True) if relation_frames else pd.DataFrame()
        return entity_df, relation_df
