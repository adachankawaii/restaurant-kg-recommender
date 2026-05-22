from __future__ import annotations

import hashlib
from typing import Optional

import pandas as pd

from cache import JsonDiskCache
from config import AppConfig
from ranker import add_user_distance_to_record


class EmbeddingService:
    def __init__(self, config: AppConfig):
        from sentence_transformers import SentenceTransformer

        self.config = config
        self.model = SentenceTransformer(config.embed_model)
        self.cache = JsonDiskCache(config.cache_dir, "embeddings")
        self.dim = len(self.embed_passage("embedding dimension check"))

    def _embed(self, text: str, prefix: str = "") -> list[float]:
        payload = {"model": self.config.embed_model, "prefix": prefix, "text": text}
        return self.cache.get_or_compute(payload, lambda: self.model.encode(prefix + text, normalize_embeddings=True).tolist())

    def embed_passage(self, text: str) -> list[float]:
        return self._embed(text, self.config.embed_prefix_passage)

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text, self.config.embed_prefix_query)

    def save_cache(self) -> None:
        self.cache.save()


def stable_int_id(text: str) -> int:
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:8], 16)


def build_restaurant_summary_doc(row: pd.Series, attr_text: str = "") -> str:
    menu_text = ", ".join(row.get("top_menu_items") or [])
    return "\n".join([
        f"Restaurant name: {row.get('name')}",
        f"Address: {row.get('address')}",
        f"Area: {row.get('district')}, {row.get('city')}",
        f"Distance from user: {row.get('distance_km')} km",
        f"Rating: {row.get('rating')}",
        f"Review count: {row.get('review_count')}",
        f"Price band: {row.get('price_band')}",
        f"Source price range: {row.get('price_min')} - {row.get('price_max')}",
        f"Menu price range: {row.get('menu_price_min')} - {row.get('menu_price_max')}, median={row.get('menu_price_median')}",
        f"Categories: {', '.join(row.get('categories') or [])}",
        f"Cuisines: {', '.join(row.get('cuisines') or [])}",
        f"Top menu items: {menu_text}",
        f"Opening hours: {row.get('opening_hours')}",
        f"Delivery time estimate: {row.get('delivery_time')}",
        f"Aggregated aspect sentiment: {attr_text}",
    ])


class QdrantVectorStore:
    def __init__(self, config: AppConfig, embedding_service: EmbeddingService):
        from qdrant_client import QdrantClient

        self.config = config
        self.embedding_service = embedding_service
        self.client = QdrantClient(host=config.qdrant_host, port=config.qdrant_port)

    def ensure_collections(self) -> None:
        from qdrant_client.models import Distance, VectorParams

        existing = {c.name for c in self.client.get_collections().collections}
        for name in [self.config.coll_restaurant, self.config.coll_text_unit]:
            if name in existing and self.config.recreate_qdrant:
                self.client.delete_collection(name)
                existing.remove(name)
            if name not in existing:
                self.client.create_collection(name, vectors_config=VectorParams(size=self.embedding_service.dim, distance=Distance.COSINE))

    def index_restaurants(self, summary: pd.DataFrame, restaurant_attrs: Optional[pd.DataFrame] = None) -> None:
        from qdrant_client.models import PointStruct

        attr_map: dict[str, str] = {}
        if restaurant_attrs is not None and not restaurant_attrs.empty:
            for store_key, grp in restaurant_attrs.groupby("store_key"):
                attr_map[store_key] = ", ".join(f"{r.attribute_type}={r.attribute_score:+.2f}" for _, r in grp.iterrows())

        points = []
        for _, row in summary.iterrows():
            doc = build_restaurant_summary_doc(row, attr_map.get(str(row["store_key"]), ""))
            vector = self.embedding_service.embed_passage(doc)
            points.append(PointStruct(
                id=stable_int_id("rest-" + str(row["store_key"])),
                vector=vector,
                payload={
                    "store_key": str(row["store_key"]),
                    "name": row.get("name"),
                    "address": row.get("address"),
                    "district": row.get("district"),
                    "city": row.get("city"),
                    "rating": row.get("rating"),
                    "lat": row.get("lat"),
                    "lng": row.get("lng"),
                    "distance_km": row.get("distance_km"),
                    "price_band": row.get("price_band"),
                    "top_menu_items": row.get("top_menu_items"),
                    "menu_price_min": row.get("menu_price_min"),
                    "menu_price_max": row.get("menu_price_max"),
                    "doc_text": doc,
                    "doc_type": "restaurant_summary",
                },
            ))
        if points:
            self.client.upsert(collection_name=self.config.coll_restaurant, points=points)

    def search_restaurants(self, query: str, top_k: int = 8, user_lat: Optional[float] = None, user_lng: Optional[float] = None) -> list[dict]:
        hits = self.client.search(
            collection_name=self.config.coll_restaurant,
            query_vector=self.embedding_service.embed_query(query),
            limit=top_k,
            with_payload=True,
        )
        rows = []
        for hit in hits:
            payload = hit.payload or {}
            rec = {
                "store_key": payload.get("store_key"),
                "name": payload.get("name"),
                "address": payload.get("address"),
                "district": payload.get("district"),
                "city": payload.get("city"),
                "rating": payload.get("rating"),
                "lat": payload.get("lat"),
                "lng": payload.get("lng"),
                "price_band": payload.get("price_band"),
                "top_menu_items": payload.get("top_menu_items"),
                "menu_price_min": payload.get("menu_price_min"),
                "menu_price_max": payload.get("menu_price_max"),
                "doc_text": payload.get("doc_text"),
                "vec_score": round(float(hit.score), 4),
                "source": "restaurant_vector",
            }
            rows.append(add_user_distance_to_record(rec, user_lat, user_lng, self.config.distance_decay_km))
        return rows

    def search_text_units(self, query: str, top_k: int = 16, store_keys: Optional[list[str]] = None) -> list[dict]:
        hits = self.client.search(
            collection_name=self.config.coll_text_unit,
            query_vector=self.embedding_service.embed_query(query),
            limit=top_k,
            with_payload=True,
        )
        allow = set(store_keys) if store_keys else None
        rows = []
        for hit in hits:
            payload = hit.payload or {}
            if allow and payload.get("store_key") not in allow:
                continue
            rows.append({
                "store_key": payload.get("store_key"),
                "text_unit_id": payload.get("text_unit_id"),
                "review_id": payload.get("review_id"),
                "store_name": payload.get("store_name"),
                "rating": payload.get("rating"),
                "sentiment": payload.get("sentiment"),
                "feedback": payload.get("feedback"),
                "aspect_scores": payload.get("aspect_scores"),
                "doc_text": payload.get("doc_text"),
                "vec_score": round(float(hit.score), 4),
                "source": "text_unit_vector",
            })
        return rows

