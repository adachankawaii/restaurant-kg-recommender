"""Graph-RAG KG builder for this repo (food / restaurant domain).

This script replaces the old demo KG builder (German legal ontology extraction).
It builds a *deterministic* knowledge graph from the structured tables in `kg_tables_all/`
(Store, Review, Category, ServiceOption, ContextTag, Aspect, MenuItem, Location, User...).

Outputs:
- `kg_tables_all/kg_graph/nodes.csv`
- `kg_tables_all/kg_graph/edges.csv`

Those files are compatible with `load_kg_to_neo4j.py` and the Neo4j docker compose.

Optional Graph-RAG step:
- Build text chunks from reviews + menu + store metadata
- Create embeddings and a simple vector index (FAISS) for retrieval

Requires:
- pandas
- numpy
- openai (optional, only if you enable embeddings)

"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

try:
    from openai import OpenAI  # optional
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = Path(".")
KG_DIR = BASE_DIR / "kg_tables_all"
GRAPH_DIR = KG_DIR / "kg_graph"


# --------------------
# IO helpers
# --------------------

def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


# --------------------
# Text / id normalization
# --------------------

def strip_accents(text: object) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def slugify(text: object) -> str:
    text = strip_accents(text).lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace(" ", "_")


def safe_int(x: object) -> int | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def safe_float(x: object) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def compact_json(props: dict[str, Any]) -> str:
    return json.dumps(props, ensure_ascii=False, sort_keys=True)


# --------------------
# Graph model helpers
# --------------------

def make_node(node_id: str, label: str, name: object = "", **props: Any) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "label": label,
        "name": "" if name is None else str(name),
        "properties": compact_json(props),
    }


def make_edge(source_id: str, relation: str, target_id: str, **props: Any) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "relation": relation,
        "target_id": target_id,
        "properties": compact_json(props),
    }


@dataclass
class KGTables:
    store_master: list[dict[str, str]]
    store_category: list[dict[str, str]]
    store_context_tag: list[dict[str, str]]
    store_service_option: list[dict[str, str]]
    store_aspect_agg: list[dict[str, str]]
    review_fact: list[dict[str, str]]
    store_source_map: list[dict[str, str]]
    store_menu_item: list[dict[str, str]]
    normalized_location: list[dict[str, str]]
    user_profile: list[dict[str, str]]
    user_session_context: list[dict[str, str]]
    user_preference: list[dict[str, str]]
    user_store_interaction: list[dict[str, str]]
    source_match_quality: list[dict[str, str]]


def load_tables(kg_dir: Path) -> KGTables:
    def r(name: str) -> list[dict[str, str]]:
        path = kg_dir / name
        if not path.exists():
            logging.warning("Missing table %s", path)
            return []
        return read_csv(path)

    return KGTables(
        store_master=r("store_master.csv"),
        store_category=r("store_category.csv"),
        store_context_tag=r("store_context_tag.csv"),
        store_service_option=r("store_service_option.csv"),
        store_aspect_agg=r("store_aspect_agg.csv"),
        review_fact=r("review_fact.csv"),
        store_source_map=r("store_source_map.csv"),
        store_menu_item=r("store_menu_item.csv"),
        normalized_location=r("normalized_location.csv"),
        user_profile=r("user_profile.csv"),
        user_session_context=r("user_session_context.csv"),
        user_preference=r("user_preference.csv"),
        user_store_interaction=r("user_store_interaction.csv"),
        source_match_quality=r("source_match_quality.csv"),
    )


def build_food_kg(tables: KGTables) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build a domain KG for restaurant / food recommender.

    Notes:
    - Node ids are stable string ids (prefixed).
    - Node `properties` is a JSON string; `load_kg_to_neo4j.py` stores it in Neo4j.
    """

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []

    def add_node(n: dict[str, Any]) -> None:
        nodes[str(n["node_id"])] = n

    # Stores
    for s in tables.store_master:
        store_id = s.get("store_id")
        if not store_id:
            continue
        sid = f"store:{store_id}"
        add_node(
            make_node(
                sid,
                "Store",
                s.get("canonical_name"),
                store_id=store_id,
                address=s.get("address"),
                ward=s.get("ward"),
                district=s.get("district"),
                city=s.get("city"),
                lat=safe_float(s.get("lat")),
                lng=safe_float(s.get("lng")),
                primary_category=s.get("primary_category"),
                median_price_vnd=safe_int(s.get("median_price_vnd")),
                be_rating_avg=safe_float(s.get("be_rating_avg")),
                be_rating_count=safe_int(s.get("be_rating_count")),
                google_place_id=s.get("google_place_id"),
                google_rating=safe_float(s.get("google_rating")),
                google_review_count=safe_int(s.get("google_review_count")),
                status=s.get("status"),
            )
        )

        # Primary category as explicit edge (useful for retrieval/explain)
        if s.get("primary_category"):
            cid = f"category:{slugify(s.get('primary_category'))}"
            add_node(make_node(cid, "Category", s.get("primary_category"), category_type="primary"))
            edges.append(make_edge(sid, "HAS_PRIMARY_CATEGORY", cid))

        # Area node (ward+district)
        if s.get("ward") or s.get("district"):
            area_key = f"{s.get('ward','')}::{s.get('district','')}::{s.get('city','')}"
            area_id = f"area:{slugify(area_key)}"
            area_name = ", ".join([p for p in [s.get("ward"), s.get("district"), s.get("city")] if p])
            add_node(make_node(area_id, "Area", area_name, ward=s.get("ward"), district=s.get("district"), city=s.get("city")))
            edges.append(make_edge(sid, "LOCATED_IN", area_id))

    # Categories
    for row in tables.store_category:
        store_id = row.get("store_id")
        value = row.get("category_value")
        if not store_id or not value:
            continue
        cid = f"category:{slugify(value)}"
        add_node(make_node(cid, "Category", value, category_type=row.get("category_type")))
        edges.append(make_edge(f"store:{store_id}", "HAS_CATEGORY", cid, source=row.get("source_name"), category_type=row.get("category_type")))

    # Context tags
    for row in tables.store_context_tag:
        store_id = row.get("store_id")
        tag_type = row.get("tag_type")
        tag_value = row.get("tag_value")
        if not store_id or not tag_value:
            continue
        tag_id = f"context:{slugify(tag_type)}:{slugify(tag_value)}"
        add_node(make_node(tag_id, "ContextTag", tag_value, tag_type=tag_type))
        edges.append(make_edge(f"store:{store_id}", "MATCHES_CONTEXT", tag_id, source=row.get("source_name")))

    # Service options
    for row in tables.store_service_option:
        store_id = row.get("store_id")
        service_option = row.get("service_option")
        if not store_id or not service_option:
            continue
        service_id = f"service:{slugify(service_option)}"
        add_node(make_node(service_id, "ServiceOption", service_option))
        edges.append(make_edge(f"store:{store_id}", "OFFERS_SERVICE", service_id, value=row.get("value"), source=row.get("source_name")))

    # Aspect aggregates
    for row in tables.store_aspect_agg:
        store_id = row.get("store_id")
        aspect_name = row.get("aspect_name")
        if not store_id or not aspect_name:
            continue
        aspect_id = f"aspect:{slugify(aspect_name)}"
        add_node(make_node(aspect_id, "Aspect", aspect_name))
        edges.append(
            make_edge(
                f"store:{store_id}",
                "HAS_ASPECT_SENTIMENT",
                aspect_id,
                aspect_sentiment=row.get("aspect_sentiment"),
                mention_count=safe_int(row.get("mention_count")),
                positive=safe_int(row.get("positive_mentions")),
                negative=safe_int(row.get("negative_mentions")),
                neutral=safe_int(row.get("neutral_mentions")),
                evidence_sources=row.get("evidence_sources"),
            )
        )

    # Reviews
    for row in tables.review_fact:
        review_id = row.get("review_id")
        store_id = row.get("store_id")
        if not review_id or not store_id:
            continue
        rid = f"review:{review_id}"
        text = row.get("review_text") or ""
        add_node(
            make_node(
                rid,
                "Review",
                text[:80],
                review_id=review_id,
                source=row.get("source_name"),
                rated_at=row.get("rated_at"),
                rating_5=safe_float(row.get("rating_5")),
                sentiment=row.get("sentiment"),
                is_promo=safe_int(row.get("is_promo")),
                reviewer=row.get("reviewer"),
                review_text=text,
            )
        )
        edges.append(make_edge(rid, "REVIEWS", f"store:{store_id}", rated_at=row.get("rated_at")))

    # Source map
    for row in tables.store_source_map:
        store_id = row.get("store_id")
        source_name = row.get("source_name")
        if not store_id or not source_name:
            continue
        source_id = f"source:{slugify(source_name)}"
        add_node(make_node(source_id, "DataSource", source_name))
        edges.append(
            make_edge(
                f"store:{store_id}",
                "HAS_SOURCE_RECORD",
                source_id,
                source_key=row.get("source_store_key"),
                match_confidence=row.get("match_confidence"),
            )
        )

    # Menu items
    for row in tables.store_menu_item:
        store_id = row.get("store_id")
        menu_item_id = row.get("menu_item_id")
        if not store_id or not menu_item_id:
            continue
        mid = f"menu_item:{menu_item_id}"
        add_node(
            make_node(
                mid,
                "MenuItem",
                row.get("item_name"),
                menu_item_id=menu_item_id,
                item_category=row.get("item_category"),
                price_vnd=safe_int(row.get("price_vnd")),
                is_signature=safe_int(row.get("is_signature")),
                spicy_level=safe_int(row.get("spicy_level")),
                dietary_tag=row.get("dietary_tag"),
                source=row.get("source_name"),
            )
        )
        edges.append(make_edge(f"store:{store_id}", "SELLS_MENU_ITEM", mid, source=row.get("source_name")))

    # Normalized location
    for row in tables.normalized_location:
        store_id = row.get("store_id")
        location_id = row.get("location_id")
        if not store_id or not location_id:
            continue
        lid = f"location:{location_id}"
        add_node(
            make_node(
                lid,
                "Location",
                row.get("canonical_address"),
                store_id=store_id,
                street=row.get("street"),
                ward=row.get("ward"),
                district=row.get("district"),
                city=row.get("city"),
                lat=safe_float(row.get("lat")),
                lng=safe_float(row.get("lng")),
                area_tag=row.get("area_tag"),
            )
        )
        edges.append(make_edge(f"store:{store_id}", "HAS_NORMALIZED_LOCATION", lid))

    # User tables (if present)
    for row in tables.user_profile:
        user_id = row.get("user_id")
        if not user_id:
            continue
        uid = f"user:{user_id}"
        add_node(make_node(uid, "User", row.get("display_name"), dietary_profile=row.get("dietary_profile"), default_budget_vnd=safe_int(row.get("default_budget_vnd")), home_district=row.get("home_district")))

    for row in tables.user_session_context:
        session_id_raw = row.get("session_id")
        user_id = row.get("user_id")
        if not session_id_raw or not user_id:
            continue
        sess_id = f"session:{session_id_raw}"
        add_node(
            make_node(
                sess_id,
                "UserSession",
                session_id_raw,
                user_id=user_id,
                query_time=row.get("query_time"),
                current_lat=safe_float(row.get("current_lat")),
                current_lng=safe_float(row.get("current_lng")),
                radius_m=safe_int(row.get("radius_m")),
                party_size=safe_int(row.get("party_size")),
                budget_min_vnd=safe_int(row.get("budget_min_vnd")),
                budget_max_vnd=safe_int(row.get("budget_max_vnd")),
                intent=row.get("intent"),
                desired_service=row.get("desired_service"),
                weather_context=row.get("weather_context"),
                time_context=row.get("time_context"),
            )
        )
        edges.append(make_edge(f"user:{user_id}", "HAS_SESSION", sess_id))
        if row.get("intent"):
            intent_tag = f"context:intent:{slugify(row.get('intent'))}"
            add_node(make_node(intent_tag, "ContextTag", row.get("intent"), tag_type="intent"))
            edges.append(make_edge(sess_id, "REQUESTS_CONTEXT", intent_tag, desired_service=row.get("desired_service"), radius_m=row.get("radius_m")))

    for row in tables.user_preference:
        user_id = row.get("user_id")
        pref_type = row.get("preference_type")
        pref_value = row.get("preference_value")
        if not user_id or not pref_type or not pref_value:
            continue
        pid = f"pref:{user_id}:{slugify(pref_type)}:{slugify(pref_value)}"
        add_node(make_node(pid, "UserPreference", pref_value, user_id=user_id, preference_type=pref_type, weight=safe_float(row.get("weight")), source=row.get("source")))
        edges.append(make_edge(f"user:{user_id}", "HAS_PREFERENCE", pid, source=row.get("source")))

    for row in tables.user_store_interaction:
        user_id = row.get("user_id")
        store_id = row.get("store_id")
        if not user_id or not store_id:
            continue
        edges.append(
            make_edge(
                f"user:{user_id}",
                "INTERACTED_WITH",
                f"store:{store_id}",
                interaction_id=row.get("interaction_id"),
                interaction_type=row.get("interaction_type"),
                interacted_at=row.get("interacted_at"),
                rating_5=safe_float(row.get("rating_5")),
                session_id=row.get("session_id"),
                source=row.get("source"),
            )
        )

    for row in tables.source_match_quality:
        store_id = row.get("store_id")
        source_name = row.get("source_name")
        if not store_id or not source_name:
            continue
        qid = f"match_quality:{store_id}:{slugify(source_name)}"
        add_node(
            make_node(
                qid,
                "SourceMatchQuality",
                f"{store_id}:{source_name}",
                store_id=store_id,
                source_name=source_name,
                match_confidence_recomputed=row.get("match_confidence_recomputed"),
                geo_distance_m=safe_float(row.get("geo_distance_m")),
                name_similarity=safe_float(row.get("name_similarity")),
                address_similarity=safe_float(row.get("address_similarity")),
                is_suspect=safe_int(row.get("is_suspect")),
                note=row.get("note"),
            )
        )
        edges.append(make_edge(f"store:{store_id}", "HAS_SOURCE_MATCH_QUALITY", qid))

    return list(nodes.values()), edges


# --------------------
# Graph-RAG chunks + embeddings (optional)
# --------------------

def build_store_documents(tables: KGTables, max_reviews_per_store: int = 10) -> list[dict[str, Any]]:
    """Create text chunks per store for retrieval (Graph-RAG).

    Each document is a dict with keys: doc_id, store_id, text, metadata.
    """

    reviews_by_store: dict[str, list[dict[str, str]]] = {}
    for r in tables.review_fact:
        sid = r.get("store_id")
        if not sid:
            continue
        reviews_by_store.setdefault(str(sid), []).append(r)

    menu_by_store: dict[str, list[dict[str, str]]] = {}
    for m in tables.store_menu_item:
        sid = m.get("store_id")
        if not sid:
            continue
        menu_by_store.setdefault(str(sid), []).append(m)

    docs: list[dict[str, Any]] = []
    for s in tables.store_master:
        store_id = s.get("store_id")
        if not store_id:
            continue

        review_rows = reviews_by_store.get(str(store_id), [])
        # prefer newest/highest rating first if timestamp parses poorly; keep deterministic order
        review_rows = sorted(review_rows, key=lambda x: (x.get("rated_at") or "", x.get("review_id") or ""), reverse=True)
        review_rows = review_rows[:max_reviews_per_store]

        menu_rows = menu_by_store.get(str(store_id), [])
        menu_rows = sorted(menu_rows, key=lambda x: (x.get("is_signature") or "0", x.get("menu_item_id") or ""), reverse=True)

        parts: list[str] = []
        parts.append(f"Tên quán: {s.get('canonical_name','')}")
        parts.append(f"Địa chỉ: {s.get('address','')}")
        if s.get("primary_category"):
            parts.append(f"Nhóm: {s.get('primary_category')}")
        if s.get("median_price_vnd"):
            parts.append(f"Giá phổ biến (VND): {s.get('median_price_vnd')}")
        if s.get("be_rating_avg"):
            parts.append(f"BE rating avg: {s.get('be_rating_avg')} (count={s.get('be_rating_count')})")
        if s.get("google_rating"):
            parts.append(f"Google rating: {s.get('google_rating')} (count={s.get('google_review_count')})")

        if menu_rows:
            menu_lines = []
            for it in menu_rows[:12]:
                name = it.get("item_name") or ""
                price = it.get("price_vnd") or ""
                menu_lines.append(f"- {name} ({price} VND)")
            parts.append("Món gợi ý:\n" + "\n".join(menu_lines))

        if review_rows:
            review_lines = []
            for rv in review_rows:
                rating = rv.get("rating_5") or ""
                text = (rv.get("review_text") or "").replace("\n", " ").strip()
                if len(text) > 280:
                    text = text[:280] + "…"
                review_lines.append(f"- ({rating}/5) {text}")
            parts.append("Review gần đây:\n" + "\n".join(review_lines))

        text = "\n".join([p for p in parts if p.strip()])
        docs.append(
            {
                "doc_id": f"store_doc:{store_id}",
                "store_id": str(store_id),
                "text": text,
                "metadata": {
                    "store_id": str(store_id),
                    "name": s.get("canonical_name"),
                    "primary_category": s.get("primary_category"),
                    "ward": s.get("ward"),
                    "district": s.get("district"),
                },
            }
        )

    return docs


def embed_texts_openai(texts: list[str], model: str = "text-embedding-3-small") -> np.ndarray:
    if OpenAI is None:
        raise RuntimeError("openai package not available; install and retry")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY in environment")

    client = OpenAI(api_key=api_key)
    resp = client.embeddings.create(model=model, input=texts)
    vectors = [d.embedding for d in resp.data]
    return np.array(vectors, dtype=np.float32)


def maybe_build_vector_index(output_dir: Path, docs: list[dict[str, Any]], enable: bool, embedding_model: str) -> None:
    if not enable:
        return

    try:
        import faiss  # type: ignore
    except Exception as e:
        raise RuntimeError("FAISS not installed. `pip install faiss-cpu` (or faiss-gpu).") from e

    texts = [d["text"] for d in docs]
    vectors = embed_texts_openai(texts, model=embedding_model)

    index = faiss.IndexFlatIP(vectors.shape[1])
    faiss.normalize_L2(vectors)
    index.add(vectors)

    output_dir.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(output_dir / "store_docs.faiss"))

    with (output_dir / "store_docs.jsonl").open("w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    logging.info("Wrote vector index to %s", output_dir)


# --------------------
# CLI
# --------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build Food Knowledge Graph (Graph-RAG ready) from kg_tables_all/*.csv")
    p.add_argument("--kg-dir", type=Path, default=KG_DIR, help="Input folder containing KG tables")
    p.add_argument("--out-dir", type=Path, default=GRAPH_DIR, help="Output folder for graph csv")
    p.add_argument("--with-vector", action="store_true", help="Build FAISS vector index for store documents")
    p.add_argument("--embedding-model", default="text-embedding-3-small", help="OpenAI embedding model")
    p.add_argument("--max-reviews", type=int, default=10, help="Max reviews included in each store document")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    tables = load_tables(args.kg_dir)

    nodes, edges = build_food_kg(tables)
    write_csv(args.out_dir / "nodes.csv", nodes, ["node_id", "label", "name", "properties"])
    write_csv(args.out_dir / "edges.csv", edges, ["source_id", "relation", "target_id", "properties"])

    docs = build_store_documents(tables, max_reviews_per_store=args.max_reviews)
    docs_dir = args.out_dir
    with (docs_dir / "store_docs.jsonl").open("w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    maybe_build_vector_index(docs_dir, docs, enable=args.with_vector, embedding_model=args.embedding_model)

    logging.info("KG nodes=%s edges=%s", len(nodes), len(edges))
    logging.info("Wrote: %s", args.out_dir)


if __name__ == "__main__":
    main()

# NOTE: The old demo visualization / NetworkX community detection code was removed.
# Use Neo4j Browser (or `neo4j_visualize.cypher`) to visualize the KG after importing.