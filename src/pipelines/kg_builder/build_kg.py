from __future__ import annotations

import os
import ast
import json
from pathlib import Path

import pandas as pd

from common import dump_json, latest_complete_dir, make_run_id, utc_now_iso
from pipelines.kg_builder.export_snapshot import write_snapshot_csv
from pipelines.kg_builder.neo4j_writer import Neo4jWriter
from settings import Settings


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _parse_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except (SyntaxError, ValueError):
            pass
    return [part.strip() for part in text.split("|") if part.strip()]


def _parse_aspect_scores(value) -> dict[str, float]:
    if isinstance(value, dict):
        raw = value
    else:
        text = str(value or "").strip()
        if not text:
            return {}
        try:
            raw = json.loads(text.replace("'", '"'))
        except json.JSONDecodeError:
            try:
                raw = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                return {}
    if not isinstance(raw, dict):
        return {}
    out = {}
    for key, score in raw.items():
        try:
            out[str(key)] = float(score)
        except (TypeError, ValueError):
            continue
    return out


def _price_band(price_min, price_max) -> str | None:
    prices = []
    for value in (price_min, price_max):
        try:
            if pd.notna(value) and str(value).strip():
                prices.append(float(value))
        except (TypeError, ValueError):
            pass
    if not prices:
        return None
    price = min(prices)
    if price <= 30000:
        return "budget"
    if price <= 70000:
        return "mid"
    return "premium"


def _latest_processed_dir(processed_root: Path) -> Path:
    return latest_complete_dir(
        processed_root,
        ["canonical_restaurants.csv", "canonical_menu_items.csv"],
        "processed ingestion output",
    )


def _dedupe_rows(rows: list[dict[str, object]], keys: tuple[str, ...]) -> list[dict[str, object]]:
    seen = set()
    out = []
    for row in rows:
        key = tuple(str(row.get(item, "")) for item in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def build_kg_snapshot(settings: Settings) -> dict[str, object]:
    processed_dir = _latest_processed_dir(settings.paths.processed_root)
    restaurants = pd.read_csv(processed_dir / "canonical_restaurants.csv")
    menu_items = _read_optional_csv(processed_dir / "menu_items_enriched.csv")
    if menu_items.empty:
        menu_items = pd.read_csv(processed_dir / "canonical_menu_items.csv")
    feedback = pd.read_csv(processed_dir / "feedback.csv").fillna("")
    text_units = pd.read_csv(processed_dir / "text_units.csv").fillna("")
    extracted_entities = _read_optional_csv(processed_dir / "extracted_entities.csv")
    extracted_relations = _read_optional_csv(processed_dir / "extracted_relations.csv")
    community_reports = _read_optional_csv(processed_dir / "community_reports.csv")

    graph_version = make_run_id("kg")
    snapshot_dir = settings.paths.kg_root / graph_version

    nodes: list[dict[str, object]] = []
    edges: list[dict[str, object]] = []

    for _, row in restaurants.iterrows():
        restaurant_id = str(row["restaurant_id"])
        nodes.append(
            {
                "id": f"restaurant:{restaurant_id}",
                "node_type": "Restaurant",
                "name": row["name"],
                "normalized_name": row["normalized_name"],
                "graph_version": graph_version,
                "data_version": row.get("data_version", "offline"),
            }
        )
        for category in _parse_list(row.get("categories")):
            category_id = f"category:{category}"
            nodes.append(
                {
                    "id": category_id,
                    "node_type": "Category",
                    "name": category,
                    "normalized_name": category.lower(),
                    "graph_version": graph_version,
                    "data_version": row.get("data_version", "offline"),
                }
            )
            edges.append(
                {
                    "src_id": f"restaurant:{restaurant_id}",
                    "relation": "HAS_CATEGORY",
                    "dst_id": category_id,
                    "graph_version": graph_version,
                }
            )
        band = _price_band(row.get("price_min"), row.get("price_max"))
        if band:
            price_id = f"price_band:{band}"
            nodes.append(
                {
                    "id": price_id,
                    "node_type": "PriceBand",
                    "name": band,
                    "normalized_name": band,
                    "graph_version": graph_version,
                    "data_version": row.get("data_version", "offline"),
                }
            )
            edges.append(
                {
                    "src_id": f"restaurant:{restaurant_id}",
                    "relation": "HAS_PRICE_BAND",
                    "dst_id": price_id,
                    "graph_version": graph_version,
                }
            )
        if pd.notna(row.get("cuisine_type")):
            cuisine_id = f"cuisine:{row['cuisine_type']}"
            nodes.append(
                {
                    "id": cuisine_id,
                    "node_type": "Cuisine",
                    "name": row["cuisine_type"],
                    "normalized_name": str(row["cuisine_type"]).lower(),
                    "graph_version": graph_version,
                    "data_version": row.get("data_version", "offline"),
                }
            )
            edges.append(
                {
                    "src_id": f"restaurant:{restaurant_id}",
                    "relation": "HAS_CUISINE",
                    "dst_id": cuisine_id,
                    "graph_version": graph_version,
                }
            )

    for _, row in menu_items.iterrows():
        item_id = str(row.get("menu_item_id") or row.get("item_id"))
        restaurant_id = str(row.get("store_key") or row.get("restaurant_id"))
        item_name = row.get("item_name") or row.get("name")
        category = row.get("category_name") or row.get("category")
        dish_family = row.get("dish_family")
        nodes.append(
            {
                "id": f"menu_item:{item_id}",
                "node_type": "MenuItem",
                "name": item_name,
                "normalized_name": row.get("normalized_name", str(item_name).lower()),
                "graph_version": graph_version,
                "data_version": row.get("data_version", "offline"),
            }
        )
        edges.append(
            {
                "src_id": f"restaurant:{restaurant_id}",
                "relation": "HAS_ITEM",
                "dst_id": f"menu_item:{item_id}",
                "graph_version": graph_version,
            }
        )
        edges.append(
            {
                "src_id": f"restaurant:{restaurant_id}",
                "relation": "HAS_MENU_ITEM",
                "dst_id": f"menu_item:{item_id}",
                "graph_version": graph_version,
            }
        )
        if pd.notna(category) and str(category).strip():
            category_id = f"menu_category:{category}"
            nodes.append(
                {
                    "id": category_id,
                    "node_type": "MenuCategory",
                    "name": category,
                    "normalized_name": str(category).lower(),
                    "graph_version": graph_version,
                    "data_version": row.get("data_version", "offline"),
                }
            )
            edges.append({"src_id": f"menu_item:{item_id}", "relation": "IN_MENU_CATEGORY", "dst_id": category_id, "graph_version": graph_version})
            edges.append({"src_id": f"restaurant:{restaurant_id}", "relation": "HAS_MENU_CATEGORY", "dst_id": category_id, "graph_version": graph_version})
        if pd.notna(dish_family) and str(dish_family).strip():
            dish_id = f"dish_family:{dish_family}"
            nodes.append(
                {
                    "id": dish_id,
                    "node_type": "DishFamily",
                    "name": dish_family,
                    "normalized_name": str(dish_family).lower(),
                    "graph_version": graph_version,
                    "data_version": row.get("data_version", "offline"),
                }
            )
            edges.append({"src_id": f"restaurant:{restaurant_id}", "relation": "SERVES_FAMILY", "dst_id": dish_id, "graph_version": graph_version})
            edges.append({"src_id": f"menu_item:{item_id}", "relation": "BELONGS_TO_FAMILY", "dst_id": dish_id, "graph_version": graph_version})

    for _, row in feedback.iterrows():
        review_id = str(row["review_id"])
        store_key = str(row["store_key"])
        nodes.append(
            {
                "id": f"review:{review_id}",
                "node_type": "Review",
                "name": row.get("feedback", "")[:120],
                "normalized_name": row.get("sentiment", "neutral"),
                "graph_version": graph_version,
                "data_version": "offline",
            }
        )
        edges.append({"src_id": f"restaurant:{store_key}", "relation": "HAS_REVIEW", "dst_id": f"review:{review_id}", "graph_version": graph_version})
        for aspect, score in _parse_aspect_scores(row.get("aspect_scores")).items():
            attr_id = f"attribute:{store_key}:{aspect}"
            nodes.append(
                {
                    "id": attr_id,
                    "node_type": "Attribute",
                    "name": aspect,
                    "normalized_name": aspect,
                    "graph_version": graph_version,
                    "data_version": "offline",
                }
            )
            edges.append({"src_id": f"restaurant:{store_key}", "relation": "HAS_ATTRIBUTE", "dst_id": attr_id, "graph_version": graph_version})

    for _, row in text_units.iterrows():
        text_unit_id = str(row["text_unit_id"])
        review_id = str(row["review_id"])
        store_key = str(row["store_key"])
        nodes.append(
            {
                "id": f"text_unit:{text_unit_id}",
                "node_type": "TextUnit",
                "name": row.get("chunk_text", "")[:120],
                "normalized_name": row.get("sentiment", "neutral"),
                "graph_version": graph_version,
                "data_version": "offline",
            }
        )
        edges.append({"src_id": f"review:{review_id}", "relation": "HAS_TEXT_UNIT", "dst_id": f"text_unit:{text_unit_id}", "graph_version": graph_version})
        edges.append({"src_id": f"text_unit:{text_unit_id}", "relation": "ABOUT", "dst_id": f"restaurant:{store_key}", "graph_version": graph_version})

    for _, row in extracted_entities.iterrows():
        entity_key = str(row.get("entity_key", "")).strip()
        if not entity_key:
            continue
        text_unit_id = str(row.get("text_unit_id", "")).strip()
        store_key = str(row.get("store_key", "")).strip()
        nodes.append(
            {
                "id": f"entity:{entity_key}",
                "node_type": "ExtractedEntity",
                "name": row.get("name", ""),
                "normalized_name": row.get("entity_type", ""),
                "graph_version": graph_version,
                "data_version": "offline",
            }
        )
        if text_unit_id:
            edges.append({"src_id": f"text_unit:{text_unit_id}", "relation": "MENTIONS_ENTITY", "dst_id": f"entity:{entity_key}", "graph_version": graph_version})
        if store_key:
            edges.append({"src_id": f"restaurant:{store_key}", "relation": "HAS_EXTRACTED_ENTITY", "dst_id": f"entity:{entity_key}", "graph_version": graph_version})

    for _, row in extracted_relations.iterrows():
        relation_key = str(row.get("relation_key", "")).strip()
        src_entity = str(row.get("source_entity_key", "")).strip()
        dst_entity = str(row.get("target_entity_key", "")).strip()
        if not relation_key or not src_entity or not dst_entity:
            continue
        nodes.append(
            {
                "id": f"relation:{relation_key}",
                "node_type": "ExtractedRelation",
                "name": row.get("relation_type", ""),
                "normalized_name": row.get("relation_type", ""),
                "graph_version": graph_version,
                "data_version": "offline",
            }
        )
        edges.append({"src_id": f"entity:{src_entity}", "relation": "SOURCE_OF", "dst_id": f"relation:{relation_key}", "graph_version": graph_version})
        edges.append({"src_id": f"relation:{relation_key}", "relation": "TARGETS", "dst_id": f"entity:{dst_entity}", "graph_version": graph_version})

    for index, row in community_reports.iterrows():
        report_id = f"community_report:{index}"
        nodes.append(
            {
                "id": report_id,
                "node_type": "CommunityReport",
                "name": row.get("title", ""),
                "normalized_name": row.get("summary", "")[:120],
                "graph_version": graph_version,
                "data_version": "offline",
            }
        )

    nodes = _dedupe_rows(nodes, ("id", "node_type"))
    edges = _dedupe_rows(edges, ("src_id", "relation", "dst_id"))
    nodes_path = write_snapshot_csv(nodes, snapshot_dir / "nodes.csv")
    edges_path = write_snapshot_csv(edges, snapshot_dir / "edges.csv")
    neo4j_result = {"status": "disabled"}
    if settings.use_neo4j:
        neo4j_result = Neo4jWriter(
            uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            user=os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j")),
            password=os.getenv("NEO4J_PASSWORD", "password"),
            database=os.getenv("NEO4J_DATABASE", "neo4j"),
        ).write_graphrag_snapshot(
            graph_version=graph_version,
            processed_dir=processed_dir,
            cache_root=settings.root.parent / "cache" / ".cache" / "graphrag",
            exported_nodes_path=settings.root.parent / "src" / "rgcn_pipeline" / "data" / "graphrag_nodes.csv",
            exported_edges_path=settings.root.parent / "src" / "rgcn_pipeline" / "data" / "graphrag_edges.csv",
        )
        if neo4j_result.get("status") == "failed" and settings.config.get("neo4j", {}).get("required", False):
            raise RuntimeError(f"Neo4j write failed: {neo4j_result.get('reason')}")

    manifest = {
        "graph_version": graph_version,
        "mode": settings.mode,
        "source_processed_dir": str(processed_dir),
        "nodes_path": str(nodes_path),
        "edges_path": str(edges_path),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "neo4j": neo4j_result,
        "created_at": utc_now_iso(),
    }
    dump_json(snapshot_dir / "manifest.json", manifest)
    (settings.paths.kg_root / "ACTIVE_VERSION").write_text(graph_version, encoding="utf-8")
    return manifest
