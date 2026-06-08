from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from pathlib import Path

import pandas as pd


NODE_TYPE_MAP = {
    "Restaurant": "Store",
    "Category": "Category",
    "Cuisine": "Category",
    "MenuItem": "MenuItem",
    "Review": "Review",
    "TextUnit": "TextUnit",
    "CommunityReport": "CommunityReport",
    "ExtractedEntity": "ExtractedEntity",
    "ExtractedRelation": "ExtractedRelation",
}


def slugify(value: object) -> str:
    text = "" if value is None else str(value).strip().lower()
    text = text.replace("đ", "d").replace("Đ", "d")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def normalize_node_id(node_id: str, node_lookup: dict[str, dict[str, str]]) -> str:
    if node_id.startswith("restaurant:"):
        return f"store:{node_id.split(':', 1)[1]}"
    if node_id.startswith("category:") or node_id.startswith("cuisine:"):
        row = node_lookup.get(node_id, {})
        name = row.get("name") or row.get("normalized_name") or node_id.split(":", 1)[1]
        return f"category:{slugify(name)}"
    if node_id.startswith("entity:dish:"):
        return node_id
    return node_id


def normalize_relation(relation: str) -> str:
    if relation == "HAS_CUISINE":
        return "HAS_CATEGORY"
    return relation


def export(
    kg_dir: Path,
    output_nodes: Path,
    output_edges: Path,
    output_store_metadata: Path,
    processed_dir: Path | None = None,
) -> tuple[Path, Path, Path]:
    kg_nodes = pd.read_csv(kg_dir / "nodes.csv").fillna("")
    kg_edges = pd.read_csv(kg_dir / "edges.csv").fillna("")
    node_lookup = {str(row["id"]): {key: str(value) for key, value in row.items()} for _, row in kg_nodes.iterrows()}

    nodes: dict[str, str] = {}
    for _, row in kg_nodes.iterrows():
        raw_id = str(row["id"]).strip()
        if not raw_id:
            continue
        node_id = normalize_node_id(raw_id, node_lookup)
        node_type = NODE_TYPE_MAP.get(str(row.get("node_type", "")).strip(), str(row.get("node_type", "")).strip() or "Unknown")
        nodes[node_id] = node_type

    edges: set[tuple[str, str, str]] = set()
    for _, row in kg_edges.iterrows():
        src_id = normalize_node_id(str(row["src_id"]).strip(), node_lookup)
        dst_id = normalize_node_id(str(row["dst_id"]).strip(), node_lookup)
        relation = normalize_relation(str(row["relation"]).strip())
        if not src_id or not dst_id or not relation:
            continue
        nodes.setdefault(src_id, "Unknown")
        nodes.setdefault(dst_id, "Unknown")
        edges.add((src_id, relation, dst_id))

    output_nodes.parent.mkdir(parents=True, exist_ok=True)
    with output_nodes.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["node_id", "node_type"])
        writer.writeheader()
        for node_id, node_type in sorted(nodes.items()):
            writer.writerow({"node_id": node_id, "node_type": node_type})

    with output_edges.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["src_id", "relation", "dst_id"])
        writer.writeheader()
        for src_id, relation, dst_id in sorted(edges):
            writer.writerow({"src_id": src_id, "relation": relation, "dst_id": dst_id})

    restaurant_meta = {}
    if processed_dir is not None and (processed_dir / "canonical_restaurants.csv").exists():
        restaurants_df = pd.read_csv(processed_dir / "canonical_restaurants.csv").fillna("")
        for _, row in restaurants_df.iterrows():
            store_id = str(row.get("restaurant_id", "")).strip()
            if not store_id:
                continue
            price_min = row.get("price_min", "")
            price_max = row.get("price_max", "")
            try:
                price_values = [float(value) for value in (price_min, price_max) if str(value).strip()]
                median_price = sum(price_values) / len(price_values) if price_values else ""
            except ValueError:
                median_price = ""
            restaurant_meta[store_id] = {
                "store_name": row.get("name", ""),
                "latitude": row.get("latitude", ""),
                "longitude": row.get("longitude", ""),
                "rating": row.get("rating", ""),
                "review_count": row.get("review_count", ""),
                "median_price": median_price,
            }

    restaurants = kg_nodes[kg_nodes["node_type"].eq("Restaurant")].copy()
    metadata_rows = []
    for _, row in restaurants.iterrows():
        store_id = str(row["id"]).split(":", 1)[1]
        meta = restaurant_meta.get(store_id, {})
        metadata_rows.append(
            {
                "store_node_id": f"store:{store_id}",
                "store_id": store_id,
                "store_name": meta.get("store_name") or row.get("name", ""),
                "latitude": meta.get("latitude", ""),
                "longitude": meta.get("longitude", ""),
                "rating": meta.get("rating", ""),
                "review_count": meta.get("review_count", ""),
                "median_price": meta.get("median_price", ""),
            }
        )
    pd.DataFrame(metadata_rows).to_csv(output_store_metadata, index=False, encoding="utf-8-sig")
    return output_nodes, output_edges, output_store_metadata


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kg-dir", type=Path, required=True)
    parser.add_argument("--output-nodes", type=Path, default=Path("rgcn_pipeline/data/cachebuilt_graphrag_nodes.csv"))
    parser.add_argument("--output-edges", type=Path, default=Path("rgcn_pipeline/data/cachebuilt_graphrag_edges.csv"))
    parser.add_argument("--output-store-metadata", type=Path, default=Path("rgcn_pipeline/data/cachebuilt_store_metadata.csv"))
    parser.add_argument("--processed-dir", type=Path)
    args = parser.parse_args()
    print(export(args.kg_dir, args.output_nodes, args.output_edges, args.output_store_metadata, args.processed_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
