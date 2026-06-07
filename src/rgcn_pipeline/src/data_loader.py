from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path
from typing import Any

import torch


DEFAULT_EXCLUDED_RELATIONS = {
    "HAS_SOURCE_RECORD",
    "HAS_SOURCE_MATCH_QUALITY",
}

NODE_ID_COLUMNS = ("node_id", "id")
NODE_TYPE_COLUMNS = ("node_type", "label", "type")
EDGE_SRC_COLUMNS = ("src_id", "source_id", "src", "source", "head")
EDGE_DST_COLUMNS = ("dst_id", "target_id", "dst", "target", "tail")
EDGE_REL_COLUMNS = ("relation", "rel", "edge_type", "predicate")

GRAPHRAG_LABEL_PRIORITY = (
    "Restaurant",
    "Review",
    "TextUnit",
    "Attribute",
    "Area",
    "Cuisine",
    "Category",
    "PriceBand",
    "AtmosphereTag",
    "MenuItem",
    "MenuCategory",
    "Community",
    "CommunityReport",
    "DishEntity",
    "DishFamily",
    "ExtractedEntity",
    "ExtractedRelation",
)

GRAPHRAG_NODE_TYPES = {
    "Restaurant": "Store",
    "Review": "Review",
    "TextUnit": "TextUnit",
    "Attribute": "Attribute",
    "Area": "Area",
    "Cuisine": "Cuisine",
    "Category": "Category",
    "PriceBand": "PriceBand",
    "AtmosphereTag": "AtmosphereTag",
    "MenuItem": "MenuItem",
    "MenuCategory": "MenuCategory",
    "Community": "Community",
    "CommunityReport": "CommunityReport",
    "DishEntity": "DishEntity",
    "DishFamily": "DishFamily",
    "ExtractedEntity": "ExtractedEntity",
    "ExtractedRelation": "ExtractedRelation",
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def _pick(row: dict[str, Any], candidates: tuple[str, ...], default: str = "") -> str:
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def _relation_allowed(relation: str, exclude_relations: set[str]) -> bool:
    relation = relation.strip()
    return bool(relation) and relation.upper() not in exclude_relations


def _slugify_id(value: object) -> str:
    text = "" if value is None else str(value).strip().lower()
    text = text.replace("\u0111", "d")
    text = text.replace("đ", "d")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("\u0111", "d")
    text = text.replace("đ", "d")
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def _primary_graphrag_label(labels: list[str]) -> str | None:
    label_set = set(labels)
    for label in GRAPHRAG_LABEL_PRIORITY:
        if label in label_set:
            return label
    return None


def _graphrag_node_id(labels: list[str], props: dict[str, Any]) -> str | None:
    label = _primary_graphrag_label(labels)
    if label is None:
        return None

    if label == "Restaurant":
        store_key = props.get("store_key")
        return f"store:{store_key}" if store_key not in {None, ""} else None
    if label == "Review":
        review_id = props.get("review_id")
        return f"review:{review_id}" if review_id not in {None, ""} else None
    if label == "TextUnit":
        text_unit_id = props.get("text_unit_id")
        return f"text_unit:{text_unit_id}" if text_unit_id not in {None, ""} else None
    if label == "Attribute":
        attribute_id = props.get("attribute_id")
        if attribute_id in {None, ""}:
            store_key = props.get("store_key")
            attr_type = props.get("type")
            if store_key in {None, ""} or attr_type in {None, ""}:
                return None
            attribute_id = f"{store_key}:{attr_type}"
        return f"attribute:{attribute_id}"
    if label == "Area":
        area_id = props.get("area_id") or f"{props.get('city', '')}:{props.get('name', '')}"
        return f"area:{_slugify_id(area_id)}"
    if label == "Cuisine":
        name = props.get("name")
        return f"cuisine:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "Category":
        name = props.get("name")
        return f"category:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "PriceBand":
        name = props.get("name")
        return f"price_band:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "AtmosphereTag":
        name = props.get("name")
        return f"atmosphere:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "MenuItem":
        menu_item_id = props.get("menu_item_id")
        if menu_item_id in {None, ""}:
            return None
        return f"menu_item:{menu_item_id}"
    if label == "MenuCategory":
        name = props.get("name")
        return f"menu_category:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "Community":
        community_id = props.get("community_id")
        return f"community:{community_id}" if community_id not in {None, ""} else None
    if label == "CommunityReport":
        report_id = props.get("report_id")
        return f"community_report:{report_id}" if report_id not in {None, ""} else None
    if label == "DishEntity":
        name = props.get("name")
        return f"dish:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "DishFamily":
        name = props.get("name")
        return f"dish_family:{_slugify_id(name)}" if name not in {None, ""} else None
    if label == "ExtractedEntity":
        entity_key = props.get("entity_key")
        if entity_key not in {None, ""}:
            return f"extracted_entity:{entity_key}"
        entity_type = props.get("type")
        name = props.get("name")
        if entity_type in {None, ""} or name in {None, ""}:
            return None
        return f"extracted_entity:{_slugify_id(entity_type)}:{_slugify_id(name)}"
    if label == "ExtractedRelation":
        relation_key = props.get("relation_key")
        return f"extracted_relation:{relation_key}" if relation_key not in {None, ""} else None
    return None


def _graphrag_node_type(labels: list[str]) -> str:
    label = _primary_graphrag_label(labels)
    if label is None:
        return "Unknown"
    return GRAPHRAG_NODE_TYPES.get(label, label)


def normalize_graph_csvs(
    source_nodes_path: Path | str,
    source_edges_path: Path | str,
    output_nodes_path: Path | str,
    output_edges_path: Path | str,
    exclude_relations: set[str] | None = None,
) -> tuple[Path, Path]:
    """Normalize repo KG CSVs or Neo4j exports to the pipeline input schema."""
    source_nodes_path = Path(source_nodes_path)
    source_edges_path = Path(source_edges_path)
    output_nodes_path = Path(output_nodes_path)
    output_edges_path = Path(output_edges_path)
    exclude_relations = {r.upper() for r in (exclude_relations or set())}

    raw_nodes = _read_csv(source_nodes_path)
    raw_edges = _read_csv(source_edges_path)

    nodes: dict[str, str] = {}
    for row in raw_nodes:
        node_id = _pick(row, NODE_ID_COLUMNS)
        if not node_id:
            continue
        nodes[node_id] = _pick(row, NODE_TYPE_COLUMNS, default="Unknown")

    edges: list[dict[str, str]] = []
    for row in raw_edges:
        src_id = _pick(row, EDGE_SRC_COLUMNS)
        dst_id = _pick(row, EDGE_DST_COLUMNS)
        relation = _pick(row, EDGE_REL_COLUMNS)
        if not src_id or not dst_id or not _relation_allowed(relation, exclude_relations):
            continue
        nodes.setdefault(src_id, "Unknown")
        nodes.setdefault(dst_id, "Unknown")
        edges.append({"src_id": src_id, "relation": relation, "dst_id": dst_id})

    output_nodes_path.parent.mkdir(parents=True, exist_ok=True)
    output_edges_path.parent.mkdir(parents=True, exist_ok=True)

    with output_nodes_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["node_id", "node_type"])
        writer.writeheader()
        for node_id, node_type in nodes.items():
            writer.writerow({"node_id": node_id, "node_type": node_type})

    with output_edges_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["src_id", "relation", "dst_id"])
        writer.writeheader()
        writer.writerows(edges)

    return output_nodes_path, output_edges_path


def export_from_neo4j(
    uri: str,
    user: str,
    password: str,
    output_nodes_path: Path | str,
    output_edges_path: Path | str,
) -> tuple[Path, Path]:
    """Export the graph imported by ``load_kg_to_neo4j.py`` into edge-list CSVs."""
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise ImportError("Install neo4j first: pip install neo4j") from exc

    output_nodes_path = Path(output_nodes_path)
    output_edges_path = Path(output_edges_path)
    output_nodes_path.parent.mkdir(parents=True, exist_ok=True)
    output_edges_path.parent.mkdir(parents=True, exist_ok=True)

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            node_rows = session.run(
                """
                MATCH (n)
                WHERE n.node_id IS NOT NULL
                RETURN n.node_id AS node_id,
                       coalesce(
                         n.node_label,
                         head([label IN labels(n) WHERE label <> 'Entity']),
                         head(labels(n)),
                         'Unknown'
                       ) AS node_type
                ORDER BY node_id
                """
            )
            edge_rows = session.run(
                """
                MATCH (s)-[r]->(t)
                WHERE s.node_id IS NOT NULL AND t.node_id IS NOT NULL
                RETURN s.node_id AS src_id, type(r) AS relation, t.node_id AS dst_id
                """
            )

            with output_nodes_path.open("w", encoding="utf-8-sig", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=["node_id", "node_type"])
                writer.writeheader()
                writer.writerows(dict(row) for row in node_rows)

            with output_edges_path.open("w", encoding="utf-8-sig", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=["src_id", "relation", "dst_id"])
                writer.writeheader()
                writer.writerows(dict(row) for row in edge_rows)
    finally:
        driver.close()

    return output_nodes_path, output_edges_path


def export_graphrag_from_neo4j(
    uri: str,
    user: str,
    password: str,
    output_nodes_path: Path | str,
    output_edges_path: Path | str,
    exclude_relations: set[str] | None = None,
) -> tuple[Path, Path]:
    """Export the GraphRAG Neo4j schema from the notebook into R-GCN CSVs.

    The notebook does not store a universal ``node_id`` property. It identifies
    nodes by label-specific keys such as ``Restaurant.store_key`` and
    ``TextUnit.text_unit_id``. This exporter normalizes those GraphRAG keys into
    the same ``store:*``/``category:*`` style IDs used by phase 2.
    """
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise ImportError("Install neo4j first: pip install neo4j") from exc

    output_nodes_path = Path(output_nodes_path)
    output_edges_path = Path(output_edges_path)
    output_nodes_path.parent.mkdir(parents=True, exist_ok=True)
    output_edges_path.parent.mkdir(parents=True, exist_ok=True)
    excluded = {r.upper() for r in (exclude_relations or set())}
    supported_labels = list(GRAPHRAG_LABEL_PRIORITY)

    nodes: dict[str, str] = {}
    element_to_node_id: dict[str, str] = {}
    edges: set[tuple[str, str, str]] = set()

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            node_rows = session.run(
                """
                MATCH (n)
                WHERE any(label IN labels(n) WHERE label IN $labels)
                RETURN elementId(n) AS element_id,
                       labels(n) AS labels,
                       properties(n) AS props
                """,
                {"labels": supported_labels},
            )
            for row in node_rows:
                labels = list(row["labels"] or [])
                props = dict(row["props"] or {})
                node_id = _graphrag_node_id(labels, props)
                if not node_id:
                    continue
                element_to_node_id[str(row["element_id"])] = node_id
                nodes.setdefault(node_id, _graphrag_node_type(labels))

            edge_rows = session.run(
                """
                MATCH (s)-[rel]->(t)
                WHERE any(label IN labels(s) WHERE label IN $labels)
                  AND any(label IN labels(t) WHERE label IN $labels)
                RETURN elementId(s) AS src_element_id,
                       elementId(t) AS dst_element_id,
                       type(rel) AS relation
                """,
                {"labels": supported_labels},
            )
            for row in edge_rows:
                relation = str(row["relation"] or "").strip()
                src_id = element_to_node_id.get(str(row["src_element_id"]))
                dst_id = element_to_node_id.get(str(row["dst_element_id"]))
                if not src_id or not dst_id or not _relation_allowed(relation, excluded):
                    continue
                edges.add((src_id, relation, dst_id))
    finally:
        driver.close()

    if not nodes:
        raise RuntimeError(
            "No GraphRAG nodes found in Neo4j. Run the GraphRAG notebook indexing cells first."
        )
    if not edges:
        raise RuntimeError(
            "No GraphRAG edges found in Neo4j. Run the GraphRAG notebook graph upsert cells first."
        )

    with output_nodes_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["node_id", "node_type"])
        writer.writeheader()
        for node_id, node_type in sorted(nodes.items(), key=lambda item: (item[1], item[0])):
            writer.writerow({"node_id": node_id, "node_type": node_type})

    with output_edges_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["src_id", "relation", "dst_id"])
        writer.writeheader()
        for src_id, relation, dst_id in sorted(edges):
            writer.writerow({"src_id": src_id, "relation": relation, "dst_id": dst_id})

    return output_nodes_path, output_edges_path


def summarize_graph_csvs(
    nodes_path: Path | str,
    edges_path: Path | str,
) -> dict[str, Any]:
    """Return lightweight counts for a prepared R-GCN CSV graph."""
    node_rows = _read_csv(Path(nodes_path))
    edge_rows = _read_csv(Path(edges_path))

    node_type_counts: dict[str, int] = {}
    community_node_ids: set[str] = set()
    community_report_node_ids: set[str] = set()
    extracted_entity_node_ids: set[str] = set()
    extracted_relation_node_ids: set[str] = set()

    for row in node_rows:
        node_id = _pick(row, NODE_ID_COLUMNS)
        node_type = _pick(row, NODE_TYPE_COLUMNS, default="Unknown")
        node_type_counts[node_type] = node_type_counts.get(node_type, 0) + 1
        if node_type == "Community" or node_id.startswith("community:"):
            community_node_ids.add(node_id)
        if node_type == "CommunityReport" or node_id.startswith("community_report:"):
            community_report_node_ids.add(node_id)
        if node_type == "ExtractedEntity" or node_id.startswith("extracted_entity:"):
            extracted_entity_node_ids.add(node_id)
        if node_type == "ExtractedRelation" or node_id.startswith("extracted_relation:"):
            extracted_relation_node_ids.add(node_id)

    relation_counts: dict[str, int] = {}
    restaurants_with_community: set[str] = set()
    for row in edge_rows:
        src_id = _pick(row, EDGE_SRC_COLUMNS)
        relation = _pick(row, EDGE_REL_COLUMNS)
        relation_counts[relation] = relation_counts.get(relation, 0) + 1
        if relation == "IN_COMMUNITY" and src_id:
            restaurants_with_community.add(src_id)

    return {
        "nodes": len(node_rows),
        "edges": len(edge_rows),
        "node_type_counts": node_type_counts,
        "relation_counts": relation_counts,
        "communities": len(community_node_ids),
        "community_reports": len(community_report_node_ids),
        "extracted_entities": len(extracted_entity_node_ids),
        "extracted_relations": len(extracted_relation_node_ids),
        "in_community_edges": relation_counts.get("IN_COMMUNITY", 0),
        "has_report_edges": relation_counts.get("HAS_REPORT", 0),
        "restaurants_with_community": len(restaurants_with_community),
    }


def _edge_index_and_type_from_triples(
    triples: torch.Tensor,
    *,
    add_reverse_edges: bool,
    num_rels: int,
) -> tuple[torch.Tensor, torch.Tensor]:
    if triples.numel() == 0:
        return torch.empty((2, 0), dtype=torch.long), torch.empty((0,), dtype=torch.long)

    edge_index = triples[:, [0, 2]].t().contiguous()
    edge_type = triples[:, 1].contiguous()
    if not add_reverse_edges:
        return edge_index, edge_type

    reverse_index = triples[:, [2, 0]].t().contiguous()
    reverse_type = edge_type + num_rels
    return torch.cat([edge_index, reverse_index], dim=1), torch.cat([edge_type, reverse_type], dim=0)


def load_graph_data(
    nodes_path: Path | str,
    edges_path: Path | str,
    *,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    seed: int = 42,
    exclude_relations: set[str] | None = None,
    add_reverse_edges: bool = False,
) -> dict[str, Any]:
    """Load CSVs, encode IDs and relations, and return tensors for training."""
    nodes_path = Path(nodes_path)
    edges_path = Path(edges_path)
    exclude_relations = {r.upper() for r in (exclude_relations or set())}

    node_rows = _read_csv(nodes_path)
    edge_rows = _read_csv(edges_path)

    node_id_to_idx: dict[str, int] = {}
    idx_to_node_id: list[str] = []
    node_types: list[str] = []

    for row in node_rows:
        node_id = _pick(row, NODE_ID_COLUMNS)
        if not node_id or node_id in node_id_to_idx:
            continue
        node_id_to_idx[node_id] = len(idx_to_node_id)
        idx_to_node_id.append(node_id)
        node_types.append(_pick(row, NODE_TYPE_COLUMNS, default="Unknown"))

    rel_to_idx: dict[str, int] = {}
    idx_to_rel: list[str] = []
    triples: list[tuple[int, int, int]] = []

    for row in edge_rows:
        src_id = _pick(row, EDGE_SRC_COLUMNS)
        dst_id = _pick(row, EDGE_DST_COLUMNS)
        relation = _pick(row, EDGE_REL_COLUMNS)
        if not src_id or not dst_id or not _relation_allowed(relation, exclude_relations):
            continue

        for node_id in (src_id, dst_id):
            if node_id not in node_id_to_idx:
                node_id_to_idx[node_id] = len(idx_to_node_id)
                idx_to_node_id.append(node_id)
                node_types.append("Unknown")

        if relation not in rel_to_idx:
            rel_to_idx[relation] = len(idx_to_rel)
            idx_to_rel.append(relation)

        triples.append((node_id_to_idx[src_id], rel_to_idx[relation], node_id_to_idx[dst_id]))

    if not triples:
        raise ValueError(f"No usable edges found in {edges_path}")

    all_edges = torch.tensor(triples, dtype=torch.long)
    generator = torch.Generator().manual_seed(seed)
    perm = torch.randperm(all_edges.size(0), generator=generator)
    shuffled = all_edges[perm]

    train_count = int(shuffled.size(0) * train_ratio)
    val_count = int(shuffled.size(0) * val_ratio)
    train_edges = shuffled[:train_count]
    val_edges = shuffled[train_count : train_count + val_count]
    test_edges = shuffled[train_count + val_count :]

    num_rels = len(idx_to_rel)
    edge_index, edge_type = _edge_index_and_type_from_triples(
        all_edges,
        add_reverse_edges=False,
        num_rels=num_rels,
    )
    message_edge_index, message_edge_type = _edge_index_and_type_from_triples(
        all_edges,
        add_reverse_edges=add_reverse_edges,
        num_rels=num_rels,
    )
    train_edge_index, train_edge_type = _edge_index_and_type_from_triples(
        train_edges,
        add_reverse_edges=add_reverse_edges,
        num_rels=num_rels,
    )

    return {
        "num_nodes": len(idx_to_node_id),
        "num_rels": num_rels,
        "num_message_rels": num_rels * (2 if add_reverse_edges else 1),
        "node_id_to_idx": node_id_to_idx,
        "idx_to_node_id": idx_to_node_id,
        "node_types": node_types,
        "rel_to_idx": rel_to_idx,
        "idx_to_rel": idx_to_rel,
        "edge_index": edge_index,
        "edge_type": edge_type,
        "message_edge_index": message_edge_index,
        "message_edge_type": message_edge_type,
        "train_edge_index": train_edge_index,
        "train_edge_type": train_edge_type,
        "all_positive_edges": all_edges,
        "train_edges": train_edges,
        "val_edges": val_edges,
        "test_edges": test_edges,
    }
