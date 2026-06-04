from __future__ import annotations

import ast
import json
import math
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd


GRAPH_LABELS = [
    "Restaurant",
    "Review",
    "TextUnit",
    "MenuItem",
    "MenuCategory",
    "Category",
    "Cuisine",
    "PriceBand",
    "Area",
    "Attribute",
    "DishFamily",
    "DishEntity",
    "ExtractedEntity",
    "ExtractedRelation",
    "Community",
    "CommunityReport",
]

NODE_TYPE_LABELS = {
    "Store": "Restaurant",
    "Restaurant": "Restaurant",
    "Review": "Review",
    "TextUnit": "TextUnit",
    "MenuItem": "MenuItem",
    "MenuCategory": "MenuCategory",
    "Category": "Category",
    "Cuisine": "Cuisine",
    "PriceBand": "PriceBand",
    "Area": "Area",
    "Attribute": "Attribute",
    "DishFamily": "DishFamily",
    "DishEntity": "DishEntity",
    "ExtractedEntity": "ExtractedEntity",
    "ExtractedRelation": "ExtractedRelation",
    "Community": "Community",
    "CommunityReport": "CommunityReport",
}

NODE_ID_PREFIX_LABELS = {
    "area": "Area",
    "attribute": "Attribute",
    "category": "Category",
    "community": "Community",
    "community_report": "CommunityReport",
    "cuisine": "Cuisine",
    "dish": "DishEntity",
    "menu_category": "MenuCategory",
    "menu_item": "MenuItem",
    "price_band": "PriceBand",
    "review": "Review",
    "store": "Restaurant",
    "restaurant": "Restaurant",
    "text_unit": "TextUnit",
}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _clean(value: Any) -> Any:
    if _is_missing(value):
        return None
    if hasattr(value, "item"):
        try:
            return _clean(value.item())
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_clean(item) for item in value]
    return value


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    return [_clean(row) for row in df.to_dict("records")]


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _parse_list(value: Any) -> list[str]:
    if _is_missing(value) or value == "":
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
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


def _safe_rel_type(value: str) -> str:
    relation = re.sub(r"[^A-Za-z0-9_]", "_", str(value or "").upper()).strip("_")
    if not relation or relation[0].isdigit():
        relation = f"REL_{relation}"
    return relation


def _graph_id_from_node(node_id: str, node_type: str) -> str:
    node_id = str(node_id)
    node_type = str(node_type)
    if node_type in {"Store", "Restaurant"} and node_id.startswith("restaurant:"):
        return "store:" + node_id.split(":", 1)[1]
    return node_id


def _label_from_graph_id(graph_id: str) -> str | None:
    prefix = str(graph_id).split(":", 1)[0]
    return NODE_ID_PREFIX_LABELS.get(prefix)


def _community_reports_from_progress(cache_root: Path) -> pd.DataFrame:
    path = cache_root / "community_report_progress.jsonl"
    if not path.exists():
        return pd.DataFrame()
    by_report_id: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            community_id = str(record.get("community_id", "")).strip()
            if not community_id:
                continue
            report_id = str(record.get("report_id") or f"community_report_{community_id}")
            target = by_report_id.setdefault(
                report_id,
                {
                    "report_id": report_id,
                    "community_id": community_id,
                    "graph_id": f"community_report:{report_id}",
                },
            )
            for key in ("title", "summary", "cache_key", "model", "prompt_version", "saved_at"):
                if record.get(key):
                    target[key] = record[key]
    return pd.DataFrame(by_report_id.values())


class Neo4jWriter:
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database

    def _connect(self):
        from neo4j import GraphDatabase

        driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        driver.verify_connectivity()
        return driver

    @staticmethod
    def _run(session, query: str, params: dict[str, Any] | None = None) -> None:
        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                session.run(query, params or {}).consume()
                return
            except Exception as exc:
                last_exc = exc
                if "DeadlockDetected" not in str(exc) or attempt == 3:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if last_exc:
            raise last_exc

    def _create_schema(self, session) -> None:
        statements = [
            "CREATE CONSTRAINT restaurant_key IF NOT EXISTS FOR (r:Restaurant) REQUIRE r.store_key IS UNIQUE",
            "CREATE CONSTRAINT review_key IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE",
            "CREATE CONSTRAINT text_unit_key IF NOT EXISTS FOR (t:TextUnit) REQUIRE t.text_unit_id IS UNIQUE",
            "CREATE CONSTRAINT menu_item_key IF NOT EXISTS FOR (m:MenuItem) REQUIRE m.menu_item_id IS UNIQUE",
            "CREATE CONSTRAINT extracted_entity_key IF NOT EXISTS FOR (e:ExtractedEntity) REQUIRE e.entity_key IS UNIQUE",
            "CREATE CONSTRAINT extracted_relation_key IF NOT EXISTS FOR (r:ExtractedRelation) REQUIRE r.relation_key IS UNIQUE",
            "CREATE CONSTRAINT community_key IF NOT EXISTS FOR (c:Community) REQUIRE c.community_id IS UNIQUE",
            "CREATE CONSTRAINT community_report_key IF NOT EXISTS FOR (cr:CommunityReport) REQUIRE cr.report_id IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT cuisine_name IF NOT EXISTS FOR (c:Cuisine) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT priceband_name IF NOT EXISTS FOR (p:PriceBand) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT menu_category_name IF NOT EXISTS FOR (m:MenuCategory) REQUIRE m.name IS UNIQUE",
            "CREATE CONSTRAINT dish_family_name IF NOT EXISTS FOR (d:DishFamily) REQUIRE d.name IS UNIQUE",
            "CREATE INDEX restaurant_graph_id IF NOT EXISTS FOR (n:Restaurant) ON (n.graph_id)",
            "CREATE INDEX text_unit_graph_id IF NOT EXISTS FOR (n:TextUnit) ON (n.graph_id)",
            "CREATE INDEX menu_item_graph_id IF NOT EXISTS FOR (n:MenuItem) ON (n.graph_id)",
            "CREATE INDEX review_graph_id IF NOT EXISTS FOR (n:Review) ON (n.graph_id)",
            "CREATE INDEX category_graph_id IF NOT EXISTS FOR (n:Category) ON (n.graph_id)",
            "CREATE INDEX menu_category_graph_id IF NOT EXISTS FOR (n:MenuCategory) ON (n.graph_id)",
            "CREATE INDEX cuisine_graph_id IF NOT EXISTS FOR (n:Cuisine) ON (n.graph_id)",
            "CREATE INDEX priceband_graph_id IF NOT EXISTS FOR (n:PriceBand) ON (n.graph_id)",
            "CREATE INDEX area_graph_id IF NOT EXISTS FOR (n:Area) ON (n.graph_id)",
            "CREATE INDEX attribute_graph_id IF NOT EXISTS FOR (n:Attribute) ON (n.graph_id)",
            "CREATE INDEX dish_entity_graph_id IF NOT EXISTS FOR (n:DishEntity) ON (n.graph_id)",
            "CREATE INDEX community_graph_id IF NOT EXISTS FOR (n:Community) ON (n.graph_id)",
            "CREATE INDEX community_report_graph_id IF NOT EXISTS FOR (n:CommunityReport) ON (n.graph_id)",
            "CREATE INDEX extracted_entity_graph_id IF NOT EXISTS FOR (n:ExtractedEntity) ON (n.graph_id)",
            "CREATE INDEX rest_rating IF NOT EXISTS FOR (r:Restaurant) ON (r.rating)",
            "CREATE INDEX rest_location IF NOT EXISTS FOR (r:Restaurant) ON (r.lat, r.lng)",
            "CREATE INDEX text_unit_store IF NOT EXISTS FOR (t:TextUnit) ON (t.store_key)",
            "CREATE INDEX menu_item_price IF NOT EXISTS FOR (m:MenuItem) ON (m.price)",
            "CREATE INDEX attr_type IF NOT EXISTS FOR (a:Attribute) ON (a.type)",
            "CREATE INDEX community_level IF NOT EXISTS FOR (c:Community) ON (c.level)",
        ]
        for statement in statements:
            self._run(session, statement)

    def _clear_graph(self, session) -> None:
        self._run(session, "MATCH (n:ProdNode) DETACH DELETE n")
        self._run(
            session,
            """
            MATCH (n)
            WHERE any(label IN labels(n) WHERE label IN $labels)
            DETACH DELETE n
            """,
            {"labels": GRAPH_LABELS},
        )

    def _upsert_restaurants(self, session, restaurants: pd.DataFrame) -> int:
        if restaurants.empty:
            return 0
        rows = []
        for _, row in restaurants.iterrows():
            store_key = str(row["restaurant_id"])
            categories = _parse_list(row.get("categories"))
            rows.append(
                {
                    "store_key": store_key,
                    "graph_id": f"store:{store_key}",
                    "name": row.get("name"),
                    "address": row.get("address"),
                    "lat": row.get("latitude"),
                    "lng": row.get("longitude"),
                    "rating": row.get("rating"),
                    "review_count": row.get("review_count"),
                    "price_min": row.get("price_min"),
                    "price_max": row.get("price_max"),
                    "price_band": self._price_band(row.get("price_min"), row.get("price_max")),
                    "categories": categories,
                    "cuisines": [row.get("cuisine_type")] if row.get("cuisine_type") else [],
                    "opening_hours": row.get("opening_hours"),
                    "data_version": row.get("data_version"),
                }
            )
        self._run(
            session,
            """
            UNWIND $rows AS row
            MERGE (r:Restaurant {store_key: row.store_key})
            SET r += row,
                r.updated_at = datetime()
            WITH r, row
            FOREACH (cat IN coalesce(row.categories, []) |
                MERGE (c:Category {name: cat})
                SET c.graph_id = 'category:' + cat
                MERGE (r)-[:HAS_CATEGORY]->(c))
            FOREACH (cui IN coalesce(row.cuisines, []) |
                MERGE (c:Cuisine {name: cui})
                SET c.graph_id = 'cuisine:' + cui
                MERGE (r)-[:HAS_CUISINE]->(c))
            FOREACH (_ IN CASE WHEN row.price_band IS NULL THEN [] ELSE [1] END |
                MERGE (p:PriceBand {name: row.price_band})
                SET p.graph_id = 'price_band:' + row.price_band
                MERGE (r)-[:HAS_PRICE_BAND]->(p))
            """,
            {"rows": _clean(rows)},
        )
        return len(rows)

    @staticmethod
    def _price_band(price_min: Any, price_max: Any) -> str | None:
        prices = []
        for value in (price_min, price_max):
            try:
                if not _is_missing(value) and str(value).strip() != "":
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

    def _upsert_menu_items(self, session, menu_items: pd.DataFrame) -> int:
        if menu_items.empty:
            return 0
        rows = []
        for _, row in menu_items.iterrows():
            menu_item_id = str(row.get("menu_item_id") or row.get("item_id") or "").strip()
            store_key = str(row.get("store_key") or row.get("restaurant_id") or "").strip()
            if not menu_item_id or not store_key:
                continue
            rows.append(
                {
                    "menu_item_id": menu_item_id,
                    "graph_id": f"menu_item:{menu_item_id}",
                    "store_key": store_key,
                    "name": row.get("item_name", row.get("name")),
                    "details": row.get("item_details", row.get("description")),
                    "price": row.get("price"),
                    "old_price": row.get("old_price"),
                    "order_count": row.get("order_count"),
                    "like_count": row.get("like_count"),
                    "dislike_count": row.get("dislike_count"),
                    "item_image": row.get("item_image"),
                    "category_name": row.get("category_name", row.get("category")),
                    "dish_family": row.get("dish_family"),
                }
            )
        self._run(
            session,
            """
            UNWIND $rows AS row
            MATCH (r:Restaurant {store_key: row.store_key})
            MERGE (mi:MenuItem {menu_item_id: row.menu_item_id})
            SET mi += row,
                mi.updated_at = datetime()
            MERGE (r)-[:HAS_MENU_ITEM]->(mi)
            FOREACH (_ IN CASE WHEN row.category_name IS NULL OR row.category_name = '' THEN [] ELSE [1] END |
                MERGE (mc:MenuCategory {name: row.category_name})
                SET mc.graph_id = 'menu_category:' + row.category_name
                MERGE (r)-[:HAS_MENU_CATEGORY]->(mc)
                MERGE (mi)-[:IN_MENU_CATEGORY]->(mc))
            FOREACH (_ IN CASE WHEN row.dish_family IS NULL OR row.dish_family = '' THEN [] ELSE [1] END |
                MERGE (d:DishFamily {name: row.dish_family})
                SET d.graph_id = 'dish:' + row.dish_family
                MERGE (r)-[:SERVES]->(d)
                MERGE (mi)-[:BELONGS_TO_FAMILY]->(d))
            """,
            {"rows": _clean(rows)},
        )
        return len(rows)

    def _upsert_text_units(self, session, text_units: pd.DataFrame) -> int:
        if text_units.empty:
            return 0
        rows = []
        for _, row in text_units.iterrows():
            rows.append(
                {
                    "review_id": str(row.get("review_id")),
                    "text_unit_id": str(row.get("text_unit_id")),
                    "graph_id": f"text_unit:{row.get('text_unit_id')}",
                    "store_key": str(row.get("store_key")),
                    "feedback": row.get("feedback"),
                    "text": row.get("chunk_text"),
                    "rating": row.get("rating"),
                    "rated_at": str(row.get("rated_at", "")),
                    "sentiment": row.get("sentiment"),
                    "aspect_scores": json.dumps(row.get("aspect_scores", {}), ensure_ascii=False),
                    "source": row.get("source"),
                }
            )
        self._run(
            session,
            """
            UNWIND $rows AS row
            MATCH (rest:Restaurant {store_key: row.store_key})
            MERGE (rv:Review {review_id: row.review_id})
            SET rv.feedback = row.feedback,
                rv.rating = row.rating,
                rv.rated_at = row.rated_at,
                rv.sentiment = row.sentiment,
                rv.aspect_scores = row.aspect_scores,
                rv.source = row.source,
                rv.graph_id = 'review:' + row.review_id
            MERGE (tu:TextUnit {text_unit_id: row.text_unit_id})
            SET tu.text = row.text,
                tu.store_key = row.store_key,
                tu.source = row.source,
                tu.review_id = row.review_id,
                tu.sentiment = row.sentiment,
                tu.rating = row.rating,
                tu.graph_id = row.graph_id,
                tu.updated_at = datetime()
            MERGE (rv)-[:HAS_TEXT_UNIT]->(tu)
            MERGE (tu)-[:ABOUT]->(rest)
            """,
            {"rows": _clean(rows)},
        )
        return len(rows)

    def _upsert_extracted_graph(self, session, entities: pd.DataFrame, relations: pd.DataFrame) -> tuple[int, int]:
        entity_count = 0
        relation_count = 0
        if not entities.empty:
            rows = []
            for _, row in entities.iterrows():
                entity_key = str(row.get("entity_key", "")).strip()
                if not entity_key:
                    continue
                rows.append(
                    {
                        "entity_key": entity_key,
                        "graph_id": entity_key,
                        "name": row.get("name"),
                        "type": row.get("entity_type"),
                        "store_key": str(row.get("store_key", "")),
                        "review_id": str(row.get("review_id", "")),
                        "text_unit_id": str(row.get("text_unit_id", "")),
                        "sentiment": row.get("sentiment"),
                        "confidence": row.get("confidence"),
                        "evidence": row.get("evidence"),
                    }
                )
            self._run(
                session,
                """
                UNWIND $rows AS row
                MERGE (e:ExtractedEntity {entity_key: row.entity_key})
                SET e.name = row.name,
                    e.type = row.type,
                    e.graph_id = row.graph_id,
                    e.updated_at = datetime()
                WITH e, row
                OPTIONAL MATCH (r:Restaurant {store_key: row.store_key})
                OPTIONAL MATCH (tu:TextUnit {text_unit_id: row.text_unit_id})
                FOREACH (_ IN CASE WHEN tu IS NULL THEN [] ELSE [1] END |
                    MERGE (tu)-[m:MENTIONS_ENTITY]->(e)
                    SET m.sentiment = row.sentiment,
                        m.confidence = row.confidence,
                        m.evidence = row.evidence,
                        m.review_id = row.review_id,
                        m.updated_at = datetime())
                FOREACH (_ IN CASE WHEN r IS NULL THEN [] ELSE [1] END |
                    MERGE (r)-[re:HAS_EXTRACTED_ENTITY]->(e)
                    SET re.last_seen_at = datetime())
                """,
                {"rows": _clean(rows)},
            )
            entity_count = len(rows)

        if not relations.empty:
            rows = []
            for _, row in relations.iterrows():
                relation_key = str(row.get("relation_key", "")).strip()
                if not relation_key:
                    continue
                rows.append(
                    {
                        "relation_key": relation_key,
                        "graph_id": f"relation:{relation_key}",
                        "source_entity_key": str(row.get("source_entity_key", "")),
                        "target_entity_key": str(row.get("target_entity_key", "")),
                        "type": row.get("relation_type"),
                        "store_key": str(row.get("store_key", "")),
                        "review_id": str(row.get("review_id", "")),
                        "text_unit_id": str(row.get("text_unit_id", "")),
                        "sentiment": row.get("sentiment"),
                        "confidence": row.get("confidence"),
                        "evidence": row.get("evidence"),
                    }
                )
            self._run(
                session,
                """
                UNWIND $rows AS row
                MATCH (source:ExtractedEntity {entity_key: row.source_entity_key})
                MATCH (target:ExtractedEntity {entity_key: row.target_entity_key})
                MERGE (rel:ExtractedRelation {relation_key: row.relation_key})
                SET rel += row,
                    rel.updated_at = datetime()
                MERGE (source)-[:SOURCE_OF]->(rel)
                MERGE (rel)-[:TARGETS]->(target)
                WITH rel, row
                OPTIONAL MATCH (tu:TextUnit {text_unit_id: row.text_unit_id})
                FOREACH (_ IN CASE WHEN tu IS NULL THEN [] ELSE [1] END |
                    MERGE (tu)-[:SUPPORTS_RELATION]->(rel))
                """,
                {"rows": _clean(rows)},
            )
            relation_count = len(rows)
        return entity_count, relation_count

    def _upsert_exported_nodes(self, session, nodes_path: Path | None) -> int:
        if not nodes_path or not nodes_path.exists():
            return 0
        nodes = _read_optional_csv(nodes_path)
        if nodes.empty:
            return 0
        count = 0
        for node_type, group in nodes.groupby("node_type"):
            label = NODE_TYPE_LABELS.get(str(node_type))
            if not label:
                continue
            rows = []
            for _, row in group.iterrows():
                node_id = str(row.get("node_id", "")).strip()
                if not node_id:
                    continue
                graph_id = _graph_id_from_node(node_id, str(node_type))
                props = {
                    "graph_id": graph_id,
                    "source_node_id": node_id,
                    "node_type": str(node_type),
                    "name": node_id.split(":", 1)[-1],
                }
                if label == "Restaurant":
                    props["store_key"] = graph_id.split(":", 1)[-1]
                elif label == "Community":
                    props["community_id"] = node_id.split(":", 1)[-1]
                elif label == "CommunityReport":
                    props["report_id"] = node_id.split(":", 1)[-1]
                elif label == "MenuItem":
                    props["menu_item_id"] = node_id.split(":", 1)[-1]
                elif label == "TextUnit":
                    props["text_unit_id"] = node_id.split(":", 1)[-1]
                elif label == "Review":
                    props["review_id"] = node_id.split(":", 1)[-1]
                elif label == "ExtractedEntity":
                    props["entity_key"] = node_id
                elif label == "ExtractedRelation":
                    props["relation_key"] = node_id.split(":", 1)[-1]
                rows.append(props)
            if not rows:
                continue
            key = self._merge_key_for_label(label)
            self._run(
                session,
                f"""
                UNWIND $rows AS row
                MERGE (n:{label} {{{key}: row.{key}}})
                SET n += row
                """,
                {"rows": _clean(rows)},
            )
            count += len(rows)
        return count

    @staticmethod
    def _merge_key_for_label(label: str) -> str:
        return {
            "Restaurant": "store_key",
            "Review": "review_id",
            "TextUnit": "text_unit_id",
            "MenuItem": "menu_item_id",
            "Community": "community_id",
            "CommunityReport": "report_id",
            "ExtractedEntity": "entity_key",
            "ExtractedRelation": "relation_key",
            "Category": "name",
            "Cuisine": "name",
            "PriceBand": "name",
            "MenuCategory": "name",
            "DishFamily": "name",
            "DishEntity": "name",
            "Area": "name",
            "Attribute": "name",
        }[label]

    def _upsert_community_reports(self, session, cache_root: Path) -> int:
        reports = _community_reports_from_progress(cache_root)
        if reports.empty:
            return 0
        self._run(
            session,
            """
            UNWIND $rows AS row
            MERGE (c:Community {community_id: row.community_id})
            SET c.graph_id = 'community:' + row.community_id
            MERGE (cr:CommunityReport {report_id: row.report_id})
            SET cr += row,
                cr.updated_at = datetime()
            MERGE (c)-[:HAS_REPORT]->(cr)
            """,
            {"rows": _records(reports)},
        )
        return len(reports)

    def _upsert_exported_edges(self, session, edges_path: Path | None) -> int:
        if not edges_path or not edges_path.exists():
            return 0
        edges = _read_optional_csv(edges_path)
        if edges.empty:
            return 0
        total = 0
        grouped: dict[tuple[str, str, str], list[dict[str, str]]] = {}
        for _, row in edges.iterrows():
            src_id = _graph_id_from_node(str(row.get("src_id", "")).strip(), "")
            dst_id = _graph_id_from_node(str(row.get("dst_id", "")).strip(), "")
            if not src_id or not dst_id:
                continue
            src_label = _label_from_graph_id(src_id)
            dst_label = _label_from_graph_id(dst_id)
            if not src_label or not dst_label:
                continue
            key = (_safe_rel_type(str(row.get("relation", ""))), src_label, dst_label)
            grouped.setdefault(key, []).append({"src_id": src_id, "dst_id": dst_id})

        for (rel_type, src_label, dst_label), rows in grouped.items():
            if not rows:
                continue
            self._run(
                session,
                f"""
                UNWIND $rows AS row
                MATCH (src:{src_label} {{graph_id: row.src_id}})
                MATCH (dst:{dst_label} {{graph_id: row.dst_id}})
                MERGE (src)-[r:{rel_type}]->(dst)
                SET r.source = 'graphrag_export'
                """,
                {"rows": rows},
            )
            total += len(rows)
        return total

    def write_graphrag_snapshot(
        self,
        *,
        graph_version: str,
        processed_dir: Path,
        cache_root: Path,
        exported_nodes_path: Path | None = None,
        exported_edges_path: Path | None = None,
    ) -> dict[str, object]:
        try:
            driver = self._connect()
        except ImportError:
            return {"status": "skipped", "reason": "neo4j package is not installed", "graph_version": graph_version}
        except Exception as exc:
            return {"status": "failed", "reason": f"{type(exc).__name__}: {exc}", "graph_version": graph_version}

        restaurants = _read_optional_csv(processed_dir / "canonical_restaurants.csv")
        menu_items = _read_optional_csv(processed_dir / "menu_items_enriched.csv")
        if menu_items.empty:
            menu_items = _read_optional_csv(processed_dir / "canonical_menu_items.csv")
        text_units = _read_optional_csv(processed_dir / "text_units.csv")
        entities = _read_optional_csv(processed_dir / "extracted_entities.csv")
        relations = _read_optional_csv(processed_dir / "extracted_relations.csv")

        try:
            with driver.session(database=self.database) as session:
                self._create_schema(session)
                self._clear_graph(session)
                counts = {
                    "restaurants": self._upsert_restaurants(session, restaurants),
                    "menu_items": self._upsert_menu_items(session, menu_items),
                    "text_units": self._upsert_text_units(session, text_units),
                }
                entity_count, relation_count = self._upsert_extracted_graph(session, entities, relations)
                counts["extracted_entities"] = entity_count
                counts["extracted_relations"] = relation_count
                counts["exported_nodes"] = self._upsert_exported_nodes(session, exported_nodes_path)
                counts["community_reports"] = self._upsert_community_reports(session, cache_root)
                counts["exported_edges"] = self._upsert_exported_edges(session, exported_edges_path)
                self._run(
                    session,
                    "MATCH (n) WHERE any(label IN labels(n) WHERE label IN $labels) SET n.graph_version = $graph_version",
                    {"labels": GRAPH_LABELS, "graph_version": graph_version},
                )
            return {
                "status": "ok",
                "schema": "graphrag_notebook",
                "graph_version": graph_version,
                "counts": counts,
                "database": self.database,
            }
        except Exception as exc:
            return {
                "status": "failed",
                "schema": "graphrag_notebook",
                "reason": f"{type(exc).__name__}: {exc}",
                "graph_version": graph_version,
            }
        finally:
            driver.close()
