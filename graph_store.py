from __future__ import annotations

import json
from typing import Optional

import pandas as pd

from config import AppConfig


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str):
        from neo4j import GraphDatabase

        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver.verify_connectivity()

    @classmethod
    def from_config(cls, config: AppConfig) -> "Neo4jClient":
        return cls(config.neo4j_uri, config.neo4j_user, config.neo4j_password)

    def close(self) -> None:
        self.driver.close()

    def run(self, query: str, params: Optional[dict] = None) -> list[dict]:
        with self.driver.session() as session:
            return [r.data() for r in session.run(query, params or {})]

    def create_schema(self) -> None:
        stmts = [
            "CREATE CONSTRAINT restaurant_key IF NOT EXISTS FOR (r:Restaurant) REQUIRE r.store_key IS UNIQUE",
            "CREATE CONSTRAINT review_key IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE",
            "CREATE CONSTRAINT text_unit_key IF NOT EXISTS FOR (t:TextUnit) REQUIRE t.text_unit_id IS UNIQUE",
            "CREATE CONSTRAINT attr_id_key IF NOT EXISTS FOR (a:Attribute) REQUIRE a.attribute_id IS UNIQUE",
            "CREATE CONSTRAINT area_id_key IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE",
            "CREATE CONSTRAINT cuisine_name IF NOT EXISTS FOR (c:Cuisine) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT priceband_name IF NOT EXISTS FOR (p:PriceBand) REQUIRE p.name IS UNIQUE",
            "CREATE CONSTRAINT atmos_name IF NOT EXISTS FOR (a:AtmosphereTag) REQUIRE a.name IS UNIQUE",
            "CREATE CONSTRAINT dish_name IF NOT EXISTS FOR (d:DishEntity) REQUIRE d.name IS UNIQUE",
            "CREATE CONSTRAINT menu_item_key IF NOT EXISTS FOR (m:MenuItem) REQUIRE m.menu_item_id IS UNIQUE",
            "CREATE CONSTRAINT menu_category_name IF NOT EXISTS FOR (m:MenuCategory) REQUIRE m.name IS UNIQUE",
            "CREATE CONSTRAINT community_key IF NOT EXISTS FOR (c:Community) REQUIRE c.community_id IS UNIQUE",
            "CREATE CONSTRAINT community_report_key IF NOT EXISTS FOR (cr:CommunityReport) REQUIRE cr.report_id IS UNIQUE",
            "CREATE INDEX rest_rating IF NOT EXISTS FOR (r:Restaurant) ON (r.rating)",
            "CREATE INDEX rest_location IF NOT EXISTS FOR (r:Restaurant) ON (r.lat, r.lng)",
            "CREATE INDEX text_unit_store IF NOT EXISTS FOR (t:TextUnit) ON (t.store_key)",
            "CREATE INDEX menu_item_price IF NOT EXISTS FOR (m:MenuItem) ON (m.price)",
            "CREATE INDEX attr_type IF NOT EXISTS FOR (a:Attribute) ON (a.type)",
            "CREATE INDEX community_level IF NOT EXISTS FOR (c:Community) ON (c.level)",
        ]
        for stmt in stmts:
            self.run(stmt)

    def upsert_restaurants(self, summary: pd.DataFrame) -> None:
        rows = summary.to_dict("records")
        self.run("""
        UNWIND $rows AS row
        MERGE (r:Restaurant {store_key: row.store_key})
        SET r.name = row.name, r.address = row.address, r.district = row.district, r.city = row.city,
            r.lat = row.lat, r.lng = row.lng, r.rating = row.rating,
            r.gmaps_rating = row.gmaps_rating, r.foody_rating = row.foody_rating,
            r.review_count = row.review_count, r.price_min = row.price_min, r.price_max = row.price_max,
            r.menu_item_count = row.menu_item_count, r.menu_price_min = row.menu_price_min,
            r.menu_price_max = row.menu_price_max, r.menu_price_median = row.menu_price_median,
            r.top_menu_items = row.top_menu_items, r.opening_hours = row.opening_hours,
            r.delivery_time = row.delivery_time, r.image_url = row.image_url, r.updated_at = datetime()
        WITH r, row
        FOREACH (cat IN coalesce(row.categories, []) | MERGE (c:Category {name: cat}) MERGE (r)-[:HAS_CATEGORY]->(c))
        FOREACH (cui IN coalesce(row.cuisines, []) | MERGE (c:Cuisine {name: cui}) MERGE (r)-[:HAS_CUISINE]->(c))
        FOREACH (_ IN CASE WHEN row.price_band IS NULL THEN [] ELSE [1] END |
          MERGE (p:PriceBand {name: row.price_band}) MERGE (r)-[:HAS_PRICE_BAND]->(p))
        FOREACH (_ IN CASE WHEN row.district IS NULL THEN [] ELSE [1] END |
          MERGE (a:Area {area_id: row.city + ':' + row.district})
          SET a.name = row.district, a.city = row.city
          MERGE (r)-[:IN_AREA]->(a))
        """, {"rows": rows})

    def upsert_attributes(self, attrs: pd.DataFrame) -> None:
        if attrs.empty:
            return
        rows = []
        for _, r in attrs.iterrows():
            rows.append({
                "attribute_id": f"{r['store_key']}:{r['attribute_type']}",
                "store_key": r["store_key"],
                "attribute_type": r["attribute_type"],
                "attribute_score": r["attribute_score"],
                "sample_count": r["sample_count"],
            })
        self.run("""
        UNWIND $rows AS row
        MATCH (r:Restaurant {store_key: row.store_key})
        MERGE (a:Attribute {attribute_id: row.attribute_id})
        SET a.store_key = row.store_key, a.type = row.attribute_type,
            a.score = row.attribute_score, a.sample_count = row.sample_count,
            a.updated_at = datetime()
        MERGE (r)-[:HAS_ATTRIBUTE]->(a)
        """, {"rows": rows})

    def upsert_text_units(self, text_units: pd.DataFrame) -> None:
        rows = []
        for _, r in text_units.iterrows():
            rows.append({
                "review_id": r["review_id"],
                "text_unit_id": r["text_unit_id"],
                "store_key": r["store_key"],
                "feedback": r["feedback"],
                "chunk_text": r["chunk_text"],
                "rating": r["rating"],
                "rated_at": str(r.get("rated_at")),
                "sentiment": r["sentiment"],
                "aspect_scores": json.dumps(r["aspect_scores"], ensure_ascii=False),
                "source": r["source"],
            })
        self.run("""
        UNWIND $rows AS row
        MATCH (rest:Restaurant {store_key: row.store_key})
        MERGE (rv:Review {review_id: row.review_id})
        SET rv.feedback = row.feedback, rv.rating = row.rating, rv.rated_at = row.rated_at,
            rv.sentiment = row.sentiment, rv.aspect_scores = row.aspect_scores, rv.source = row.source
        MERGE (tu:TextUnit {text_unit_id: row.text_unit_id})
        SET tu.text = row.chunk_text, tu.store_key = row.store_key, tu.source = row.source,
            tu.review_id = row.review_id, tu.sentiment = row.sentiment, tu.rating = row.rating,
            tu.updated_at = datetime()
        MERGE (rv)-[:HAS_TEXT_UNIT]->(tu)
        MERGE (tu)-[:ABOUT]->(rest)
        """, {"rows": rows})

    def upsert_menu_items(self, menu_items: pd.DataFrame) -> None:
        if menu_items.empty:
            return
        self.run("""
        UNWIND $rows AS row
        MATCH (r:Restaurant {store_key: row.store_key})
        MERGE (mi:MenuItem {menu_item_id: row.menu_item_id})
        SET mi.name = row.item_name, mi.details = row.item_details, mi.price = row.price,
            mi.old_price = row.old_price, mi.order_count = row.order_count,
            mi.like_count = row.like_count, mi.dislike_count = row.dislike_count,
            mi.item_image = row.item_image, mi.updated_at = datetime()
        MERGE (r)-[:HAS_MENU_ITEM]->(mi)
        FOREACH (_ IN CASE WHEN row.category_name IS NULL OR row.category_name = '' THEN [] ELSE [1] END |
          MERGE (mc:MenuCategory {name: row.category_name})
          MERGE (r)-[:HAS_MENU_CATEGORY]->(mc)
          MERGE (mi)-[:IN_MENU_CATEGORY]->(mc))
        """, {"rows": menu_items.to_dict("records")})

    def upsert_dish_entities(self, dish_entities: pd.DataFrame) -> None:
        if dish_entities.empty:
            return
        rows = dish_entities.rename(columns={"dish_name": "name"}).to_dict("records")
        self.run("""
        UNWIND $rows AS row
        MATCH (r:Restaurant {store_key: row.store_key})
        MERGE (d:DishEntity {name: row.name})
        MERGE (r)-[s:SERVES]->(d)
        SET s.menu_item_count = row.total_mentions,
            s.like_count = row.like_count,
            s.order_count = row.order_count,
            s.avg_price = row.avg_price,
            s.menu_item_ids = row.menu_item_ids,
            s.updated_at = datetime()
        """, {"rows": rows})

