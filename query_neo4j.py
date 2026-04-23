from __future__ import annotations

import argparse
import json
import logging
import os
from typing import Any

from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _json_loads_maybe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value
    value = value.strip()
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def run_cypher(uri: str, user: str, password: str, cypher: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            res = session.run(cypher, **(params or {}))
            return [r.data() for r in res]
    finally:
        driver.close()


def recommend_stores_by_text(query: str, limit: int = 10) -> tuple[str, dict[str, Any]]:
    """Simple entity search using KG text fields.

    Since this KG is imported as :Entity plus typed labels (:Store, :Category, ...),
    we can search `n.name` and enrich with connected categories/context/service.
    """

    cypher = """
    MATCH (s:Store)
    WHERE toLower(s.name) CONTAINS toLower($q)
       OR toLower(coalesce(s.properties_json,'')) CONTAINS toLower($q)
    OPTIONAL MATCH (s)-[:HAS_PRIMARY_CATEGORY|:HAS_CATEGORY]->(c:Category)
    OPTIONAL MATCH (s)-[:MATCHES_CONTEXT]->(ct:ContextTag)
    OPTIONAL MATCH (s)-[:OFFERS_SERVICE]->(so:ServiceOption)
    WITH s,
         collect(DISTINCT c.name)[0..5] AS categories,
         collect(DISTINCT ct.name)[0..8] AS context_tags,
         collect(DISTINCT so.name)[0..8] AS services
    RETURN s.node_id AS store_id,
           s.name AS store_name,
           categories,
           context_tags,
           services,
           s.properties_json AS properties_json
    ORDER BY store_name
    LIMIT $limit
    """

    return cypher, {"q": query, "limit": limit}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Query Neo4j for the restaurant KG (no LlamaIndex).")
    p.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    p.add_argument("--user", default=os.getenv("NEO4J_USERNAME", "neo4j"))
    p.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"), required=os.getenv("NEO4J_PASSWORD") is None)
    p.add_argument("--limit", type=int, default=10)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    while True:
        q = input("Query (q/quit to exit): ").strip()
        if not q or q.lower() in {"q", "quit", "exit"}:
            break

        cypher, params = recommend_stores_by_text(q, limit=args.limit)
        logger.info("Cypher:\n%s", cypher.strip())

        rows = run_cypher(args.uri, args.user, args.password, cypher, params)
        if not rows:
            print("Không tìm thấy kết quả phù hợp.")
            continue

        for i, r in enumerate(rows, 1):
            props = _json_loads_maybe(r.get("properties_json"))
            print(f"\n#{i} {r.get('store_name')} ({r.get('store_id')})")
            if r.get("categories"):
                print("  - Category:", ", ".join([x for x in r["categories"] if x]))
            if r.get("services"):
                print("  - Service:", ", ".join([x for x in r["services"] if x]))
            if r.get("context_tags"):
                print("  - Context:", ", ".join([x for x in r["context_tags"] if x]))
            if isinstance(props, dict):
                addr = props.get("address") or props.get("full_address")
                if addr:
                    print("  - Address:", addr)


if __name__ == "__main__":
    main()
