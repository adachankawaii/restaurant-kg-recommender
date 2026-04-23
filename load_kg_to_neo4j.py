from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from neo4j import GraphDatabase


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def chunked(items: list[dict[str, str]], size: int) -> list[list[dict[str, str]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def relation_type(raw_relation: object) -> str:
    value = re.sub(r"[^0-9A-Za-z_]", "_", str(raw_relation or "RELATION").strip().upper())
    value = re.sub(r"_+", "_", value).strip("_")
    if not value:
        return "RELATION"
    if value[0].isdigit():
        return f"R_{value}"
    return value


def import_nodes(session, node_rows: list[dict[str, str]], batch_size: int) -> None:
    session.run("CREATE CONSTRAINT entity_node_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.node_id IS UNIQUE")

    query = """
    UNWIND $rows AS row
    MERGE (n:Entity {node_id: row.node_id})
    SET n.name = row.name,
        n.node_label = row.label,
        n.properties_json = row.properties
    FOREACH (_ IN CASE WHEN row.label = 'Store' THEN [1] ELSE [] END | SET n:Store)
    FOREACH (_ IN CASE WHEN row.label = 'Category' THEN [1] ELSE [] END | SET n:Category)
    FOREACH (_ IN CASE WHEN row.label = 'Area' THEN [1] ELSE [] END | SET n:Area)
    FOREACH (_ IN CASE WHEN row.label = 'ContextTag' THEN [1] ELSE [] END | SET n:ContextTag)
    FOREACH (_ IN CASE WHEN row.label = 'ServiceOption' THEN [1] ELSE [] END | SET n:ServiceOption)
    FOREACH (_ IN CASE WHEN row.label = 'Aspect' THEN [1] ELSE [] END | SET n:Aspect)
    FOREACH (_ IN CASE WHEN row.label = 'Review' THEN [1] ELSE [] END | SET n:Review)
    FOREACH (_ IN CASE WHEN row.label = 'DataSource' THEN [1] ELSE [] END | SET n:DataSource)
    FOREACH (_ IN CASE WHEN row.label = 'User' THEN [1] ELSE [] END | SET n:User)
    FOREACH (_ IN CASE WHEN row.label = 'UserSession' THEN [1] ELSE [] END | SET n:UserSession)
    FOREACH (_ IN CASE WHEN row.label = 'UserPreference' THEN [1] ELSE [] END | SET n:UserPreference)
    FOREACH (_ IN CASE WHEN row.label = 'MenuItem' THEN [1] ELSE [] END | SET n:MenuItem)
    FOREACH (_ IN CASE WHEN row.label = 'Location' THEN [1] ELSE [] END | SET n:Location)
    FOREACH (_ IN CASE WHEN row.label = 'SourceMatchQuality' THEN [1] ELSE [] END | SET n:SourceMatchQuality)
    """

    for batch in chunked(node_rows, batch_size):
        session.run(query, rows=batch).consume()


def import_edges(session, edge_rows: list[dict[str, str]], batch_size: int) -> None:
    grouped_rows: dict[str, list[dict[str, str]]] = {}
    for row in edge_rows:
        grouped_rows.setdefault(relation_type(row.get("relation")), []).append(row)

    for rel_type, rows in grouped_rows.items():
        query = f"""
        UNWIND $rows AS row
        MATCH (s:Entity {{node_id: row.source_id}})
        MATCH (t:Entity {{node_id: row.target_id}})
        MERGE (s)-[r:`{rel_type}` {{
            source_id: row.source_id,
            relation: row.relation,
            target_id: row.target_id
        }}]->(t)
        SET r.properties_json = row.properties
        """

        for batch in chunked(rows, batch_size):
            session.run(query, rows=batch).consume()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import KG CSV files into Neo4j and prepare graph visualization.")
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", required=True, help="Neo4j password")
    parser.add_argument(
        "--nodes",
        type=Path,
        default=Path("kg_tables_all") / "kg_graph" / "nodes.csv",
        help="Path to nodes.csv",
    )
    parser.add_argument(
        "--edges",
        type=Path,
        default=Path("kg_tables_all") / "kg_graph" / "edges.csv",
        help="Path to edges.csv",
    )
    parser.add_argument("--wipe", action="store_true", help="Delete existing graph data before import")
    parser.add_argument("--batch-size", type=int, default=1000, help="UNWIND batch size")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    node_rows = read_csv_rows(args.nodes)
    edge_rows = read_csv_rows(args.edges)

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    with driver.session() as session:
        if args.wipe:
            session.run("MATCH (n) DETACH DELETE n").consume()

        import_nodes(session, node_rows, args.batch_size)
        import_edges(session, edge_rows, args.batch_size)

        summary = session.run(
            """
            MATCH (n:Entity)
            OPTIONAL MATCH ()-[r]->()
            RETURN count(DISTINCT n) AS node_count, count(r) AS edge_count
            """
        ).single()

    driver.close()

    print(f"Imported {summary['node_count']} nodes and {summary['edge_count']} relationships into Neo4j.")
    print("Open Neo4j Browser and run: MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 300")


if __name__ == "__main__":
    main()
