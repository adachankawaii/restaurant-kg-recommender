from __future__ import annotations

from typing import Any


class Neo4jWriter:
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database

    def write_snapshot(self, graph_version: str, nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, object]:
        try:
            from neo4j import GraphDatabase
        except ImportError:
            return {"status": "skipped", "reason": "neo4j package is not installed", "graph_version": graph_version}

        driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        try:
            with driver.session(database=self.database) as session:
                session.run("CREATE CONSTRAINT prod_node_id IF NOT EXISTS FOR (n:ProdNode) REQUIRE n.id IS UNIQUE")
                session.run(
                    """
                    UNWIND $nodes AS row
                    MERGE (n:ProdNode {id: row.id})
                    SET n += row,
                        n.graph_version = $graph_version
                    """,
                    nodes=nodes,
                    graph_version=graph_version,
                )
                session.run(
                    """
                    UNWIND $edges AS row
                    MATCH (src:ProdNode {id: row.src_id})
                    MATCH (dst:ProdNode {id: row.dst_id})
                    MERGE (src)-[r:PROD_REL {relation: row.relation, graph_version: $graph_version}]->(dst)
                    SET r += row
                    """,
                    edges=edges,
                    graph_version=graph_version,
                )
            return {
                "status": "ok",
                "graph_version": graph_version,
                "node_count": len(nodes),
                "edge_count": len(edges),
                "database": self.database,
            }
        except Exception as exc:
            return {
                "status": "failed",
                "reason": f"{type(exc).__name__}: {exc}",
                "graph_version": graph_version,
                "node_count": len(nodes),
                "edge_count": len(edges),
            }
        finally:
            driver.close()
