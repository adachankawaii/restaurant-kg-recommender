#!/usr/bin/env python
"""Validate KG data imported into Neo4j"""
from neo4j import GraphDatabase

uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', 'neo4j123'))

queries = [
    ('Store nodes', 'MATCH (s:Store) RETURN count(s) as store_count'),
    ('Category nodes', 'MATCH (c:Category) RETURN count(c) as category_count'),
    ('Area nodes', 'MATCH (a:Area) RETURN count(a) as area_count'),
    ('Stores with categories (sample)', 'MATCH (s:Store)-[:HAS_PRIMARY_CATEGORY]->(c:Category) RETURN s.name, c.name LIMIT 5'),
    ('Review count by source', 'MATCH (r:Review) RETURN r.source_name, count(r) as review_count LIMIT 10'),
]

try:
    with driver.session() as session:
        for query_name, query in queries:
            result = session.run(query)
            records = list(result)
            print(f'\n{query_name}:')
            if records:
                for i, record in enumerate(records[:5]):  # Limit to 5 results per query
                    print(f'  [{i+1}] {dict(record)}')
                if len(records) > 5:
                    print(f'  ... ({len(records) - 5} more results)')
            else:
                print('  (no results)')
finally:
    driver.close()

print('\n✅ Validation complete!')
