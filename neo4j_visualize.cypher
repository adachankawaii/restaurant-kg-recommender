// 1) Tổng quan số lượng node/relationship
MATCH (n:Entity)
OPTIONAL MATCH ()-[r:RELATED]->()
RETURN count(DISTINCT n) AS node_count, count(r) AS relationship_count;

// 2) Hiển thị subgraph tổng quát
MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 300;

// 3) Tập trung vào user va store
MATCH (u:User)-[r]->(s:Store)
RETURN u, r, s
LIMIT 200;

// 4) Cua hang va category
MATCH (s:Store)-[r]->(c:Category)
RETURN s, r, c
LIMIT 200;

// 5) Top store theo so review edge
MATCH (:Review)-[r]->(s:Store)
RETURN s.name AS store, count(r) AS review_edges
ORDER BY review_edges DESC
LIMIT 20;

// 6) Danh sach tat ca loai quan he
MATCH ()-[r]->()
RETURN DISTINCT type(r) AS relationship_type, count(*) AS occurrences
ORDER BY occurrences DESC, relationship_type;
