# Hướng dẫn sử dụng Qdrant cho Graph-RAG Restaurant Recommender

README này hướng dẫn cách chạy Qdrant bằng Docker và cách dùng Qdrant làm vector store cho pipeline Graph-RAG gợi ý nhà hàng.

---

## 1. Qdrant dùng để làm gì trong pipeline?

Trong pipeline Graph-RAG, Qdrant lưu các vector embedding cho:

- `TextUnit`: các đoạn review/chunk nhỏ.
- `Restaurant`: embedding tổng hợp từ các text unit của từng nhà hàng.
- `CommunityReport`: embedding của báo cáo cộng đồng.

Khi người dùng nhập truy vấn, hệ thống encode truy vấn thành vector rồi tìm các vector gần nhất trong Qdrant.

```text
User Query
   ↓
Vietnamese Embedding Model
   ↓
Query Vector
   ↓
Qdrant Similarity Search
   ↓
Candidate TextUnits / Restaurants / CommunityReports
   ↓
Neo4j Graph Expansion
   ↓
Final Recommendation
```

---

## 2. Chạy Qdrant bằng Docker

### 2.1. Start Docker Desktop

Trên Windows PowerShell:

```powershell
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"

while (-not (docker info 2>$null)) {
    Start-Sleep -Seconds 2
}
Write-Host "Docker is ready"
```

### 2.2. Chạy Qdrant container

```powershell
docker run -d --name qdrant-graphrag -p 6333:6333 -p 6334:6334 -v qdrant_storage:/qdrant/storage qdrant/qdrant:latest
```

Trong đó:

- `6333`: REST API.
- `6334`: gRPC API.
- `qdrant_storage`: volume lưu dữ liệu vector.

### 2.3. Kiểm tra Qdrant đã chạy chưa

```powershell
docker ps
```

Bạn cần thấy container `qdrant-graphrag`.

Kiểm tra health bằng browser:

```text
http://localhost:6333/
```

Nếu chạy đúng, Qdrant trả JSON dạng:

```json
{
  "title": "qdrant - vector search engine",
  "version": "..."
}
```

---

## 3. Cài Python client

```bash
pip install qdrant-client
```

Nếu pipeline dùng embedding model:

```bash
pip install sentence-transformers transformers accelerate
```

---

## 4. Cấu hình kết nối Qdrant

```python
QDRANT_URL = "http://127.0.0.1:6333"
TEXT_UNIT_COLLECTION = "restaurant_text_units"
RESTAURANT_COLLECTION = "restaurant_embeddings"
COMMUNITY_COLLECTION = "community_reports"
```

Khởi tạo client:

```python
from qdrant_client import QdrantClient

qdrant_client = QdrantClient(url=QDRANT_URL)
print(qdrant_client.get_collections())
```

Nếu dòng trên lỗi, không fallback. Cần dừng pipeline và sửa hạ tầng.

---

## 5. Tạo collection

Vector size phải khớp với embedding model.

Ví dụ nếu model trả vector 768 chiều:

```python
from qdrant_client.models import Distance, VectorParams

VECTOR_SIZE = 768

for collection_name in [
    TEXT_UNIT_COLLECTION,
    RESTAURANT_COLLECTION,
    COMMUNITY_COLLECTION,
]:
    qdrant_client.recreate_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(
            size=VECTOR_SIZE,
            distance=Distance.COSINE,
        ),
    )
```

Lưu ý: `recreate_collection` sẽ xóa collection cũ nếu tồn tại. Khi chạy production, nên dùng logic kiểm tra tồn tại thay vì recreate.

---

## 6. Upsert TextUnit embeddings

Mỗi `TextUnit` cần lưu:

- vector embedding
- `text_unit_id`
- `store_key`
- `review_id`
- `text`
- `source`
- `sentiment`
- `rating`

```python
from qdrant_client.models import PointStruct

points = []

for _, row in text_units.iterrows():
    points.append(
        PointStruct(
            id=row["text_unit_id"],
            vector=row["embedding"],
            payload={
                "text_unit_id": row["text_unit_id"],
                "store_key": row["store_key"],
                "review_id": row["review_id"],
                "text": row["chunk_text"],
                "source": row["source"],
                "sentiment": row["sentiment"],
                "rating": float(row["rating"]) if row["rating"] is not None else None,
            },
        )
    )

qdrant_client.upsert(
    collection_name=TEXT_UNIT_COLLECTION,
    points=points,
)
```

---

## 7. Search TextUnit bằng query

```python
query = "Tìm quán ăn gia đình ở Cầu Giấy, đồ ăn ngon, phục vụ ổn"

query_vector = embedding_model.encode(
    query,
    normalize_embeddings=True,
).tolist()

results = qdrant_client.search(
    collection_name=TEXT_UNIT_COLLECTION,
    query_vector=query_vector,
    limit=10,
    with_payload=True,
)

for hit in results:
    print(hit.score, hit.payload["store_key"], hit.payload["text"])
```

Các `store_key` này sẽ được đưa sang Neo4j để graph expansion.

---

## 8. Search có filter

Ví dụ chỉ tìm review ở một quận cụ thể nếu payload có `district`:

```python
from qdrant_client.models import Filter, FieldCondition, MatchValue

results = qdrant_client.search(
    collection_name=TEXT_UNIT_COLLECTION,
    query_vector=query_vector,
    limit=10,
    query_filter=Filter(
        must=[
            FieldCondition(
                key="district",
                match=MatchValue(value="Cầu Giấy"),
            )
        ]
    ),
    with_payload=True,
)
```

---

## 9. Restaurant embedding

Restaurant embedding nên được tổng hợp từ embedding của các text units:

```python
import numpy as np

def mean_pool_normalized(vectors):
    arr = np.asarray(vectors, dtype=np.float32)
    pooled = arr.mean(axis=0)
    norm = np.linalg.norm(pooled)
    if norm == 0:
        raise RuntimeError("Restaurant embedding has zero norm")
    return (pooled / norm).tolist()
```

Upsert restaurant vectors:

```python
from qdrant_client.models import PointStruct

restaurant_points = []

for store_key, group in text_units.groupby("store_key"):
    vectors = group["embedding"].tolist()
    restaurant_vector = mean_pool_normalized(vectors)

    restaurant_points.append(
        PointStruct(
            id=store_key,
            vector=restaurant_vector,
            payload={
                "store_key": store_key,
            },
        )
    )

qdrant_client.upsert(
    collection_name=RESTAURANT_COLLECTION,
    points=restaurant_points,
)
```

---

## 10. CommunityReport embedding

```python
community_points = []

for _, row in community_reports.iterrows():
    report_text = row["summary"]

    vector = embedding_model.encode(
        report_text,
        normalize_embeddings=True,
    ).tolist()

    community_points.append(
        PointStruct(
            id=row["report_id"],
            vector=vector,
            payload={
                "report_id": row["report_id"],
                "community_id": row["community_id"],
                "summary": report_text,
            },
        )
    )

qdrant_client.upsert(
    collection_name=COMMUNITY_COLLECTION,
    points=community_points,
)
```

---

## 11. Kết hợp Qdrant với Neo4j

Qdrant chỉ làm semantic retrieval. Neo4j làm graph expansion.

```python
text_hits = qdrant_client.search(
    collection_name=TEXT_UNIT_COLLECTION,
    query_vector=query_vector,
    limit=20,
    with_payload=True,
)

seed_store_keys = list({
    hit.payload["store_key"]
    for hit in text_hits
})
```

Sau đó truy vấn Neo4j:

```python
records = neo4j_client.run(
    """
    MATCH (r:Restaurant)
    WHERE r.store_key IN $store_keys
    OPTIONAL MATCH (r)-[:HAS_CUISINE]->(c:Cuisine)
    OPTIONAL MATCH (r)-[:IN_AREA]->(a:Area)
    OPTIONAL MATCH (r)-[:HAS_ATTRIBUTE]->(att:Attribute)
    RETURN
        r.store_key AS store_key,
        r.name AS name,
        collect(DISTINCT c.name) AS cuisines,
        collect(DISTINCT a.name) AS areas,
        collect(DISTINCT {
            type: att.type,
            score: att.score,
            sample_count: att.sample_count
        }) AS attributes
    """,
    {"store_keys": seed_store_keys},
)
```

---

## 12. Health-check fail-fast

Không dùng fallback như:

```python
if qdrant_client is None:
    return []
```

Thay vào đó:

```python
def assert_qdrant_ready(client: QdrantClient):
    try:
        client.get_collections()
    except Exception as e:
        raise RuntimeError(
            "Qdrant is not available. Start qdrant-graphrag container before running the pipeline."
        ) from e
```

Dùng:

```python
assert_qdrant_ready(qdrant_client)
```

---

## 13. Lệnh quản lý Qdrant

Start container:

```powershell
docker start qdrant-graphrag
```

Stop container:

```powershell
docker stop qdrant-graphrag
```

Xem logs:

```powershell
docker logs -f qdrant-graphrag
```

Xóa container:

```powershell
docker rm -f qdrant-graphrag
```

Xóa cả dữ liệu vector:

```powershell
docker volume rm qdrant_storage
```

---

## 14. Script start environment

Tạo file `start_env.ps1`:

```powershell
Write-Host "Starting Docker Desktop..."
Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"

Write-Host "Waiting for Docker..."
while (-not (docker info 2>$null)) {
    Start-Sleep -Seconds 2
}

Write-Host "Starting Neo4j..."
docker start neo4j-graphrag

Write-Host "Starting Qdrant..."
docker start qdrant-graphrag

Write-Host "Waiting for services..."
Start-Sleep -Seconds 10

Write-Host "Checking containers..."
docker ps

Write-Host "Environment ready."
```

Chạy:

```powershell
.\start_env.ps1
```

---

## 15. Checklist trước khi chạy notebook

Trước khi chạy notebook Graph-RAG, kiểm tra:

```powershell
docker ps
```

Cần có:

```text
neo4j-graphrag
qdrant-graphrag
```

Kiểm tra Qdrant:

```text
http://localhost:6333/
```

Kiểm tra Neo4j:

```text
http://localhost:7474/
```

Trong notebook:

```python
assert_qdrant_ready(qdrant_client)
neo4j_client.driver.verify_connectivity()
```

Nếu một trong hai bước lỗi, dừng pipeline và sửa hạ tầng.
