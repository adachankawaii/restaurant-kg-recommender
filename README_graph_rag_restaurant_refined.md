# Graph RAG Restaurant Recommendation — refined run guide

Notebook: `graph_rag_restaurant_recommend_refined_official.ipynb`

Bản này cố tình chạy theo kiểu **fail-fast**: thiếu file dữ liệu, thiếu Neo4j, thiếu Qdrant, thiếu LLM key, thiếu model hoặc thiếu Neo4j GDS plugin thì cell sẽ lỗi ngay. Không có mock mode, demo fallback, hay rule-based fallback.

## 1. Những thay đổi chính

- Embedding mặc định đổi sang `bkai-foundation-models/vietnamese-bi-encoder`.
- Có thể đổi sang `VoVanPhuc/sup-SimCSE-VietNamese-phobert-base` bằng biến môi trường `EMBED_MODEL`.
- Aspect sentiment không còn keyword matching. Mỗi review được đưa qua PhoBERT-based sentiment model theo từng aspect prompt.
- Intent parsing dùng LLM làm primary path với Pydantic structured output `RestaurantIntent`.
- Graph schema có `TextUnit`, `Community`, `CommunityReport`.
- `SIMILAR_TO` được tính bằng cosine similarity giữa embedding của restaurant summary, không dùng attribute vector thủ công.
- Community detection dùng Neo4j Graph Data Science `gds.leiden.write`.
- Rerank dùng Reciprocal Rank Fusion thay cho bộ weight hardcode kiểu magic number.

## 2. Cấu trúc file cần có

Đặt notebook ở repo root cùng các file dữ liệu sau, hoặc cấu hình đường dẫn bằng `.env`:

```text
repo_root/
├── graph_rag_restaurant_recommend_refined_official.ipynb
├── be_google_maps_unique.csv
├── store_feedback_crawled.csv
└── foody_hust_output/
    └── foody_hust_places_from_store_csv.csv
```

Các biến đường dẫn hỗ trợ:

```env
DATA_ROOT=.
GOOGLE_MAPS_PATH=./be_google_maps_unique.csv
FEEDBACK_PATH=./store_feedback_crawled.csv
FOODY_PATH=./foody_hust_output/foody_hust_places_from_store_csv.csv
```

## 3. Chạy Neo4j + Qdrant

Neo4j cần cài Graph Data Science plugin vì notebook gọi trực tiếp `gds.leiden.write`.

Ví dụ `docker-compose.yml`:

```yaml
services:
  neo4j:
    image: neo4j:5.21.0
    container_name: neo4j-graphrag
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      - NEO4J_AUTH=neo4j/neo4j123
      - NEO4J_PLUGINS=["graph-data-science"]
      - NEO4J_dbms_security_procedures_unrestricted=gds.*
      - NEO4J_dbms_security_procedures_allowlist=gds.*
    volumes:
      - neo4j_data:/data

  qdrant:
    image: qdrant/qdrant:v1.9.1
    container_name: qdrant-graphrag
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  neo4j_data:
  qdrant_data:
```

Chạy:

```bash
docker compose up -d
```

Kiểm tra Neo4j GDS:

```cypher
RETURN gds.version() AS gds_version;
```

## 4. Tạo file `.env`

Dùng OpenAI:

```env
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=neo4j123

QDRANT_HOST=localhost
QDRANT_PORT=6333

EMBED_MODEL=bkai-foundation-models/vietnamese-bi-encoder
ASPECT_SENTIMENT_MODEL=wonrax/phobert-base-vietnamese-sentiment
```

Hoặc dùng Anthropic:

```env
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-sonnet-4-20250514
```

Đổi embedding sang SimCSE PhoBERT nếu muốn:

```env
EMBED_MODEL=VoVanPhuc/sup-SimCSE-VietNamese-phobert-base
```

## 5. Thứ tự chạy notebook

Chạy tuần tự từ trên xuống dưới:

1. Cài thư viện.
2. Load config và kiểm tra file dữ liệu.
3. Đọc CSV, chuẩn hóa schema.
4. Chạy PhoBERT-based aspect sentiment.
5. Tạo `TextUnit` từ review.
6. Kết nối Neo4j và tạo constraint/index.
7. Upsert `Restaurant`, `Review`, `TextUnit`, `Attribute`, các node domain.
8. Load embedding model, kết nối Qdrant, recreate collection.
9. Embed restaurant summary và text units, index vào Qdrant.
10. Tạo `SIMILAR_TO` bằng embedding cosine.
11. Chạy GDS Leiden để tạo `Community`.
12. Dùng LLM tạo `CommunityReport`.
13. Dùng `recommend(query, top_k=5)` để hỏi.

## 6. Cách gọi thử sau khi build xong index

```python
recommend("gợi ý quán ăn ngon, sạch sẽ, giá hợp lý quanh Hai Bà Trưng", top_k=5)
```

## 7. Evaluation

Notebook không còn synthetic demo labels. Muốn đánh giá retrieval, tự truyền nhãn thật:

```python
test_cases = [
    {"query": "quán bún ngon gần Bách Khoa", "relevant_store_keys": ["..."]},
]
evaluate_retrieval(test_cases, k=5)
```

Nếu `test_cases=[]`, hàm sẽ lỗi để tránh tạo cảm giác đã có benchmark thật.

## 8. Lỗi thường gặp

### `There is no procedure with the name gds...`

Neo4j chưa có Graph Data Science plugin. Cài lại container với:

```yaml
NEO4J_PLUGINS=["graph-data-science"]
```

### `LLM_PROVIDER=openai but OPENAI_API_KEY is missing`

Notebook bắt buộc LLM thật cho structured intent và community report. Thêm key vào `.env`.

### Model sentiment trả label lạ

Cell aspect sentiment sẽ lỗi nếu model trả label ngoài `negative/neutral/positive`, `neg/neu/pos`, hoặc `0/1/2`. Khi đó cần cập nhật `LABEL_TO_SCORE` đúng theo `model.config.id2label`.

### Qdrant collection bị xóa rồi tạo lại

Notebook dùng `recreate_collection` để tránh index cũ làm nhiễu kết quả. Nếu muốn giữ index cũ, đổi hàm này trước khi chạy.
