# Restaurant GraphRAG Recommender

Hệ thống gợi ý quán ăn dùng GraphRAG cho dữ liệu BeFood/Foody quanh khu Bách Khoa. Repo kết hợp:

- Neo4j để lưu knowledge graph nhà hàng, menu, review, aspect, khu vực và cộng đồng.
- Qdrant để semantic search trên restaurant summary và review text units.
- LLM để parse intent và sinh câu trả lời.
- RRF, geo-aware ranking, post-fusion constraint validation và optional cross-encoder để rerank kết quả.
- Distance không được embed vào restaurant summary vì đây là tín hiệu theo từng user/session; hệ thống chỉ lưu `lat/lng` và tính `distance_km` tại thời điểm query.

Notebook chính vẫn là `graph_rag.ipynb`, nhưng logic quan trọng đã được tách ra module Python để dễ test và tiến tới API/production.

## Repo Structure

```text
restaurant-kg-recommender/
├── graph_rag.ipynb
├── README.md
├── README_qdrant_graph_rag.md
├── README_graph_rag_restaurant_refined.md
├── requirements.txt
├── .env.example
├── config.py
├── ingest.py
├── aspect_sentiment.py
├── cache.py
├── graph_store.py
├── vector_store.py
├── retriever.py
├── ranker.py
├── evaluation.py
├── observability.py
├── llm.py
├── api.py
├── Utils/
│   ├── befood_bachkhoa_restaurants.csv
│   ├── befood_bachkhoa_menu_items.csv
│   └── foody_hust_places_from_store_csv.csv
└── tests/
    ├── test_ingest.py
    ├── test_ranker.py
    └── test_retriever.py
```

## File Roles

### Notebook

- `graph_rag.ipynb`: notebook end-to-end để thử nghiệm GraphRAG. Nó load data, canonicalize, build graph, index Qdrant, detect community, retrieve, rerank và generate answer.

### Data

- `Utils/befood_bachkhoa_restaurants.csv`: dữ liệu quán BeFood, gồm tên quán, địa chỉ, tọa độ, rating, categories, opening hours, delivery time, comments.
- `Utils/befood_bachkhoa_menu_items.csv`: dữ liệu menu đầy đủ, gồm món, category, giá, order count, like/dislike.
- `Utils/foody_hust_places_from_store_csv.csv`: dữ liệu Foody bổ sung cho một phần quán.

### Core Modules

- `config.py`: đọc `.env`, gom toàn bộ config: đường dẫn data, Neo4j, Qdrant, model, LLM, distance, cache, observability.
- `ingest.py`: xử lý CSV, canonicalize schema, parse comments, chuẩn hóa menu, suy luận district, tính distance, tính `price_band` từ phân phối giá menu, tạo `summary`, `feedback`, `menu_items`, `dish_families`.
- `aspect_sentiment.py`: PhoBERT aspect sentiment cho review/comment, có cache disk để tránh chạy lại model khi input không đổi.
- `cache.py`: JSON disk cache dùng cho embedding và aspect sentiment.
- `graph_store.py`: Neo4j client, tạo constraints/indexes, upsert `Restaurant`, `Review`, `TextUnit`, `Attribute`, `MenuItem`, `DishFamily`.
- `vector_store.py`: embedding service có cache, Qdrant collection management, index/search restaurant summary và text units.
- `retriever.py`: hybrid retriever: graph filtering, graph neighbor expansion, vector search, text evidence search, trace hook.
- `cross_encoder.py`: optional cross-encoder reranker cho API/module; lazy-load `FlagReranker`, build passage giàu context và normalize CE score theo từng query.
- `ranker.py`: RRF fusion, geo intent (`nearest`, `nearby`, `normal`), post-fusion validation cho dish/price/distance/rating/evidence, rating bonus, source flags, score components.
- `evaluation.py`: offline metrics gồm Recall@K, MRR@K, nDCG@K, latency và cost field.
- `observability.py`: ghi trace JSONL cho mỗi request: intent, candidates, source flags, distance, score components, latency.
- `llm.py`: LLM provider wrapper, intent parser, fallback parser, answer generation.
- `api.py`: FastAPI app với endpoint `/recommend`.

### Tests

- `tests/test_ingest.py`: test canonicalize data, Haversine distance, price band, district inference, explode comments, prepare data.
- `tests/test_ranker.py`: test `safe_float`, distance-aware reranking và merge evidence từ text units.
- `tests/test_retriever.py`: test graph query builder có sinh đúng filter district, dish, rating, distance và attribute.

## GraphRAG Pipeline

Luồng chính:

1. Load CSV từ `Utils/`.
2. Canonicalize:
   - Quán -> `summary`
   - Comments -> `feedback`
   - Menu -> `menu_items`
   - Menu item names -> `dish_families`
3. Chấm aspect sentiment cho review/comment.
4. Chunk review thành `TextUnit`.
5. Upsert Neo4j graph.
6. Build restaurant summary docs và text unit docs.
7. Embed docs và index vào Qdrant.
8. Tạo `SIMILAR_TO` edges từ cosine similarity embedding.
9. Detect community bằng Leiden (`python-igraph`).
10. Parse user query thành intent.
11. Retrieve bằng graph + vector + text evidence.
12. Rerank bằng RRF + rating + geo-aware distance, validate constraints sau fusion, rồi optional cross-encoder.
13. Sinh câu trả lời bằng LLM.

## Graph Schema

Node chính:

- `Restaurant`
- `Review`
- `TextUnit`
- `Attribute`
- `MenuItem`
- `MenuCategory`
- `DishFamily`
- `Area`
- `Category`
- `Cuisine`
- `PriceBand`
- `AtmosphereTag`
- `Community`
- `CommunityReport`

Quan hệ chính:

- `(Review)-[:HAS_TEXT_UNIT]->(TextUnit)`
- `(TextUnit)-[:ABOUT]->(Restaurant)`
- `(TextUnit)-[:MENTIONS_ASPECT]->(Attribute)`
- `(Restaurant)-[:HAS_ATTRIBUTE]->(Attribute)`
- `(Restaurant)-[:HAS_MENU_ITEM]->(MenuItem)`
- `(MenuItem)-[:IN_MENU_CATEGORY]->(MenuCategory)`
- `(Restaurant)-[:SERVES_FAMILY]->(DishFamily)`
- `(Restaurant)-[:IN_AREA]->(Area)`
- `(Restaurant)-[:HAS_CATEGORY]->(Category)`
- `(Restaurant)-[:HAS_PRICE_BAND]->(PriceBand)`
- `(Restaurant)-[:SIMILAR_TO]->(Restaurant)`
- `(Restaurant)-[:IN_COMMUNITY]->(Community)`
- `(Community)-[:HAS_REPORT]->(CommunityReport)`

## Setup

### 1. Create Virtual Env

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install Dependencies

```powershell
pip install -r requirements.txt
```

### 3. Create `.env`

```powershell
Copy-Item .env.example .env
```

Edit `.env`:

```env
DATA_ROOT=./Utils

NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password123

QDRANT_HOST=localhost
QDRANT_PORT=6333
RECREATE_QDRANT=true

LLM_PROVIDER=openai
OPENAI_API_KEY=your_key
OPENAI_MODEL=gpt-4o-mini
OPENAI_BASE_URL=

USER_LAT=21.005
USER_LNG=105.843
MAX_DISTANCE_KM=3
DISTANCE_WEIGHT=0.20
DISTANCE_DECAY_KM=3
CROSS_ENCODER_MODEL=BAAI/bge-reranker-base
USE_CROSS_ENCODER=true
CROSS_ENCODER_WEIGHT=0.30

RUN_COMMUNITY_REPORTS=true
CACHE_DIR=.cache/graphrag
OBSERVABILITY_LOG_PATH=logs/retrieval_traces.jsonl
```

Nếu dùng OpenRouter hoặc endpoint OpenAI-compatible khác, set:

```env
OPENAI_BASE_URL=https://openrouter.ai/api/v1
```

Nếu dùng OpenAI trực tiếp, để `OPENAI_BASE_URL=` rỗng.

## Run Services

Notebook/API cần Neo4j và Qdrant đang chạy.

Ví dụ chạy bằng Docker:

```powershell
docker run -d --name neo4j-graphrag `
  -p 7474:7474 -p 7687:7687 `
  -e NEO4J_AUTH=neo4j/password123 `
  neo4j:5
```

```powershell
docker run -d --name qdrant-graphrag `
  -p 6333:6333 `
  -v qdrant_storage:/qdrant/storage `
  qdrant/qdrant
```

## Run Notebook

Mở `graph_rag.ipynb` và chạy theo thứ tự:

1. Install/import/config.
2. Inspect data.
3. Canonicalize data.
4. Build menu-derived entities.
5. Run aspect sentiment.
6. Build text units.
7. Connect Neo4j and create schema.
8. Upsert graph.
9. Connect Qdrant and index vectors.
10. Build similarity edges and communities.
11. Load LLM parser.
12. Run retrieval/generation.

Với location-aware recommendation:

```python
set_user_location(21.005, 105.843, max_distance_km=3)
intent, ranked = hybrid_retrieve("gợi ý quán cơm ngon gần tôi", top_k=5)
```

Hoặc:

```python
answer = recommend(
    "gợi ý quán cơm ngon gần tôi",
    top_k=5,
    user_lat=21.005,
    user_lng=105.843,
)
```

## Run API

Start server:

```powershell
uvicorn api:app --host 0.0.0.0 --port 8000
```

Health check:

```powershell
curl http://localhost:8000/health
```

Recommend request:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8000/recommend `
  -ContentType "application/json" `
  -Body '{
    "query": "gợi ý quán cơm ngon gần tôi",
    "top_k": 5,
    "user_lat": 21.005,
    "user_lng": 105.843
  }'
```

Response fields:

- `query`: original query.
- `intent`: parsed intent.
- `answer`: generated answer or fallback context.
- `results`: reranked candidates.
- `trace_id`: id để tra log observability.

## Run Tests

Chạy toàn bộ test:

```powershell
python -m pytest tests -q
```

Kết quả hiện tại:

```text
10 passed
```

Chạy từng nhóm:

```powershell
python -m pytest tests/test_ingest.py -q
python -m pytest tests/test_ranker.py -q
python -m pytest tests/test_retriever.py -q
```

## Test Cases Explained

### `tests/test_ingest.py`

Các case:

- `test_haversine_km_zero_distance`: cùng tọa độ thì distance phải bằng `0.0`.
- `test_haversine_km_known_small_distance`: kiểm tra khoảng cách thực tế nhỏ giữa 2 điểm gần Bách Khoa.
- `test_price_band_from_bounds`: map khoảng giá nguồn sang `budget`, `mid`, `premium`.
- `test_price_band_from_menu_prices_uses_distribution_not_max`: đảm bảo `price_band` chính được suy ra từ median và tỷ lệ món giá rẻ trong menu, không bị một món max-price kéo lên `premium`.
- `test_normalize_dish_family_groups_menu_items`: gom tên món cụ thể như `Cơm Gà Xối Mỡ`, `Bún chả đặc biệt`, `Gà rán combo` về family rộng.
- `test_infer_district_from_vietnamese_address`: suy luận quận từ địa chỉ text.
- `test_feedback_explodes_comments_list`: tách `comments_list` JSON string thành nhiều dòng feedback.
- `test_prepare_data_shapes_and_distance`: tạo mock BeFood/Menu/Foody nhỏ và kiểm tra output `summary`, `feedback`, `menu_items`, `dish_families`, `distance_km`.

### `tests/test_ranker.py`

Các case:

- `test_safe_float_handles_bad_values`: đảm bảo score parser không crash với `None` hoặc string lỗi.
- `test_rerank_uses_distance_for_near_query`: query có ý “gần tôi” thì quán gần được ưu tiên dù rating thấp hơn.
- `test_text_unit_evidence_is_merged`: evidence từ review text unit được merge vào candidate.
- `test_post_fusion_validation_filters_wrong_price_and_dish`: candidate từ vector branch bị loại nếu sai dish/price constraint.
- `test_nearest_geo_intent_sorts_by_distance_first`: query “gần nhất” sort theo khoảng cách trước score tổng.

### `tests/test_retriever.py`

Các case:

- `test_build_graph_candidate_query_includes_filters`: kiểm tra Cypher query builder có đủ filter `district`, `dish_name`, `min_rating`, `max_distance_km`, `required_attributes`.

Các test hiện không cần Neo4j/Qdrant/model thật, nên chạy nhanh và phù hợp cho regression test logic.

## Cache

Cache mặc định:

```text
.cache/graphrag/
```

Cache files:

- `embeddings.json`: cache embedding theo `{model, prefix, text}`.
- `aspect_sentiment.json`: cache sentiment theo `{model, review}`.

Xóa cache khi đổi model hoặc muốn rebuild:

```powershell
Remove-Item .cache\graphrag -Recurse -Force
```

## Observability

Trace log mặc định:

```text
logs/retrieval_traces.jsonl
```

Mỗi request có thể log:

- `query`
- `intent`
- `candidates`
- `source_flags`
- `distance_km`
- `rating`
- `rrf_score`
- `graph_rank_score`
- `neighbor_score`
- `restaurant_vec_score`
- `text_unit_vec_score`
- `distance_score`
- `ce_raw_score`
- `ce_score`
- `final_score`
- `latency_ms`

Mục đích: debug vì sao một quán được recommend, so sánh score components và kiểm tra hệ thống có dùng đúng distance/source evidence không.

## Offline Evaluation

`evaluation.py` hỗ trợ:

- `recall_at_k`
- `mrr_at_k`
- `ndcg_at_k`
- latency per query
- cost field per query

Ví dụ:

```python
from evaluation import evaluate_retriever

test_cases = [
    {
        "query": "quán cơm gần Bách Khoa",
        "relevant_store_keys": ["11471", "11305"],
    }
]

df = evaluate_retriever(
    test_cases,
    retrieve_fn=lambda q, k: [r["store_key"] for r in retriever.retrieve(q, top_k=k)[1]],
    k=5,
)
```

## Notes

- Backend/notebook không tự lấy GPS của user. Frontend/mobile phải xin quyền location rồi gửi `user_lat/user_lng` lên API.
- Browser geolocation thường yêu cầu HTTPS và user permission.
- IP geolocation không đủ chính xác cho bài toán gợi ý quán ăn gần user.
- `RECREATE_QDRANT=true` sẽ xóa và tạo lại collection Qdrant. Đặt `false` nếu muốn giữ index cũ.
- `RUN_COMMUNITY_REPORTS=true` bật sinh/check `CommunityReport`; đặt `false` nếu muốn tránh tự động gọi LLM tốn chi phí khi chạy notebook.
