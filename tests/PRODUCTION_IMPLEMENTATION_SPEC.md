# Production Implementation Spec for GraphRAG + KG + R-GCN Restaurant Recommendation System

> Target reader: Codex / coding agent.
>
> Goal: Refactor the current experimental repository into a production-oriented system under a new top-level folder named `production/`.
>
> Main requirement: support two execution modes:
>
> 1. `offline` mode: reuse existing local CSV files and existing local/cache data.
> 2. `online` mode: run the ingestion/crawling/API-driven data collection pipeline, dump raw data to lake/MinIO, process data, build/update Neo4j KG, and serve GraphRAG/R-GCN recommendation.

---

## 0. Current repository context

The current repository is not a set of fully separated projects. It is a group of related experiments around the same restaurant dataset near Bach Khoa/HUST.

Known raw/local data sources:

```text
Utils/
├── befood_bachkhoa_restaurants.csv
├── befood_bachkhoa_menu_items.csv
└── foody_hust_places_from_store_csv.csv
```

Known GraphRAG/root components:

```text
graph_rag.ipynb
graph_rag_new.ipynb
ingest.py
graph_store.py
vector_store.py
retriever.py
ranker.py
llm.py
api.py
```

Known R-GCN branch:

```text
rgcn_pipeline/
├── data/
│   ├── graphrag_nodes.csv
│   ├── graphrag_edges.csv
│   └── store_metadata.csv
├── user_scenarios_70_befood_bachkhoa.csv
├── data/user_scenarios_phase2_top5.csv
└── outputs_*/
```

Important architectural interpretation:

- The root GraphRAG pipeline can ingest local/cache data and export KG-like structures.
- The R-GCN pipeline mostly uses derived KG/GraphRAG outputs, not raw CSV files directly.
- The production system must keep the static restaurant KG separate from dynamic user behavior logs.
- Neo4j should serve the relatively static KG.
- User behavior logs should be stored in a separate DB/lake and periodically converted into behavior graph snapshots for R-GCN training.

---

## 1. Target production architecture

The production folder must introduce a clean architecture without deleting the old experimental code.

Create a new top-level folder:

```text
production/
```

The production system has two data planes.

### 1.1 Static restaurant data plane

This is low-frequency data. The restaurant/menu/place data changes slowly and can be crawled monthly.

```text
CSV / crawler / API sources
    ↓
raw data lake / MinIO
    ↓
validation + normalization + deduplication
    ↓
canonical restaurant store
    ↓
KG builder
    ↓
Neo4j
    ↓
GraphRAG index builder + vector index
    ↓
GraphRAG serving
```

### 1.2 Dynamic user behavior data plane

This is high-frequency data. It should not be written directly into the main static KG.

```text
Frontend/API user interaction
    ↓
logging service
    ↓
Kafka and/or PostgreSQL
    ↓
MinIO/data lake archive
    ↓
Airflow/Spark periodic aggregation
    ↓
behavior graph snapshot
    ↓
R-GCN training
    ↓
model registry
    ↓
R-GCN serving artifact
```

### 1.3 Online serving flow

```text
User natural language query + optional rule filters
    ↓
API server
    ↓
query parser: natural language → structured rules
    ↓
GraphRAG service and/or R-GCN service
    ↓
rank fusion
    ↓
recommendation response
    ↓
log query, parsed rules, results shown, clicks, feedback
```

GraphRAG is the default engine at the early stage. R-GCN is initially optional/experimental and should only gain more ranking weight when evaluation metrics prove improvement.

---

## 2. Required folder structure

Create the following structure under `production/`:

```text
production/
├── README.md
├── .env.example
├── docker-compose.yml
├── Makefile
│
├── configs/
│   ├── base.yaml
│   ├── offline.yaml
│   └── online.yaml
│
├── data_contracts/
│   ├── befood_restaurants.schema.yaml
│   ├── befood_menu_items.schema.yaml
│   ├── foody_places.schema.yaml
│   ├── canonical_restaurants.schema.yaml
│   ├── canonical_menu_items.schema.yaml
│   └── user_events.schema.yaml
│
├── apps/
│   ├── api/
│   │   ├── main.py
│   │   ├── deps.py
│   │   ├── settings.py
│   │   ├── schemas.py
│   │   └── routers/
│   │       ├── health.py
│   │       ├── recommend.py
│   │       ├── restaurants.py
│   │       ├── feedback.py
│   │       └── admin.py
│   │
│   └── frontend/
│       ├── README.md
│       └── placeholder.txt
│
├── services/
│   ├── query_parser/
│   │   ├── __init__.py
│   │   ├── parser.py
│   │   └── rules.py
│   │
│   ├── graphrag_service/
│   │   ├── __init__.py
│   │   ├── service.py
│   │   ├── retriever.py
│   │   ├── ranker.py
│   │   └── context_builder.py
│   │
│   ├── rgcn_service/
│   │   ├── __init__.py
│   │   ├── service.py
│   │   ├── model_loader.py
│   │   └── scorer.py
│   │
│   ├── ranking_service/
│   │   ├── __init__.py
│   │   └── fusion.py
│   │
│   └── logging_service/
│       ├── __init__.py
│       ├── event_logger.py
│       └── event_schemas.py
│
├── pipelines/
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── offline_ingest.py
│   │   ├── online_ingest.py
│   │   └── source_registry.py
│   │
│   ├── validation/
│   │   ├── __init__.py
│   │   └── validate_csv.py
│   │
│   ├── normalization/
│   │   ├── __init__.py
│   │   ├── normalize_restaurants.py
│   │   ├── normalize_menu_items.py
│   │   └── deduplicate.py
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── lake.py
│   │   ├── postgres.py
│   │   └── minio_client.py
│   │
│   ├── kg_builder/
│   │   ├── __init__.py
│   │   ├── build_kg.py
│   │   ├── neo4j_writer.py
│   │   ├── kg_schema.py
│   │   └── export_snapshot.py
│   │
│   ├── vector_builder/
│   │   ├── __init__.py
│   │   └── build_vector_index.py
│   │
│   └── rgcn_exporter/
│       ├── __init__.py
│       └── export_rgcn_snapshot.py
│
├── training/
│   └── rgcn/
│       ├── README.md
│       ├── train.py
│       ├── evaluate.py
│       ├── build_behavior_graph.py
│       └── model_registry.py
│
├── dags/
│   ├── monthly_restaurant_ingestion.py
│   ├── daily_user_log_processing.py
│   ├── weekly_rgcn_training.py
│   └── evaluation_report.py
│
├── scripts/
│   ├── run_offline_ingest.py
│   ├── run_online_ingest.py
│   ├── build_kg.py
│   ├── build_indexes.py
│   ├── export_rgcn_snapshot.py
│   ├── train_rgcn.py
│   └── smoke_test.py
│
├── tests/
│   ├── test_query_parser.py
│   ├── test_validation.py
│   ├── test_normalization.py
│   ├── test_kg_builder.py
│   ├── test_rank_fusion.py
│   └── test_api.py
│
└── data_lake/
    ├── raw/
    ├── processed/
    ├── kg_snapshots/
    ├── user_events/
    ├── rgcn_snapshots/
    └── models/
```

Notes:

- `data_lake/` is a local fallback for offline mode.
- In online mode, replace or sync `data_lake/` with MinIO buckets.
- Do not remove the existing root notebooks or experimental `rgcn_pipeline/` folder. The new `production/` folder should wrap, reuse, or gradually replace logic from them.

---

## 3. Execution modes

The system must support exactly two high-level modes:

```text
APP_MODE=offline
APP_MODE=online
```

### 3.1 Offline mode

Offline mode must use existing local CSV files and local caches. It must not require crawler APIs, Kafka, Spark, or external object storage to work.

Offline source files:

```text
../Utils/befood_bachkhoa_restaurants.csv
../Utils/befood_bachkhoa_menu_items.csv
../Utils/foody_hust_places_from_store_csv.csv
../rgcn_pipeline/data/graphrag_nodes.csv
../rgcn_pipeline/data/graphrag_edges.csv
../rgcn_pipeline/data/store_metadata.csv
../rgcn_pipeline/user_scenarios_70_befood_bachkhoa.csv
../rgcn_pipeline/data/user_scenarios_phase2_top5.csv
```

Offline mode responsibilities:

1. Read the local CSV files.
2. Validate schemas.
3. Dump a copy into `production/data_lake/raw/offline/{run_id}/`.
4. Normalize and deduplicate restaurants/menu items/places.
5. Write canonical processed CSV into `production/data_lake/processed/{run_id}/`.
6. Build KG using logic extracted from the current GraphRAG notebook/code.
7. Write KG into Neo4j if available, or export a local KG snapshot if Neo4j is not running.
8. Build local vector index.
9. Export R-GCN snapshot.
10. Serve API using the local/cached KG and vector index.

Offline mode must be runnable by:

```bash
cd production
make offline-ingest
make build-kg MODE=offline
make build-indexes MODE=offline
make api MODE=offline
```

Or a single command:

```bash
cd production
make offline-all
```

### 3.2 Online mode

Online mode must use production infrastructure and optionally run crawlers/API data collectors.

Online mode responsibilities:

1. Run crawler/API ingestion.
2. Dump raw source data into MinIO or local data lake fallback.
3. Optionally publish ingestion events to Kafka.
4. Process data using Airflow DAGs.
5. Use Spark for heavier transforms if enabled.
6. Write canonical data to PostgreSQL.
7. Build/update Neo4j KG with versioning.
8. Build/update vector DB.
9. Serve API.
10. Log user behavior to PostgreSQL/Kafka.
11. Periodically aggregate user logs and create behavior graph snapshots.
12. Train/evaluate R-GCN offline.
13. Register a new R-GCN model only if it passes evaluation gates.
14. Update R-GCN serving artifact and rank-fusion weights only after successful evaluation.

Online mode must be runnable by:

```bash
cd production
make up
make online-ingest
make api MODE=online
```

---

## 4. Configuration files

### 4.1 `.env.example`

Create `production/.env.example`:

```bash
APP_MODE=offline
ENV=dev

# API
API_HOST=0.0.0.0
API_PORT=8000

# Storage
DATA_LAKE_ROOT=./data_lake
USE_MINIO=false
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_BUCKET=restaurant-prod

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=restaurant_prod
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
NEO4J_DATABASE=neo4j
USE_NEO4J=true

# Vector store
VECTOR_BACKEND=local
VECTOR_INDEX_PATH=./data_lake/vector_index
QDRANT_URL=http://localhost:6333

# Kafka
USE_KAFKA=false
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Spark
USE_SPARK=false
SPARK_MASTER=local[*]

# R-GCN
RGCN_MODEL_DIR=./data_lake/models/rgcn
RGCN_SNAPSHOT_DIR=./data_lake/rgcn_snapshots
RGCN_ENABLED=true
RGCN_DEFAULT_WEIGHT=0.10
GRAPHRAG_DEFAULT_WEIGHT=0.70
RULE_DEFAULT_WEIGHT=0.15
POPULARITY_DEFAULT_WEIGHT=0.05

# LLM
LLM_PROVIDER=none
LLM_MODEL=local-or-api-model-name
LLM_API_KEY=
```

### 4.2 `configs/base.yaml`

Create `production/configs/base.yaml`:

```yaml
app:
  name: restaurant-graphrag-rgcn-production
  default_mode: offline

ranking:
  default_strategy: hybrid
  weights:
    graphrag: 0.70
    rgcn: 0.10
    rule: 0.15
    popularity: 0.05
  evaluation_gate:
    min_top5_recall_gain: 0.05
    max_latency_ms: 3000
    require_non_decreasing_ctr: true

kg:
  version_prefix: kg
  use_versioned_graph: true
  active_version_file: ./data_lake/kg_snapshots/ACTIVE_VERSION

logging:
  save_raw_query: true
  save_inferred_rules: true
  save_manual_rules: true
  save_results_shown: true
  save_clicks: true
  save_feedback: true
```

### 4.3 `configs/offline.yaml`

Create `production/configs/offline.yaml`:

```yaml
mode: offline

sources:
  befood_restaurants: ../Utils/befood_bachkhoa_restaurants.csv
  befood_menu_items: ../Utils/befood_bachkhoa_menu_items.csv
  foody_places: ../Utils/foody_hust_places_from_store_csv.csv
  rgcn_nodes: ../rgcn_pipeline/data/graphrag_nodes.csv
  rgcn_edges: ../rgcn_pipeline/data/graphrag_edges.csv
  store_metadata: ../rgcn_pipeline/data/store_metadata.csv
  user_scenarios_v1: ../rgcn_pipeline/user_scenarios_70_befood_bachkhoa.csv
  user_scenarios_v2: ../rgcn_pipeline/data/user_scenarios_phase2_top5.csv

storage:
  use_minio: false
  data_lake_root: ./data_lake

neo4j:
  required: false
  fallback_to_local_snapshot: true

kafka:
  enabled: false

spark:
  enabled: false
```

### 4.4 `configs/online.yaml`

Create `production/configs/online.yaml`:

```yaml
mode: online

sources:
  enable_crawlers: true
  crawl_frequency: monthly

storage:
  use_minio: true
  data_lake_root: ./data_lake
  raw_bucket_prefix: raw
  processed_bucket_prefix: processed

neo4j:
  required: true
  fallback_to_local_snapshot: false

kafka:
  enabled: true
  topics:
    user_query_created: user.query.created
    user_rule_parsed: user.rule.parsed
    user_result_shown: user.result.shown
    user_restaurant_clicked: user.restaurant.clicked
    user_feedback_created: user.feedback.created
    rgcn_model_updated: rgcn.model.updated

spark:
  enabled: true

airflow:
  enabled: true
  monthly_ingestion_cron: "0 2 1 * *"
  daily_log_processing_cron: "0 3 * * *"
  weekly_rgcn_training_cron: "0 4 * * 0"
```

---

## 5. Data contracts

The production code must validate incoming CSV files before processing.

### 5.1 Minimal canonical restaurant schema

Canonical restaurant records should include at least:

```text
restaurant_id
name
normalized_name
address
normalized_address
latitude
longitude
phone
opening_hours
cuisine_type
price_min
price_max
rating
source_names
source_record_ids
created_at
updated_at
data_version
```

### 5.2 Minimal canonical menu item schema

```text
item_id
restaurant_id
name
normalized_name
description
price
category
availability
source_names
source_record_ids
created_at
updated_at
data_version
```

### 5.3 User event schema

The system must log both natural-language queries and structured rules.

```text
event_id
session_id
user_id_nullable
event_type
raw_query
manual_rules_json
inferred_rules_json
final_rules_json
rule_source
parse_confidence
algorithm_requested
algorithm_used
results_shown_json
clicked_restaurant_id
feedback_value
latency_ms
timestamp
```

Event types:

```text
query_created
rule_parsed
result_shown
restaurant_clicked
feedback_created
```

Important logging rules:

- Always save `raw_query` if the user typed a natural language query.
- Save `manual_rules_json` if the user used explicit filters.
- If the user only typed natural language, parse it into `inferred_rules_json` and save the inferred rules.
- Save `final_rules_json`, which is the actual rule object used for retrieval/ranking.
- For training, prefer manual rules over inferred rules.
- Do not write all raw user logs into the main Neo4j KG directly.

---

## 6. Ingestion pipeline implementation

### 6.1 Source registry

Implement `production/pipelines/ingestion/source_registry.py`.

Responsibilities:

- Load mode-specific source config.
- Resolve source paths.
- Check required files exist in offline mode.
- Register online crawler sources in online mode.

Pseudo-interface:

```python
class SourceRegistry:
    def __init__(self, config: dict): ...
    def get_sources(self) -> dict: ...
    def validate_sources_exist(self) -> None: ...
```

### 6.2 Offline ingestion

Implement `production/pipelines/ingestion/offline_ingest.py`.

Responsibilities:

1. Create a `run_id`, e.g. `offline_YYYYMMDD_HHMMSS`.
2. Read source CSV files from `../Utils/` and `../rgcn_pipeline/`.
3. Copy raw files to `production/data_lake/raw/offline/{run_id}/`.
4. Run schema validation.
5. Run normalization.
6. Write processed canonical CSV files to `production/data_lake/processed/{run_id}/`.
7. Write a manifest file:

```text
production/data_lake/processed/{run_id}/manifest.json
```

Manifest should include:

```json
{
  "run_id": "offline_20260602_120000",
  "mode": "offline",
  "sources": {...},
  "outputs": {...},
  "row_counts": {...},
  "created_at": "..."
}
```

### 6.3 Online ingestion

Implement `production/pipelines/ingestion/online_ingest.py`.

Responsibilities:

1. Run crawler/API data collection if enabled.
2. Write raw outputs to MinIO or local fallback.
3. Publish ingestion event if Kafka is enabled.
4. Validate, normalize, deduplicate.
5. Write canonical records to PostgreSQL.
6. Write processed snapshot to MinIO/local lake.

The online code should be robust even if Kafka/Spark are disabled. Use feature flags:

```python
USE_MINIO
USE_KAFKA
USE_SPARK
```

---

## 7. Normalization and deduplication

Implement normalization as deterministic Python functions first. Do not depend on LLM for core normalization.

### 7.1 Text normalization

Implement:

```python
def normalize_text(value: str) -> str:
    """Lowercase, strip spaces, normalize unicode, remove duplicate spaces."""
```

Also implement:

```python
def normalize_price(value) -> int | None

def normalize_address(value: str) -> str

def normalize_restaurant_name(value: str) -> str

def normalize_menu_item_name(value: str) -> str
```

### 7.2 Deduplication logic

Restaurant deduplication should use conservative rules:

- Same normalized name + same/close address → duplicate.
- Same source external ID → duplicate.
- Same name + very similar menu list → possible duplicate.

Output:

```text
canonical_restaurants.csv
canonical_menu_items.csv
source_records.csv
restaurant_aliases.csv
```

Keep source provenance. Do not drop source records.

---

## 8. KG design in Neo4j

### 8.1 Static KG nodes

Create nodes:

```text
(:Restaurant)
(:MenuItem)
(:Cuisine)
(:Location)
(:Landmark)
(:PriceRange)
(:Source)
(:FoodCategory)
(:Keyword)
```

Minimum properties:

```text
id
name
normalized_name
data_version
graph_version
source_names
created_at
updated_at
```

### 8.2 Static KG relationships

Create relationships:

```text
(:Restaurant)-[:HAS_ITEM]->(:MenuItem)
(:Restaurant)-[:LOCATED_IN]->(:Location)
(:Restaurant)-[:NEAR]->(:Landmark)
(:Restaurant)-[:HAS_CUISINE]->(:Cuisine)
(:MenuItem)-[:BELONGS_TO_CATEGORY]->(:FoodCategory)
(:MenuItem)-[:HAS_PRICE_RANGE]->(:PriceRange)
(:Restaurant)-[:FROM_SOURCE]->(:Source)
(:MenuItem)-[:FROM_SOURCE]->(:Source)
(:MenuItem)-[:HAS_KEYWORD]->(:Keyword)
```

### 8.3 Graph versioning

Every KG build must use a graph version:

```text
kg_YYYYMMDD_HHMMSS
```

Do not overwrite active production graph blindly.

Build flow:

```text
build new KG version
    ↓
run KG smoke tests
    ↓
if valid: mark active version
    ↓
API uses active version
```

For Neo4j, either:

1. Store `graph_version` property on all nodes/edges and filter by active version, or
2. Clear/reload graph only in offline/dev mode.

For production, prefer version property.

### 8.4 Reuse existing GraphRAG KG logic

Extract the KG construction logic from existing GraphRAG notebook/code into:

```text
production/pipelines/kg_builder/build_kg.py
```

Do not keep KG production logic only inside notebook.

The notebook can remain as documentation/experiment, but production pipeline must be scriptable.

---

## 9. GraphRAG service

Implement `production/services/graphrag_service/service.py`.

Responsibilities:

1. Receive final structured rules and raw query.
2. Retrieve candidates from Neo4j and vector store.
3. Apply GraphRAG ranker.
4. Build context for LLM if enabled.
5. Return candidate restaurants with scores and evidence.

Expected interface:

```python
class GraphRAGService:
    def recommend(self, query: str, rules: dict, top_k: int = 5) -> list[dict]:
        ...
```

Each returned item should include:

```json
{
  "restaurant_id": "...",
  "name": "...",
  "matched_items": [...],
  "graphrag_score": 0.0,
  "evidence": [
    {
      "source": "befood",
      "source_record_id": "...",
      "field": "menu_items",
      "value": "com ga"
    }
  ]
}
```

GraphRAG must be the default engine when the user chooses both GraphRAG and R-GCN or chooses `hybrid`.

---

## 10. Query parser: natural language to structured rules

Implement `production/services/query_parser/parser.py`.

The parser must map Vietnamese natural language queries into structured constraints.

Example input:

```text
tìm quán cơm rang gần Bách Khoa dưới 50k, ưu tiên rating cao
```

Expected output:

```json
{
  "food": "cơm rang",
  "location": "Bách Khoa",
  "max_price": 50000,
  "priority": ["rating", "distance"],
  "cuisine": null,
  "time_constraint": null,
  "confidence": 0.86
}
```

Implementation stages:

1. Start with deterministic/rule-based parser:
   - detect price: `dưới 50k`, `<= 50000`, `khoảng 30k`
   - detect location: `gần Bách Khoa`, `quanh HUST`, `gần trường`
   - detect food keywords from query
   - detect priority keywords: `rẻ`, `gần`, `rating cao`, `nhiều món`
2. Add optional LLM parser later.
3. Always return `confidence`.
4. If manual filters exist, merge them with inferred rules, where manual filters override inferred rules.

Pseudo-interface:

```python
class QueryParser:
    def parse(self, raw_query: str) -> dict: ...

    def merge_rules(self, inferred_rules: dict, manual_rules: dict | None) -> dict:
        ...
```

Rule precedence:

```text
manual_rules > inferred_rules > defaults
```

---

## 11. R-GCN service and training

### 11.1 R-GCN should not train directly on live Neo4j queries

Training flow:

```text
Neo4j active KG snapshot
    ↓
export static graph nodes/edges
    ↓
load user behavior snapshot
    ↓
build R-GCN training graph
    ↓
train R-GCN
    ↓
evaluate
    ↓
register model if better
    ↓
serve model artifact
```

### 11.2 R-GCN snapshot export

Implement:

```text
production/pipelines/rgcn_exporter/export_rgcn_snapshot.py
```

Outputs:

```text
production/data_lake/rgcn_snapshots/{snapshot_id}/
├── nodes.csv
├── edges.csv
├── node_types.json
├── edge_types.json
├── store_metadata.csv
├── behavior_edges.csv
└── manifest.json
```

The static graph can be exported from Neo4j or from the local KG builder output.

### 11.3 Behavior graph construction

Implement:

```text
production/training/rgcn/build_behavior_graph.py
```

Convert user events into edges:

```text
(:Session)-[:HAS_INTENT]->(:Intent)
(:Session)-[:USED_RULE]->(:Rule)
(:Session)-[:SHOWN]->(:Restaurant)
(:Session)-[:CLICKED]->(:Restaurant)
(:Session)-[:LIKED]->(:Restaurant)
(:Intent)-[:MATCHES_FOOD]->(:Keyword/MenuItem)
(:Intent)-[:MATCHES_PRICE]->(:PriceRange)
```

Training target:

```text
Given an intent/rule/session node, rank Restaurant nodes.
```

### 11.4 R-GCN serving interface

Implement `production/services/rgcn_service/service.py`:

```python
class RGCNService:
    def recommend(self, query: str, rules: dict, top_k: int = 5) -> list[dict]:
        ...
```

Output:

```json
{
  "restaurant_id": "...",
  "rgcn_score": 0.0,
  "reason": "matched learned behavior pattern"
}
```

If no trained model exists, return an empty list and do not crash API.

---

## 12. Rank fusion

Implement:

```text
production/services/ranking_service/fusion.py
```

Support three algorithm modes:

```text
graphrag
rgcn
hybrid
```

Rules:

1. If the user selects `graphrag`, only use GraphRAG.
2. If the user selects `rgcn`, use R-GCN if available; fallback to GraphRAG if no R-GCN model exists.
3. If the user selects `hybrid` or both algorithms, prioritize GraphRAG first and combine with R-GCN.

Initial default weights:

```text
graphrag: 0.70
rgcn: 0.10
rule: 0.15
popularity: 0.05
```

Fusion formula:

```text
final_score =
    w_graphrag * normalized_graphrag_score
  + w_rgcn * normalized_rgcn_score
  + w_rule * rule_match_score
  + w_popularity * popularity_score
```

Important:

- Do not automatically increase R-GCN weight after each training run.
- Increase R-GCN weight only if the new model passes evaluation gates.
- Keep the current active ranking config in PostgreSQL or `data_lake/models/rgcn/active_config.json` in offline mode.

---

## 13. Logging service

Implement:

```text
production/services/logging_service/event_logger.py
```

The logger must support both offline and online mode.

### 13.1 Offline logging

Write JSONL files:

```text
production/data_lake/user_events/{date}/events.jsonl
```

### 13.2 Online logging

Write to PostgreSQL and optionally Kafka.

Kafka topics:

```text
user.query.created
user.rule.parsed
user.result.shown
user.restaurant.clicked
user.feedback.created
```

### 13.3 What to log

For each recommendation request:

```json
{
  "event_type": "query_created",
  "session_id": "...",
  "user_id": null,
  "raw_query": "...",
  "manual_rules": {...},
  "inferred_rules": {...},
  "final_rules": {...},
  "rule_source": "manual_filter|nl_parser|mixed",
  "parse_confidence": 0.0,
  "algorithm_requested": "hybrid",
  "algorithm_used": "hybrid",
  "timestamp": "..."
}
```

For shown results:

```json
{
  "event_type": "result_shown",
  "session_id": "...",
  "results_shown": ["res_001", "res_002"],
  "scores": {...},
  "timestamp": "..."
}
```

For click:

```json
{
  "event_type": "restaurant_clicked",
  "session_id": "...",
  "restaurant_id": "res_001",
  "rank_position": 1,
  "timestamp": "..."
}
```

---

## 14. API design

Implement FastAPI under:

```text
production/apps/api/
```

### 14.1 Endpoints

```text
GET  /health
POST /recommend
GET  /restaurants/{restaurant_id}
POST /feedback
GET  /admin/active-version
POST /admin/reload-models
```

### 14.2 `POST /recommend`

Request:

```json
{
  "query": "quán cơm gà gần Bách Khoa dưới 50k",
  "manual_rules": {
    "max_price": 50000,
    "location": "Bách Khoa"
  },
  "algorithm": "hybrid",
  "top_k": 5,
  "session_id": "optional-session-id"
}
```

Response:

```json
{
  "session_id": "...",
  "query": "quán cơm gà gần Bách Khoa dưới 50k",
  "inferred_rules": {...},
  "final_rules": {...},
  "algorithm_used": "hybrid",
  "results": [
    {
      "restaurant_id": "res_001",
      "name": "...",
      "matched_items": [
        {
          "name": "Cơm gà",
          "price": 45000
        }
      ],
      "scores": {
        "final": 0.87,
        "graphrag": 0.91,
        "rgcn": 0.33,
        "rule": 1.0,
        "popularity": 0.2
      },
      "evidence": [...]
    }
  ],
  "latency_ms": 123
}
```

### 14.3 `POST /feedback`

Request:

```json
{
  "session_id": "...",
  "restaurant_id": "res_001",
  "feedback": "like|dislike|click|save",
  "rank_position": 1
}
```

---

## 15. Airflow DAGs

Airflow DAGs should live in:

```text
production/dags/
```

### 15.1 Monthly restaurant ingestion DAG

File:

```text
production/dags/monthly_restaurant_ingestion.py
```

Tasks:

```text
crawl_or_load_sources
    ↓
dump_raw_to_lake
    ↓
validate_sources
    ↓
normalize_and_deduplicate
    ↓
load_canonical_store
    ↓
build_neo4j_kg
    ↓
build_vector_index
    ↓
export_rgcn_static_snapshot
    ↓
run_smoke_tests
    ↓
mark_kg_version_active
```

### 15.2 Daily user log processing DAG

File:

```text
production/dags/daily_user_log_processing.py
```

Tasks:

```text
read_user_events
    ↓
clean_user_events
    ↓
aggregate_preferences
    ↓
build_behavior_edges
    ↓
save_behavior_snapshot
```

### 15.3 Weekly R-GCN training DAG

File:

```text
production/dags/weekly_rgcn_training.py
```

Tasks:

```text
load_active_kg_snapshot
    ↓
load_latest_behavior_snapshot
    ↓
build_training_graph
    ↓
train_rgcn
    ↓
evaluate_rgcn
    ↓
compare_with_current_model
    ↓
if_pass_gate_register_model
    ↓
if_pass_gate_update_ranking_weight
```

### 15.4 Evaluation report DAG

File:

```text
production/dags/evaluation_report.py
```

Tasks:

```text
sample_recent_queries
    ↓
evaluate_graphrag
    ↓
evaluate_rgcn
    ↓
evaluate_hybrid
    ↓
write_report_to_lake
```

---

## 16. Docker Compose

Create `production/docker-compose.yml`.

Minimum services:

```yaml
services:
  api:
    build:
      context: ..
      dockerfile: production/docker/Dockerfile.api
    env_file:
      - .env
    ports:
      - "8000:8000"
    depends_on:
      - postgres
      - neo4j

  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: restaurant_prod
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  neo4j:
    image: neo4j:5
    environment:
      NEO4J_AUTH: neo4j/password
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - neo4j_data:/data

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minio
      MINIO_ROOT_PASSWORD: minio123
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

  qdrant:
    image: qdrant/qdrant:latest
    ports:
      - "6333:6333"
    volumes:
      - qdrant_data:/qdrant/storage

volumes:
  postgres_data:
  neo4j_data:
  minio_data:
  qdrant_data:
```

Kafka, Spark, and Airflow can be added after the minimum services work.

---

## 17. Makefile

Create `production/Makefile`.

Required commands:

```makefile
MODE ?= offline

install:
	pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

offline-ingest:
	python scripts/run_offline_ingest.py --config configs/offline.yaml

online-ingest:
	python scripts/run_online_ingest.py --config configs/online.yaml

build-kg:
	python scripts/build_kg.py --mode $(MODE)

build-indexes:
	python scripts/build_indexes.py --mode $(MODE)

export-rgcn:
	python scripts/export_rgcn_snapshot.py --mode $(MODE)

train-rgcn:
	python scripts/train_rgcn.py --mode $(MODE)

api:
	APP_MODE=$(MODE) uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload

offline-all:
	$(MAKE) offline-ingest
	$(MAKE) build-kg MODE=offline
	$(MAKE) build-indexes MODE=offline
	$(MAKE) export-rgcn MODE=offline

smoke-test:
	python scripts/smoke_test.py --mode $(MODE)

test:
	pytest tests -q
```

---

## 18. Implementation order for Codex

Codex should implement in this order.

### Phase 1: Production skeleton

1. Create `production/` folder.
2. Create folder structure.
3. Create `.env.example` and config files.
4. Create README with quickstart commands.
5. Create Makefile.

### Phase 2: Offline pipeline first

1. Implement source registry.
2. Implement offline ingestion.
3. Implement CSV validation.
4. Implement normalization.
5. Implement deduplication.
6. Write processed canonical CSV.
7. Write manifest JSON.

Acceptance criteria:

```bash
cd production
make offline-ingest
```

must create:

```text
data_lake/raw/offline/{run_id}/
data_lake/processed/{run_id}/canonical_restaurants.csv
data_lake/processed/{run_id}/canonical_menu_items.csv
data_lake/processed/{run_id}/manifest.json
```

### Phase 3: KG builder

1. Extract/reimplement KG construction logic from GraphRAG code/notebook.
2. Create KG nodes/edges from canonical CSV.
3. Write to Neo4j if available.
4. Otherwise write local snapshots:

```text
data_lake/kg_snapshots/{kg_version}/nodes.csv
data_lake/kg_snapshots/{kg_version}/edges.csv
```

Acceptance criteria:

```bash
cd production
make build-kg MODE=offline
```

must produce KG snapshot and optionally Neo4j graph.

### Phase 4: Query parser + GraphRAG service

1. Implement Vietnamese rule parser.
2. Implement GraphRAG service wrapper.
3. Reuse existing retriever/ranker logic where possible.
4. Implement local fallback if Neo4j/vector DB is unavailable.

Acceptance criteria:

```bash
cd production
make api MODE=offline
```

Then:

```bash
curl -X POST http://localhost:8000/recommend \
  -H "Content-Type: application/json" \
  -d '{"query":"quán cơm gà gần Bách Khoa dưới 50k","algorithm":"graphrag","top_k":5}'
```

must return non-crashing JSON response.

### Phase 5: Logging service

1. Implement event logger.
2. In offline mode, write JSONL logs.
3. In online mode, write to PostgreSQL and optionally Kafka.
4. Log raw query, inferred rules, final rules, algorithm, results shown.

Acceptance criteria:

After calling `/recommend`, file exists:

```text
data_lake/user_events/{date}/events.jsonl
```

### Phase 6: R-GCN exporter and service

1. Export active KG snapshot into R-GCN format.
2. Build behavior graph from user logs.
3. Implement R-GCN service placeholder that loads current model if available.
4. If no model exists, it should return empty results and API should fallback to GraphRAG.

Acceptance criteria:

```bash
cd production
make export-rgcn MODE=offline
```

must create:

```text
data_lake/rgcn_snapshots/{snapshot_id}/nodes.csv
data_lake/rgcn_snapshots/{snapshot_id}/edges.csv
data_lake/rgcn_snapshots/{snapshot_id}/manifest.json
```

### Phase 7: Rank fusion

1. Implement GraphRAG-only mode.
2. Implement R-GCN-only mode with fallback.
3. Implement hybrid mode.
4. Use default weights.
5. Load active ranking config from config file or model registry.

Acceptance criteria:

`POST /recommend` must support:

```json
{"algorithm":"graphrag"}
{"algorithm":"rgcn"}
{"algorithm":"hybrid"}
```

### Phase 8: Online infrastructure

1. Add Docker Compose services.
2. Add online ingestion implementation.
3. Add PostgreSQL persistence.
4. Add MinIO support.
5. Add Kafka support behind feature flag.
6. Add Airflow DAGs.

Do not block offline mode if online dependencies are missing.

---

## 19. Important design constraints

1. Do not train R-GCN directly from live Neo4j at request time.
2. Do not write raw user behavior logs into the main static Neo4j KG.
3. Do not automatically increase R-GCN weight after every training run.
4. GraphRAG must remain the default recommendation engine at the start.
5. R-GCN should become more important only after passing evaluation gates.
6. Offline mode must work without Kafka, Spark, Airflow, MinIO, and PostgreSQL.
7. Online mode should support these services, but they should be configurable.
8. Every data run must have `run_id` or `data_version`.
9. Every KG build must have `kg_version`.
10. Every R-GCN model must have `model_version` and evaluation report.
11. Always preserve source provenance from raw CSV to final recommendation evidence.

---

## 20. Minimum tests

Create tests for:

```text
test_validation.py
- missing required columns
- invalid price values
- empty restaurant name

test_normalization.py
- Vietnamese text normalization
- price normalization: 50k → 50000
- address normalization

test_query_parser.py
- "dưới 50k" → max_price 50000
- "gần Bách Khoa" → location Bách Khoa
- "ưu tiên rẻ" → priority price

test_kg_builder.py
- canonical restaurant creates Restaurant node
- menu item creates HAS_ITEM edge
- source creates FROM_SOURCE edge

test_rank_fusion.py
- GraphRAG-only returns GraphRAG ranking
- R-GCN-only falls back if model missing
- Hybrid prioritizes GraphRAG with default weights

test_api.py
- /health returns ok
- /recommend returns JSON
- /feedback logs event
```

---

## 21. README quickstart content

Create `production/README.md` with at least:

```markdown
# Production Restaurant GraphRAG + R-GCN System

## Modes

- offline: use local CSV files and local data lake.
- online: use crawler/API ingestion, MinIO, PostgreSQL, Neo4j, optional Kafka/Spark/Airflow.

## Offline quickstart

```bash
cd production
cp .env.example .env
make install
make offline-all
make api MODE=offline
```

## Online quickstart

```bash
cd production
cp .env.example .env
make up
make online-ingest
make api MODE=online
```

## Test recommendation

```bash
curl -X POST http://localhost:8000/recommend \
  -H "Content-Type: application/json" \
  -d '{"query":"quán cơm gà gần Bách Khoa dưới 50k","algorithm":"hybrid","top_k":5}'
```
```

---

## 22. Final target behavior

After implementation, the system should support:

1. Running fully offline from existing CSV files.
2. Running online ingestion/crawling when configured.
3. Dumping raw data into local lake or MinIO.
4. Building canonical restaurant/menu data.
5. Building a versioned Neo4j KG.
6. Serving GraphRAG recommendation from natural language and rule filters.
7. Logging user behavior.
8. Converting logs into R-GCN training snapshots.
9. Training/evaluating R-GCN periodically.
10. Using GraphRAG as default and R-GCN as a gradually improving ranking component.

The most important engineering goal is separation of concerns:

```text
Crawler/ingestion does not directly serve user requests.
GraphRAG does not own raw data cleaning.
R-GCN does not directly depend on raw CSV files.
User logs do not pollute the static Neo4j KG.
API uses services with clear interfaces.
```

