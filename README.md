# Restaurant Recommender

A restaurant recommendation system for the Bach Khoa area built with GraphRAG, Neo4j, Qdrant, and R-GCN.

## Features

- Natural-language and hard-feature restaurant recommendations
- Graph retrieval, vector search, reranking, and distance-aware scoring
- Web UI, API, and monitoring dashboard
- Pipeline for ingestion, KG build, index build, and snapshot export

## Project Layout

- `src/`: API, pipelines, services, and training code
- `tests/`: unit and regression tests
- `Utils/`: input CSV files
- `production/`: production/offline configs and workflow

## Requirements

- Python 3.9+
- Docker Desktop
- Neo4j 5.x
- Qdrant

## Installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

## Configuration

Key `.env` values:

```env
DATA_ROOT=./Utils
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password123
QDRANT_HOST=localhost
QDRANT_PORT=6333
OPENAI_API_KEY=your_key
OPENAI_MODEL=gpt-4o-mini
```

For OpenRouter or another OpenAI-compatible endpoint:

```env
OPENAI_BASE_URL=https://openrouter.ai/api/v1
```

## Run the System

Start the full stack:

```powershell
.\start_system.ps1
```

After startup:

- UI: `http://127.0.0.1:8000/`
- Monitoring: `http://127.0.0.1:8000/monitoring`
- API docs: `http://127.0.0.1:8000/docs`
- Health: `http://127.0.0.1:8000/health`


## Test

```powershell
python -m pytest tests -q
```

## Data

- Input CSVs: `Utils/`
- KG snapshots: `production/data_lake/kg_snapshots/`
- Intermediate artifacts: `production/data_lake/processed/`
- R-GCN models: `production/data_lake/models/rgcn/`


### Contributor: Minh-Tung Nguyen, Ngoc-Anh Nguyen, Hoang Le, Ngoc-Anh Nguyen, Duc-Manh Tran