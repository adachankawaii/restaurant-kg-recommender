from fastapi.testclient import TestClient

from apps.api.main import app
from pipelines.ingestion.offline_ingest import run_offline_ingest
from pipelines.kg_builder.build_kg import build_kg_snapshot
from pipelines.vector_builder.build_vector_index import build_local_vector_index
from settings import load_settings


def test_api_health_and_recommend():
    settings = load_settings(mode="offline")
    run_offline_ingest(settings)
    build_kg_snapshot(settings)
    build_local_vector_index(settings)
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    response = client.post("/recommend", json={"query": "tim quan com ga gan bach khoa", "algorithm": "hybrid", "top_k": 3})
    assert response.status_code == 200
