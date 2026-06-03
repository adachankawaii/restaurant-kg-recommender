from pathlib import Path

from pipelines.ingestion.offline_ingest import run_offline_ingest
from pipelines.kg_builder.build_kg import build_kg_snapshot
from settings import load_settings


def test_build_kg_snapshot():
    settings = load_settings(mode="offline")
    run_offline_ingest(settings)
    result = build_kg_snapshot(settings)
    assert result["node_count"] > 0
    assert result["edge_count"] > 0
