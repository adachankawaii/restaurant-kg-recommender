from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient

from apps.api.main import app
from pipelines.ingestion.offline_ingest import run_offline_ingest
from pipelines.kg_builder.build_kg import build_kg_snapshot
from pipelines.rgcn_exporter.export_rgcn_snapshot import export_rgcn_snapshot
from pipelines.vector_builder.build_vector_index import build_local_vector_index
from settings import load_settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="offline")
    args = parser.parse_args()
    if args.mode != "offline":
        raise SystemExit("Only offline smoke testing is executed in this implementation.")

    settings = load_settings(mode="offline")
    run_offline_ingest(settings)
    build_kg_snapshot(settings)
    build_local_vector_index(settings)
    export_rgcn_snapshot(settings)

    client = TestClient(app)
    assert client.get("/health").status_code == 200
    response = client.post(
        "/recommend",
        json={
            "query": "tim quan com rang gan bach khoa duoi 50k rating cao",
            "algorithm": "hybrid",
            "top_k": 3,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["results"] is not None
    print({"status": "ok", "results": len(payload["results"])})


if __name__ == "__main__":
    main()
