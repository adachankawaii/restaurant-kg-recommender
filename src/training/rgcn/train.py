from __future__ import annotations

import subprocess
import sys
import csv
from pathlib import Path

from common import dump_json, make_run_id, utc_now_iso
from settings import Settings
from training.rgcn.model_registry import register_model


def _latest_rgcn_snapshot(settings: Settings) -> Path:
    candidates = [path for path in settings.paths.rgcn_root.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError("No R-GCN snapshot found. Run export-rgcn first.")
    return sorted(candidates)[-1]


def _query_dataset_stats(path: Path) -> tuple[int, int]:
    if not path.exists() or path.stat().st_size == 0:
        return 0, 0
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        rows = 0
        query_ids: set[str] = set()
        for row in reader:
            query_id = row.get("query_node_id") or row.get("query_id") or ""
            store_id = row.get("store_node_id") or row.get("store_id") or ""
            if query_id and store_id:
                rows += 1
                query_ids.add(query_id)
    return rows, len(query_ids)


def _latest_query_dataset(settings: Settings) -> tuple[Path, dict[str, object]]:
    fallback = settings.root.parent / "src" / "rgcn_pipeline" / "data" / "user_scenarios_phase2_top5.csv"
    training_set_dir = settings.paths.data_lake_root / "rgcn_training_sets"
    candidates = sorted(training_set_dir.glob("user_scenarios_labeled_*.csv")) if training_set_dir.exists() else []
    for candidate in reversed(candidates):
        rows, unique_queries = _query_dataset_stats(candidate)
        if rows > 0 and unique_queries >= 2:
            return candidate, {
                "source": "user_click_logs",
                "rows": rows,
                "unique_queries": unique_queries,
                "fallback_used": False,
            }

    rows, unique_queries = _query_dataset_stats(fallback)
    return fallback, {
        "source": "sample_user_scenarios",
        "rows": rows,
        "unique_queries": unique_queries,
        "fallback_used": True,
        "fallback_reason": "Need at least 2 clicked user queries for supervised train/eval split.",
    }


def train_rgcn(settings: Settings) -> dict:
    snapshot_dir = _latest_rgcn_snapshot(settings)
    run_id = make_run_id("rgcn_train")
    output_dir = settings.paths.models_root / "rgcn" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    query_path, query_dataset = _latest_query_dataset(settings)
    command = [
        sys.executable,
        str(settings.root.parent / "src" / "rgcn_pipeline" / "main.py"),
        "--nodes",
        str(snapshot_dir / "nodes.csv"),
        "--edges",
        str(snapshot_dir / "edges.csv"),
        "--store-metadata",
        str(snapshot_dir / "store_metadata.csv"),
        "--queries",
        str(query_path),
        "--outputs",
        str(output_dir),
        "--epochs",
        "5",
        "--query-epochs",
        "5",
        "--add-reverse-edges",
    ]
    result = subprocess.run(command, cwd=str(settings.root.parent), text=True, capture_output=True, timeout=1800)
    dump_json(
        output_dir / "training_invocation.json",
        {
            "run_id": run_id,
            "command": command,
            "query_dataset": query_dataset,
            "returncode": result.returncode,
            "stdout_tail": result.stdout[-4000:],
            "stderr_tail": result.stderr[-4000:],
            "created_at": utc_now_iso(),
        },
    )
    if result.returncode != 0:
        raise RuntimeError(f"R-GCN training failed. See {output_dir / 'training_invocation.json'}")

    artifact_path = output_dir / "query_ranker.pt"
    metrics = {"trained": 1.0, "registered_at_epoch_budget": 5.0}
    return register_model(settings.paths.models_root, str(artifact_path), metrics)
