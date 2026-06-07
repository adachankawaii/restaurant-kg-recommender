from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import dump_json, ensure_dir
from settings import load_settings


def _unique_query_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    column = "query_node_id" if "query_node_id" in frame.columns else "query_id"
    if column not in frame.columns:
        return 0
    return int(frame[column].dropna().astype(str).nunique())


def main() -> None:
    settings = load_settings(mode="online")
    scenario_root = settings.paths.data_lake_root / "user_scenarios"
    rows = []
    for path in scenario_root.glob("*/labeled_scenarios.csv"):
        rows.append(pd.read_csv(path))
    output_dir = ensure_dir(settings.paths.data_lake_root / "rgcn_training_sets")
    output_path = output_dir / f"user_scenarios_labeled_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.csv"
    metadata = {
        "created_at": datetime.now(UTC).isoformat(),
        "source": "clicked_user_scenarios",
        "clicked_rows": 0,
        "bootstrap_rows": 0,
        "unique_clicked_queries": 0,
        "output_rows": 0,
    }
    if rows:
        clicked = pd.concat(rows, ignore_index=True)
        metadata["clicked_rows"] = int(len(clicked))
        metadata["unique_clicked_queries"] = _unique_query_count(clicked)
        output = clicked
        if metadata["unique_clicked_queries"] < 2:
            bootstrap_path = settings.root.parent / "src" / "rgcn_pipeline" / "data" / "user_scenarios_phase2_top5.csv"
            bootstrap = pd.read_csv(bootstrap_path)
            metadata["source"] = "clicked_user_scenarios_with_bootstrap_sample"
            metadata["bootstrap_rows"] = int(len(bootstrap))
            output = pd.concat([clicked, bootstrap], ignore_index=True, sort=False)
        metadata["output_rows"] = int(len(output))
        output.to_csv(output_path, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(output_path, index=False, encoding="utf-8-sig")
    dump_json(output_path.with_suffix(".metadata.json"), metadata)
    print({"training_set": str(output_path), **metadata})


if __name__ == "__main__":
    main()
