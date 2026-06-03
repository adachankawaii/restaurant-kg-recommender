from __future__ import annotations

from pathlib import Path

import pandas as pd

from common import dump_json, load_json, utc_now_iso
from settings import Settings


def _active_graph_version(settings: Settings) -> str:
    return (settings.paths.kg_root / "ACTIVE_VERSION").read_text(encoding="utf-8").strip()


def build_local_vector_index(settings: Settings) -> dict[str, object]:
    graph_version = _active_graph_version(settings)
    processed_dirs = [path for path in settings.paths.processed_root.iterdir() if path.is_dir()]
    if not processed_dirs:
        raise FileNotFoundError("No processed directories found for vector build.")
    processed_dir = sorted(processed_dirs)[-1]
    restaurants = pd.read_csv(processed_dir / "canonical_restaurants.csv")
    text_units = pd.read_csv(processed_dir / "text_units.csv").fillna("") if (processed_dir / "text_units.csv").exists() else pd.DataFrame()
    community_reports = pd.read_csv(processed_dir / "community_reports.csv").fillna("") if (processed_dir / "community_reports.csv").exists() else pd.DataFrame()

    records = []
    for _, row in restaurants.iterrows():
        text = " ".join(
            [
                str(row.get("name", "")),
                str(row.get("cuisine_type", "")),
                str(row.get("address", "")),
                str(row.get("categories", "")),
            ]
        ).strip()
        records.append(
            {
                "restaurant_id": str(row["restaurant_id"]),
                "text": text,
                "source_type": "restaurant",
                "graph_version": graph_version,
            }
        )
    for _, row in text_units.iterrows():
        records.append(
            {
                "restaurant_id": str(row.get("store_key", "")),
                "text": str(row.get("chunk_text", "")),
                "text_unit_id": str(row.get("text_unit_id", "")),
                "source_type": "text_unit",
                "graph_version": graph_version,
            }
        )
    for _, row in community_reports.iterrows():
        records.append(
            {
                "restaurant_id": "",
                "text": f"{row.get('title', '')} {row.get('summary', '')}",
                "source_type": "community_report",
                "graph_version": graph_version,
            }
        )

    index_dir = settings.paths.data_lake_root / "vector_index"
    index_dir.mkdir(parents=True, exist_ok=True)
    index_path = index_dir / f"{graph_version}.json"
    dump_json(index_path, {"graph_version": graph_version, "records": records, "created_at": utc_now_iso()})
    dump_json(index_dir / "ACTIVE_INDEX.json", {"graph_version": graph_version, "index_path": str(index_path)})
    return load_json(index_path, {})
