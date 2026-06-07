from __future__ import annotations

from pathlib import Path

import pandas as pd

from common import dump_json, latest_complete_dir, load_json, utc_now_iso
from settings import Settings


def _read_optional_csv(path):
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _active_graph_version(settings: Settings) -> str:
    return (settings.paths.kg_root / "ACTIVE_VERSION").read_text(encoding="utf-8").strip()


def build_local_vector_index(settings: Settings) -> dict[str, object]:
    graph_version = _active_graph_version(settings)
    processed_dir = latest_complete_dir(
        settings.paths.processed_root,
        ["canonical_restaurants.csv", "canonical_menu_items.csv"],
        "processed ingestion output",
    )
    restaurants = pd.read_csv(processed_dir / "canonical_restaurants.csv")
    text_units = _read_optional_csv(processed_dir / "text_units.csv")
    community_reports = _read_optional_csv(processed_dir / "community_reports.csv")

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
