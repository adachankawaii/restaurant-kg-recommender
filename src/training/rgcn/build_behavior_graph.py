from __future__ import annotations

from pathlib import Path
import json

import pandas as pd


def build_behavior_edges(events_path: Path, output_path: Path) -> Path:
    rows = []
    if events_path.exists():
        with events_path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    rows.append(json.loads(line))

    edges = []
    for row in rows:
        session_id = row.get("session_id")
        event_type = row.get("event_type")
        if not session_id or not event_type:
            continue
        if event_type == "result_shown":
            for restaurant_id in row.get("results_shown_json", []):
                edges.append({"src_id": f"session:{session_id}", "relation": "SHOWN", "dst_id": f"restaurant:{restaurant_id}"})
        if event_type == "feedback_created" and row.get("feedback_value") == "like" and row.get("clicked_restaurant_id"):
            edges.append({"src_id": f"session:{session_id}", "relation": "LIKED", "dst_id": f"restaurant:{row['clicked_restaurant_id']}"})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(edges).to_csv(output_path, index=False, encoding="utf-8-sig")
    return output_path
