from __future__ import annotations

from pathlib import Path
import shutil

from common import dump_json, make_run_id, utc_now_iso
from settings import Settings


def export_rgcn_snapshot(settings: Settings) -> dict[str, object]:
    snapshot_id = make_run_id("rgcn_snapshot")
    snapshot_dir = settings.paths.rgcn_root / snapshot_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    source_map = {
        "nodes.csv": settings.root.parent / "rgcn_pipeline" / "data" / "graphrag_nodes.csv",
        "edges.csv": settings.root.parent / "rgcn_pipeline" / "data" / "graphrag_edges.csv",
        "store_metadata.csv": settings.root.parent / "rgcn_pipeline" / "data" / "store_metadata.csv",
        "behavior_edges.csv": settings.root / "data_lake" / "user_events" / "behavior_edges.csv",
    }

    exported = {}
    for name, source in source_map.items():
        if source.exists():
            target = snapshot_dir / name
            shutil.copy2(source, target)
            exported[name] = str(target)

    manifest = {
        "snapshot_id": snapshot_id,
        "mode": settings.mode,
        "files": exported,
        "created_at": utc_now_iso(),
    }
    dump_json(snapshot_dir / "manifest.json", manifest)
    return manifest
