from __future__ import annotations

from pathlib import Path

from common import dump_json, load_json, utc_now_iso


def register_model(models_root: Path, artifact_path: str, metrics: dict[str, float]) -> dict:
    registry_path = models_root / "rgcn" / "active_config.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "artifact_path": artifact_path,
        "metrics": metrics,
        "registered_at": utc_now_iso(),
    }
    dump_json(registry_path, payload)
    return payload


def load_active_model_config(models_root: Path) -> dict:
    return load_json(models_root / "rgcn" / "active_config.json", {})
