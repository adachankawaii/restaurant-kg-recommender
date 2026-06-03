from __future__ import annotations

import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import dump_json, ensure_dir
from pipelines.storage.minio_client import MinioStorageAdapter
from settings import load_settings


def _clear_staging(target: Path, allowed_root: Path) -> None:
    target = target.resolve()
    allowed_root = allowed_root.resolve()
    if allowed_root not in target.parents and target != allowed_root:
        raise RuntimeError(f"Refusing to clear staging outside data lake: {target}")
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)


def main() -> None:
    settings = load_settings(mode="online")
    staging_root = ensure_dir(settings.paths.data_lake_root / "minio_staging")
    target = staging_root / "user_scenarios"
    _clear_staging(target, staging_root)

    adapter = MinioStorageAdapter()
    result = adapter.download_prefix("user_scenarios", target)
    manifest = {
        "created_at": datetime.now(UTC).isoformat(),
        "source": "minio",
        "bucket": adapter.bucket,
        "prefix": "user_scenarios",
        "target": str(target),
        "download_result": result,
    }
    dump_json(staging_root / "user_scenarios_sync_manifest.json", manifest)
    print(manifest)
    if result.get("status") != "ok":
        raise RuntimeError(f"Failed to sync user scenarios from MinIO: {result}")


if __name__ == "__main__":
    main()
