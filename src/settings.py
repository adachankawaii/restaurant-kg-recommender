from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from common import RuntimePaths, deep_merge, ensure_dir, env_bool, simple_yaml_load


@dataclass(frozen=True)
class Settings:
    root: Path
    mode: str
    config: dict[str, Any]
    paths: RuntimePaths
    api_host: str
    api_port: int
    use_neo4j: bool
    use_minio: bool
    use_kafka: bool
    use_spark: bool


def load_settings(mode: str | None = None, config_name: str | None = None) -> Settings:
    root = Path(__file__).resolve().parent.parent / "production"
    load_dotenv(root / ".env", override=False)
    load_dotenv(root / ".env.example", override=False)

    selected_mode = mode or os.getenv("APP_MODE", "offline")
    selected_config = config_name or f"{selected_mode}.yaml"

    base_config = simple_yaml_load(root / "configs" / "base.yaml")
    mode_config = simple_yaml_load(root / "configs" / selected_config)
    merged = deep_merge(base_config, mode_config)

    data_lake_root = (root / merged.get("storage", {}).get("data_lake_root", "./data_lake")).resolve()
    paths = RuntimePaths(
        root=root,
        data_lake_root=data_lake_root,
        raw_root=ensure_dir(data_lake_root / "raw"),
        processed_root=ensure_dir(data_lake_root / "processed"),
        kg_root=ensure_dir(data_lake_root / "kg_snapshots"),
        user_events_root=ensure_dir(data_lake_root / "user_events"),
        rgcn_root=ensure_dir(data_lake_root / "rgcn_snapshots"),
        models_root=ensure_dir(data_lake_root / "models"),
    )

    return Settings(
        root=root,
        mode=selected_mode,
        config=merged,
        paths=paths,
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("API_PORT", "8000")),
        use_neo4j=env_bool("USE_NEO4J", merged.get("neo4j", {}).get("required", False)),
        use_minio=env_bool("USE_MINIO", merged.get("storage", {}).get("use_minio", False)),
        use_kafka=env_bool("USE_KAFKA", merged.get("kafka", {}).get("enabled", False)),
        use_spark=env_bool("USE_SPARK", merged.get("spark", {}).get("enabled", False)),
    )
