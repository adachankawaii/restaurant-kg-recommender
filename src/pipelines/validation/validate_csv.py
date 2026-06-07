from __future__ import annotations

from pathlib import Path

import pandas as pd

from common import simple_yaml_load


def validate_csv_against_schema(csv_path: Path, schema_path: Path) -> dict[str, object]:
    df = pd.read_csv(csv_path)
    schema = simple_yaml_load(schema_path)
    required = schema.get("required", [])
    missing = [column for column in required if column not in df.columns]
    return {
        "file": str(csv_path),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "missing_required_columns": missing,
        "valid": not missing,
    }
