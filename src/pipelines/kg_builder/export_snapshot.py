from __future__ import annotations

from pathlib import Path

import pandas as pd


def write_snapshot_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    return path
