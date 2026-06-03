from __future__ import annotations

import shutil
from pathlib import Path

from common import ensure_dir


def copy_to_lake(source: Path, target_dir: Path) -> Path:
    ensure_dir(target_dir)
    target = target_dir / source.name
    shutil.copy2(source, target)
    return target
