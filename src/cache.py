from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Optional


def stable_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class JsonDiskCache:
    def __init__(self, cache_dir: Path, namespace: str):
        self.path = Path(cache_dir) / f"{namespace}.json"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict[str, Any] = {}
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                self._data = {}

    def get(self, key: str) -> Optional[Any]:
        return self._data.get(key)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get_or_compute(self, key_payload: Any, fn: Callable[[], Any]) -> Any:
        key = stable_hash(key_payload)
        cached = self.get(key)
        if cached is not None:
            return cached
        value = fn()
        self.set(key, value)
        return value

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)

