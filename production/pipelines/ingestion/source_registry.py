from __future__ import annotations

from pathlib import Path


class SourceRegistry:
    def __init__(self, root: Path, config: dict):
        self.root = root
        self.config = config

    def get_sources(self) -> dict[str, str]:
        return dict(self.config.get("sources", {}))

    def resolve_sources(self) -> dict[str, Path]:
        resolved: dict[str, Path] = {}
        for key, value in self.get_sources().items():
            if isinstance(value, str):
                resolved[key] = (self.root / value).resolve()
        return resolved

    def validate_sources_exist(self) -> None:
        if self.config.get("mode") != "offline":
            return
        missing = [str(path) for path in self.resolve_sources().values() if not path.exists()]
        if missing:
            raise FileNotFoundError("Missing offline sources:\n" + "\n".join(missing))

