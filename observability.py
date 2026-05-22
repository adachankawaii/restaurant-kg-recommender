from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class RetrievalTrace:
    query: str
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: float = field(default_factory=time.time)
    intent: Optional[dict[str, Any]] = None
    candidates: list[dict[str, Any]] = field(default_factory=list)
    prompt_context: Optional[str] = None
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def add_candidates(self, rows: list[dict[str, Any]]) -> None:
        self.candidates = [
            {
                "store_key": r.get("store_key"),
                "name": r.get("name"),
                "source_flags": r.get("source_flags"),
                "distance_km": r.get("distance_km"),
                "rating": r.get("rating"),
                "rrf_score": r.get("rrf_score"),
                "graph_rank_score": r.get("graph_rank_score"),
                "neighbor_score": r.get("neighbor_score"),
                "restaurant_vec_score": r.get("restaurant_vec_score"),
                "text_unit_vec_score": r.get("text_unit_vec_score"),
                "distance_score": r.get("distance_score"),
                "final_score": r.get("final_score"),
            }
            for r in rows
        ]

    def finish(self) -> None:
        self.metrics["latency_ms"] = round((time.time() - self.started_at) * 1000, 2)


class JsonlTraceLogger:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, trace: RetrievalTrace) -> None:
        trace.finish()
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(trace), ensure_ascii=False, default=str) + "\n")

