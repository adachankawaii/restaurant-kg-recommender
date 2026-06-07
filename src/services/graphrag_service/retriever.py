from __future__ import annotations

import json
from pathlib import Path


class LocalVectorRetriever:
    def __init__(self, index_path: Path):
        payload = json.loads(index_path.read_text(encoding="utf-8"))
        self.records = payload.get("records", [])

    def search(self, query: str, top_k: int = 5) -> list[dict]:
        query_terms = {term for term in query.lower().split() if term}
        scored = []
        for record in self.records:
            text_terms = set(str(record.get("text", "")).lower().split())
            overlap = len(query_terms & text_terms)
            scored.append({**record, "graphrag_score": float(overlap)})
        scored.sort(key=lambda row: row["graphrag_score"], reverse=True)
        return scored[:top_k]
