from __future__ import annotations


def build_context(query: str, candidates: list[dict]) -> dict:
    return {
        "query": query,
        "candidate_count": len(candidates),
        "top_names": [item.get("name") for item in candidates[:5]],
    }
