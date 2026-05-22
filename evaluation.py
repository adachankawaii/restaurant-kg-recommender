from __future__ import annotations

import math
import time
from typing import Callable, Optional

import pandas as pd


def recall_at_k(retrieved: list[str], relevant: list[str], k: int) -> float:
    rel = set(relevant)
    if not rel:
        return 0.0
    return len(rel.intersection(retrieved[:k])) / len(rel)


def mrr_at_k(retrieved: list[str], relevant: list[str], k: int) -> float:
    rel = set(relevant)
    for i, sid in enumerate(retrieved[:k], start=1):
        if sid in rel:
            return 1.0 / i
    return 0.0


def ndcg_at_k(retrieved: list[str], relevant: list[str], k: int) -> float:
    rel = set(relevant)
    dcg = 0.0
    for i, sid in enumerate(retrieved[:k], start=1):
        gain = 1.0 if sid in rel else 0.0
        dcg += gain / math.log2(i + 1)
    ideal_hits = min(len(rel), k)
    idcg = sum(1.0 / math.log2(i + 1) for i in range(1, ideal_hits + 1))
    return dcg / idcg if idcg > 0 else 0.0


def evaluate_retriever(
    test_cases: list[dict],
    retrieve_fn: Callable[[str, int], list[str]],
    k: int = 5,
    cost_by_query: Optional[dict[str, float]] = None,
) -> pd.DataFrame:
    rows = []
    cost_by_query = cost_by_query or {}
    for tc in test_cases:
        query = tc["query"]
        start = time.perf_counter()
        retrieved = retrieve_fn(query, k)
        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        relevant = tc.get("relevant_store_keys", [])
        rows.append({
            "query": query,
            "recall_at_k": recall_at_k(retrieved, relevant, k),
            "mrr_at_k": mrr_at_k(retrieved, relevant, k),
            "ndcg_at_k": ndcg_at_k(retrieved, relevant, k),
            "latency_ms": latency_ms,
            "cost": cost_by_query.get(query, 0.0),
            "retrieved": retrieved,
            "relevant": relevant,
        })
    return pd.DataFrame(rows)

