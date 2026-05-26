from __future__ import annotations

from typing import Any, Optional, Protocol

from config import AppConfig
from ranker import infer_geo_intent, safe_float, validate_post_fusion


class PairScorer(Protocol):
    def compute_score(self, pairs: list[list[str]], normalize: bool = False) -> Any: ...


def minmax_normalize(values: list[float]) -> list[float]:
    if not values:
        return []
    lo, hi = min(values), max(values)
    if abs(hi - lo) < 1e-9:
        return [0.5 for _ in values]
    return [(v - lo) / (hi - lo) for v in values]


def build_cross_encoder_passage(candidate: dict) -> str:
    evidence = " | ".join((candidate.get("evidence") or [])[:2])
    attrs = ", ".join(
        f"{a.get('type')}={safe_float(a.get('score')):+.2f}"
        for a in (candidate.get("attributes") or [])
        if a and a.get("score") is not None
    )
    extracted = ", ".join(
        f"{e.get('type')}:{e.get('name')}"
        for e in (candidate.get("extracted_entities") or [])
        if isinstance(e, dict) and e.get("name")
    )
    parts = [
        f"Name: {candidate.get('name')}",
        f"Address: {candidate.get('address')}",
        f"District: {candidate.get('district')}, {candidate.get('city')}",
        f"Rating: {candidate.get('rating')}",
        f"Distance_km: {candidate.get('distance_km')}",
        f"Price_band: {candidate.get('price_band')}",
        f"Menu_price: {candidate.get('menu_price_min')} - {candidate.get('menu_price_max')}, median={candidate.get('menu_price_median')}",
        f"Categories: {', '.join(candidate.get('categories') or [])}",
        f"Cuisines: {', '.join(candidate.get('cuisines') or [])}",
        f"Dish_families: {', '.join(candidate.get('dish_families') or [])}",
        f"Top_menu_items: {', '.join(candidate.get('top_menu_items') or [])}",
        f"Attributes: {attrs}",
        f"Extracted_entities: {extracted}",
        f"Community_report: {candidate.get('community_report') or ''}",
        f"Evidence: {evidence}",
    ]
    clean = [p for p in parts if p and not p.endswith("None") and not p.endswith(": ")]
    return "\n".join(clean)[:1200]


class CrossEncoderReranker:
    def __init__(
        self,
        config: AppConfig,
        scorer: Optional[PairScorer] = None,
        enabled: Optional[bool] = None,
        ce_weight: Optional[float] = None,
    ):
        self.config = config
        self.scorer = scorer
        self.enabled = config.use_cross_encoder if enabled is None else enabled
        self.ce_weight = config.cross_encoder_weight if ce_weight is None else ce_weight

    def _get_scorer(self) -> Optional[PairScorer]:
        if not self.enabled:
            return None
        if self.scorer is not None:
            return self.scorer
        try:
            import torch
            from FlagEmbedding import FlagReranker
        except ImportError:
            return None
        self.scorer = FlagReranker(self.config.cross_encoder_model, use_fp16=torch.cuda.is_available())
        return self.scorer

    @staticmethod
    def _as_float_list(raw_scores: Any, n: int) -> list[float]:
        if hasattr(raw_scores, "tolist"):
            raw_scores = raw_scores.tolist()
        if isinstance(raw_scores, tuple):
            raw_scores = list(raw_scores)
        if not isinstance(raw_scores, list):
            raw_scores = [raw_scores] * n
        return [float(x) for x in raw_scores[:n]]

    def rerank(
        self,
        query: str,
        candidates: list[dict],
        top_k: int,
        intent: Optional[dict] = None,
        max_distance_km: Optional[float] = None,
    ) -> list[dict]:
        intent = intent or {}
        candidates = validate_post_fusion(candidates, intent, query, max_distance_km=max_distance_km)
        scorer = self._get_scorer()
        if scorer is None or not candidates:
            return candidates[:top_k]

        passages = [build_cross_encoder_passage(candidate) for candidate in candidates]
        raw = scorer.compute_score([[query, passage] for passage in passages], normalize=False)
        raw_scores = self._as_float_list(raw, len(candidates))
        ce_scores = minmax_normalize(raw_scores)

        for candidate, raw_score, ce_score in zip(candidates, raw_scores, ce_scores):
            base_score = float(candidate.get("final_score") or 0.0)
            candidate["ce_raw_score"] = round(raw_score, 4)
            candidate["ce_score"] = round(float(ce_score), 4)
            candidate["final_score_before_ce"] = base_score
            candidate["final_score"] = (1.0 - self.ce_weight) * base_score + self.ce_weight * float(ce_score)

        if infer_geo_intent(query, intent) == "nearest":
            candidates.sort(key=lambda x: (x.get("distance_km") is None, safe_float(x.get("distance_km"), 1e9) or 1e9, -(safe_float(x.get("final_score")) or 0.0)))
        else:
            candidates.sort(key=lambda x: safe_float(x.get("final_score")) or 0.0, reverse=True)
        return candidates[:top_k]
