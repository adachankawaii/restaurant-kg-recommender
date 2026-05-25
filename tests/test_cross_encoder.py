from __future__ import annotations

from pathlib import Path

from config import load_config
from cross_encoder import CrossEncoderReranker, build_cross_encoder_passage, minmax_normalize


class FakeScorer:
    def compute_score(self, pairs, normalize=False):
        return [0.1, 0.9]


def test_minmax_normalize_per_query():
    assert minmax_normalize([2.0, 4.0, 6.0]) == [0.0, 0.5, 1.0]
    assert minmax_normalize([3.0, 3.0]) == [0.5, 0.5]


def test_cross_encoder_passage_contains_ranking_features():
    passage = build_cross_encoder_passage({
        "name": "Quan A",
        "price_band": "budget",
        "distance_km": 0.5,
        "categories": ["Cơm"],
        "dish_families": ["cơm gà"],
        "top_menu_items": ["Cơm gà xối mỡ"],
        "evidence": ["đồ ăn ngon"],
    })
    assert "Price_band: budget" in passage
    assert "Distance_km: 0.5" in passage
    assert "Dish_families: cơm gà" in passage
    assert "Evidence: đồ ăn ngon" in passage


def test_cross_encoder_rerank_blends_scores_without_loading_model():
    config = load_config(Path.cwd())
    reranker = CrossEncoderReranker(config, scorer=FakeScorer(), enabled=True, ce_weight=0.5)
    ranked = reranker.rerank(
        query="com ga ngon",
        intent={},
        top_k=2,
        candidates=[
            {"store_key": "a", "name": "A", "final_score": 1.0, "evidence": ["ngon"]},
            {"store_key": "b", "name": "B", "final_score": 0.2, "evidence": ["ngon"]},
        ],
    )
    assert ranked[0]["store_key"] == "b"
    assert ranked[0]["ce_score"] == 1.0
    assert ranked[0]["ce_raw_score"] == 0.9
    assert ranked[0]["final_score_before_ce"] == 0.2
