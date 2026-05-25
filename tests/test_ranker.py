from __future__ import annotations

from ranker import rerank_candidates, safe_float


def test_safe_float_handles_bad_values():
    assert safe_float(None, default=None) is None
    assert safe_float("bad", default=0.0) == 0.0


def test_rerank_uses_distance_for_near_query():
    near = {
        "store_key": "near",
        "name": "Near",
        "rating": 4.0,
        "distance_km": 0.2,
        "distance_score": 0.93,
    }
    far = {
        "store_key": "far",
        "name": "Far",
        "rating": 5.0,
        "distance_km": 5.0,
        "distance_score": 0.37,
    }
    ranked = rerank_candidates(
        query="quan gan toi",
        graph_hits=[near, far],
        neighbor_hits=[],
        restaurant_vector_hits=[],
        text_unit_hits=[],
        distance_weight=0.30,
    )
    assert ranked[0]["store_key"] == "near"
    assert "graph_filter" in ranked[0]["source_flags"]


def test_text_unit_evidence_is_merged():
    ranked = rerank_candidates(
        query="do an ngon",
        graph_hits=[],
        neighbor_hits=[],
        restaurant_vector_hits=[{"store_key": "1", "name": "A", "vec_score": 0.8}],
        text_unit_hits=[{"store_key": "1", "feedback": "ngon", "vec_score": 0.9}],
    )
    assert ranked[0]["store_key"] == "1"
    assert ranked[0]["evidence"] == ["ngon"]
    assert "text_unit_vector" in ranked[0]["source_flags"]


def test_post_fusion_validation_filters_wrong_price_and_dish():
    ranked = rerank_candidates(
        query="com ga gia re",
        intent={"dish_name": "cơm gà", "price_band": "budget"},
        graph_hits=[],
        neighbor_hits=[],
        restaurant_vector_hits=[
            {"store_key": "bad", "name": "Bad", "price_band": "premium", "dish_families": ["phở"], "vec_score": 0.99},
            {"store_key": "good", "name": "Good", "price_band": "budget", "dish_families": ["cơm gà"], "vec_score": 0.7},
        ],
        text_unit_hits=[],
    )
    assert [r["store_key"] for r in ranked] == ["good"]


def test_nearest_geo_intent_sorts_by_distance_first():
    near = {"store_key": "near", "name": "Near", "rating": 3.0, "distance_km": 0.2, "distance_score": 0.9}
    far = {"store_key": "far", "name": "Far", "rating": 5.0, "distance_km": 1.8, "distance_score": 0.55}
    ranked = rerank_candidates(
        query="quan gan nhat",
        intent={"geo_intent": "nearest"},
        graph_hits=[far, near],
        neighbor_hits=[],
        restaurant_vector_hits=[],
        text_unit_hits=[],
    )
    assert ranked[0]["store_key"] == "near"
    assert ranked[0]["geo_intent"] == "nearest"
