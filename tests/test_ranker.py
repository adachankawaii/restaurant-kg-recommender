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

