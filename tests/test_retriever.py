from __future__ import annotations

from retriever import build_graph_candidate_query


def test_build_graph_candidate_query_includes_filters():
    query, params = build_graph_candidate_query(
        {
            "district": "Dong Da",
            "dish_name": "com",
            "min_rating": 4.0,
            "max_distance_km": 3,
            "required_attributes": ["food_quality"],
        },
        has_user_location=True,
        has_max_distance=True,
    )
    assert "MATCH (r)-[:IN_AREA]->(area:Area)" in query
    assert "MATCH (r)-[:SERVES_FAMILY]->(dish:DishFamily)" in query
    assert "dish_families" in query
    assert "point.distance" in query
    assert "att0" in query
    assert params["district"] == "Dong Da"
    assert params["dish_name"] == "cơm"
    assert params["min_rating"] == 4.0
    assert params["attr_0"] == "food_quality"
