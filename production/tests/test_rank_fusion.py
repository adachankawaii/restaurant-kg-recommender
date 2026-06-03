from services.ranking_service.fusion import fuse_results


def test_rank_fusion_returns_scores():
    fused = fuse_results(
        [{"restaurant_id": "1", "graphrag_score": 2.0, "rule_score": 1.0, "popularity_score": 0.5}],
        [{"restaurant_id": "1", "rgcn_score": 0.5}],
        "hybrid",
        {"graphrag": 0.7, "rgcn": 0.1, "rule": 0.15, "popularity": 0.05},
    )
    assert fused[0]["scores"]["final"] >= 0.0
