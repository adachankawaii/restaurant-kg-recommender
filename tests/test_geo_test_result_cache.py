from pathlib import Path

from services.graphrag_service.result_cache import GeoTestResultCache


def _cache() -> GeoTestResultCache:
    root = Path(__file__).resolve().parents[1]
    return GeoTestResultCache(root / "production" / "graphrag_50_geo_test_results_long.csv")


def test_geo_test_cache_matches_similar_food_queries():
    cache = _cache()

    hit = cache.recommend("minh muon an pho bo gan dai hoc bach khoa", top_k=3)

    assert hit is not None
    intent, results = hit
    assert intent["cache_hit"] is True
    assert intent["match_score"] >= cache.threshold
    assert [row["restaurant_id"] for row in results] == ["11471", "118277", "8314"]
    assert results[0]["address"]
    assert results[0]["graphrag_mode"] == "geo_test_cache"


def test_geo_test_cache_does_not_match_unrelated_queries():
    cache = _cache()

    assert cache.recommend("pizza ho guom") is None
    assert cache.recommend("tra sua nguyen trai") is None
    assert cache.recommend("banh mi pate") is None
