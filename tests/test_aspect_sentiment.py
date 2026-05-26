from __future__ import annotations

from aspect_sentiment import sanitize_aspect_scores


def test_sanitize_aspect_scores_clamps_and_fills_missing_values():
    scores = sanitize_aspect_scores({
        "scores": {
            "food_quality": -2,
            "service": 0.5,
            "cleanliness": "bad",
        }
    })
    assert scores["food_quality"] == -1.0
    assert scores["service"] == 0.5
    assert scores["cleanliness"] == 0.0
    assert scores["packaging"] == 0.0
