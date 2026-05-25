from __future__ import annotations

import pandas as pd

from vector_store import build_restaurant_summary_doc


def test_restaurant_summary_doc_excludes_session_distance():
    doc = build_restaurant_summary_doc(pd.Series({
        "name": "Quan A",
        "address": "Dong Da",
        "district": "Dong Da",
        "city": "Ha Noi",
        "distance_km": 0.25,
        "rating": 4.5,
        "review_count": 10,
        "price_band": "budget",
        "top_menu_items": ["Cơm gà"],
        "dish_families": ["cơm gà"],
        "categories": ["Cơm"],
        "cuisines": [],
    }))
    assert "Distance from user" not in doc
    assert "0.25 km" not in doc
