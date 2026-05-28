from __future__ import annotations

import pandas as pd

from aspect_sentiment import score_feedback_dataframe_from_rating
from ingest import (
    canonicalize_feedback_from_befood,
    haversine_km,
    infer_district,
    prepare_data,
    price_band_from_bounds,
    price_band_from_menu_prices,
    normalize_dish_family,
)


def test_haversine_km_zero_distance():
    assert haversine_km(21.0, 105.0, 21.0, 105.0) == 0.0


def test_haversine_km_known_small_distance():
    dist = haversine_km(21.005, 105.843, 21.002831, 105.841988)
    assert dist is not None
    assert 0.20 <= dist <= 0.35


def test_price_band_from_bounds():
    assert price_band_from_bounds(10000, 50000) == "budget"
    assert price_band_from_bounds(58000, 95000) == "mid"
    assert price_band_from_bounds(15000, 490009) == "premium"


def test_price_band_from_menu_prices_uses_distribution_not_max():
    assert price_band_from_menu_prices([25000, 30000, 35000, 45000, 490000]) == "budget"
    assert price_band_from_menu_prices([60000, 75000, 90000, 150000]) == "mid"
    assert price_band_from_menu_prices([140000, 180000, 220000]) == "premium"


def test_normalize_dish_family_groups_menu_items():
    assert normalize_dish_family("Cơm Gà Xối Mỡ") == "cơm gà"
    assert normalize_dish_family("Bún chả đặc biệt") == "bún chả"
    assert normalize_dish_family("Gà rán combo") == "gà rán"


def test_infer_district_from_vietnamese_address():
    assert infer_district("101A5 ngo 167 Tay Son, Quang Trung, Dong Da, Ha Noi") == "Quan Dong Da"
    assert infer_district("101 X\u00e3 \u0110\u00e0n, Qu\u1eadn \u0110\u1ed1ng \u0110a, H\u00e0 N\u1ed9i") == "Quan Dong Da"
    assert infer_district("64 Ng\u00f5 8 B\u00f9i Ng\u1ecdc D\u01b0\u01a1ng, Hai B\u00e0 Tr\u01b0ng, H\u00e0 N\u1ed9i") == "Quan Hai Ba Trung"
    assert infer_district("154 X\u00e3 \u0110\u00e0n, H\u00e0 N\u1ed9i") is None


def test_feedback_explodes_comments_list():
    raw = pd.DataFrame([{
        "restaurant_id": 1,
        "restaurant_name": "Quan A",
        "rating": 4.5,
        "comments_list": '["ngon", "sach"]',
    }])
    feedback = canonicalize_feedback_from_befood(raw)
    assert len(feedback) == 2
    assert set(feedback["feedback"]) == {"ngon", "sach"}


def test_rating_based_feedback_sentiment_ignores_comment_text():
    feedback = pd.DataFrame([
        {"rating": 5.0, "feedback": "bad text"},
        {"rating": 1.0, "feedback": "good text"},
    ])
    scored = score_feedback_dataframe_from_rating(feedback)
    assert scored.loc[0, "sentiment"] == "positive"
    assert scored.loc[1, "sentiment"] == "negative"
    assert scored.loc[0, "aspect_scores"]["food_quality"] > 0


def test_prepare_data_shapes_and_distance():
    raw_restaurants = pd.DataFrame([{
        "restaurant_id": 1,
        "restaurant_name": "Quan A",
        "source": "befood",
        "matched_terms_text": "com | pho",
        "latitude": 21.0,
        "longitude": 105.0,
        "address": "Dong Da, Ha Noi",
        "rating": 4.5,
        "review_count": 10,
        "price_min": 20000,
        "price_max": 50000,
        "categories_text": "COM",
        "opening_hours": "08:00-20:00",
        "delivery_time": 20,
        "image_url": "",
        "menu_count": 1,
        "comment_count": 1,
        "comments_list": '["ngon"]',
    }])
    raw_menu = pd.DataFrame([{
        "restaurant_id": 1,
        "restaurant_name": "Quan A",
        "category_id": 10,
        "category_name": "COM",
        "restaurant_item_id": 100,
        "item_name": "Com ga",
        "item_details": "",
        "price": 40000,
        "old_price": 40000,
        "order_count": 5,
        "like_count": 1,
        "dislike_count": 0,
        "category_position": 1,
        "item_position": 1,
        "item_image": "",
    }])
    raw_foody = pd.DataFrame([{
        "input_store_id": 1,
        "input_store_name": "Quan A",
        "address": None,
        "district": None,
        "city": None,
        "lat": None,
        "lng": None,
        "avg_rating": None,
        "total_review": None,
        "price_min": None,
        "price_max": None,
        "categories": None,
        "cuisines": None,
        "audiences": None,
        "opening_hours": None,
    }])
    prepared = prepare_data(raw_restaurants, raw_menu, raw_foody, user_lat=21.0, user_lng=105.0)
    assert len(prepared.summary) == 1
    assert len(prepared.feedback) == 1
    assert len(prepared.menu_items) == 1
    assert len(prepared.dish_families) == 1
    assert prepared.dish_families.iloc[0]["dish_family"] == "cơm gà"
    assert prepared.summary.iloc[0]["price_band"] == "budget"
    assert prepared.summary.iloc[0]["distance_km"] == 0.0
