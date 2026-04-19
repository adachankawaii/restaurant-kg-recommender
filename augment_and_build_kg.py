from __future__ import annotations

import csv
import json
import math
import re
import unicodedata
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(".")
KG_DIR = BASE_DIR / "kg_tables_all"
GRAPH_DIR = KG_DIR / "kg_graph"
STORE_SOURCE_FILE = BASE_DIR / "store_from_top5_json.csv"
MENU_ITEM_SOURCE_FILE = BASE_DIR / "menu_item_from_top5_json.csv"
FOODY_PLACES_FILE = BASE_DIR / "foody_hust_output" / "foody_hust_places_from_store_csv.csv"
GOOGLE_PLACES_FILE = BASE_DIR / "be_google_maps_unique.csv"
FEEDBACK_FILE = BASE_DIR / "store_feedback_crawled.csv"
USER_SCENARIOS_FILE = BASE_DIR / "user_scenarios_1.csv"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, object]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def strip_accents(text: object) -> str:
    if text is None:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def slugify(text: object) -> str:
    text = strip_accents(text).lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace(" ", "_")


def norm_text(text: object) -> str:
    return slugify(text).replace("_", " ")


def token_similarity(a: object, b: object) -> float:
    ta = set(norm_text(a).split())
    tb = set(norm_text(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def as_float(value: object) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except ValueError:
        return None


def haversine_m(lat1: object, lng1: object, lat2: object, lng2: object) -> float | None:
    lat1f, lng1f, lat2f, lng2f = map(as_float, [lat1, lng1, lat2, lng2])
    if None in [lat1f, lng1f, lat2f, lng2f]:
        return None
    r = 6371000
    p1 = math.radians(lat1f)
    p2 = math.radians(lat2f)
    dphi = math.radians(lat2f - lat1f)
    dlambda = math.radians(lng2f - lng1f)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def parse_location_parts(address: str) -> dict[str, str]:
    parts = [p.strip() for p in str(address).split(",") if p.strip()]
    city = "Hà Nội"
    district = ""
    ward = ""
    street = parts[0] if parts else ""

    for part in parts:
        n = norm_text(part)
        if "hai ba trung" in n:
            district = "Hai Bà Trưng"
        if "bach khoa" in n:
            ward = "Bách Khoa"
        if "bach mai" in n:
            ward = ward or "Bạch Mai"
    if not district and len(parts) >= 2:
        district = parts[-2].replace("Quận ", "").strip()
    return {"street": street, "ward": ward, "district": district, "city": city}


def read_optional_csv(path: Path) -> list[dict[str, str]]:
    return read_csv(path) if path.exists() else []


def first_non_empty(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def as_int(value: object) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value).replace(",", "")))
    except ValueError:
        return None


def extract_time_component(value: object) -> str:
    text = first_non_empty(value)
    if not text:
        return ""
    match = re.search(r"(\d{1,2}:\d{2})", text)
    return match.group(1) if match else ""


def split_multi_values(value: object) -> list[str]:
    text = first_non_empty(value)
    if not text:
        return []
    parts = re.split(r"[|;]", text)
    return [part.strip() for part in parts if part and part.strip()]


def parse_json_blob(value: object) -> dict[str, object]:
    text = first_non_empty(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_google_open_close(opening_hours: object) -> tuple[str, str, str]:
    if not isinstance(opening_hours, dict):
        return "", "", ""

    open_state = first_non_empty(opening_hours.get("open_state"))
    hours = opening_hours.get("hours", [])

    if open_state and "mở cả ngày" in norm_text(open_state):
        return open_state, "00:00", "23:59"

    parsed_ranges: list[tuple[str, str]] = []
    for item in hours:
        if not isinstance(item, dict):
            continue
        for _, value in item.items():
            if value is None:
                continue
            text = str(value).strip()
            if "mở cửa cả ngày" in norm_text(text):
                parsed_ranges.append(("00:00", "23:59"))
                continue
            match = re.match(r"^\s*(\d{1,2}:\d{2})\s*[–-]\s*(\d{1,2}:\d{2})\s*$", text)
            if match:
                parsed_ranges.append((match.group(1), match.group(2)))

    if not parsed_ranges:
        return open_state, "", ""

    most_common = max(set(parsed_ranges), key=parsed_ranges.count)
    return open_state, most_common[0], most_common[1]


def price_range_to_budget(price_range_id: object) -> int:
    value = slugify(price_range_id)
    if value in {"under_50k", "u50k", "below_50k"}:
        return 50000
    if value in {"50k_100k", "50k_to_100k", "between_50k_100k"}:
        return 100000
    if value in {"over_100k", "above_100k"}:
        return 200000
    return 60000


def time_slot_to_clock(time_slot_id: object) -> tuple[str, str]:
    slot = slugify(time_slot_id)
    if slot == "lunch":
        return "12:00", "13:00"
    if slot == "afternoon":
        return "15:00", "16:00"
    if slot == "dinner":
        return "18:30", "19:30"
    return "12:00", "13:00"


def classify_review_sentiment(rating: object, review_text: object) -> str:
    rating_value = as_float(rating)
    if rating_value is not None:
        if rating_value >= 4:
            return "positive"
        if rating_value <= 2:
            return "negative"
    text = norm_text(review_text)
    if any(token in text for token in ["ngon", "dang dong tien", "sach se", "thom", "tot", "danh cho", "de chiu"]):
        return "positive"
    if any(token in text for token in ["thieu", "te", "cham", "ban", "khong hop", "nhat", "dat"]):
        return "negative"
    return "neutral"


def store_rank_key(row: dict[str, str]) -> tuple[float, float, float]:
    rank = as_float(row.get("rank"))
    score = as_float(row.get("score"))
    distance = as_float(row.get("distance_m"))
    return (
        rank if rank is not None else 9999.0,
        -(score if score is not None else 0.0),
        distance if distance is not None else 999999.0,
    )


def build_tables_from_sources() -> dict[str, list[dict[str, object]]]:
    store_rows = read_csv(STORE_SOURCE_FILE)
    menu_rows = read_csv(MENU_ITEM_SOURCE_FILE)
    foody_rows = read_optional_csv(FOODY_PLACES_FILE)
    google_rows = read_optional_csv(GOOGLE_PLACES_FILE)
    feedback_rows = read_optional_csv(FEEDBACK_FILE)
    scenario_rows = read_optional_csv(USER_SCENARIOS_FILE)

    google_by_id = {str(row.get("id", "")).strip(): row for row in google_rows if first_non_empty(row.get("id"))}
    foody_by_store = {str(row.get("input_store_id", "")).strip(): row for row in foody_rows if first_non_empty(row.get("input_store_id"))}
    menu_by_store: dict[str, list[dict[str, str]]] = {}
    for row in menu_rows:
        store_id = str(row.get("store_id", "")).strip()
        if store_id:
            menu_by_store.setdefault(store_id, []).append(row)

    feedback_by_store: dict[str, list[dict[str, str]]] = {}
    for row in feedback_rows:
        store_id = str(row.get("store_id", "")).strip()
        if store_id:
            feedback_by_store.setdefault(store_id, []).append(row)

    scenario_by_user = {str(row.get("user_id", "")).strip(): row for row in scenario_rows if first_non_empty(row.get("user_id"))}

    unique_stores: dict[str, dict[str, str]] = {}
    for row in store_rows:
        store_id = str(row.get("store_id", "")).strip()
        if not store_id:
            continue
        current = unique_stores.get(store_id)
        if current is None or store_rank_key(row) < store_rank_key(current):
            unique_stores[store_id] = row

    store_master: list[dict[str, object]] = []
    store_category: list[dict[str, object]] = []
    store_context_tag: list[dict[str, object]] = []
    store_service_option: list[dict[str, object]] = []
    store_aspect_agg: list[dict[str, object]] = []
    review_fact: list[dict[str, object]] = []
    store_source_map: list[dict[str, object]] = []
    user_profile: list[dict[str, object]] = []
    user_session_context: list[dict[str, object]] = []
    user_preference: list[dict[str, object]] = []
    store_menu_item: list[dict[str, object]] = []
    user_store_interaction: list[dict[str, object]] = []
    normalized_location: list[dict[str, object]] = []
    source_match_quality: list[dict[str, object]] = []

    seen_store_categories: set[tuple[str, str, str]] = set()
    seen_store_tags: set[tuple[str, str, str]] = set()
    seen_store_services: set[tuple[str, str]] = set()
    seen_user_preferences: set[tuple[str, str, str]] = set()
    seen_user_interactions: set[tuple[str, str]] = set()

    for store_id, row in unique_stores.items():
        google_row = google_by_id.get(store_id, {})
        foody_row = foody_by_store.get(store_id, {})
        feedback_items = feedback_by_store.get(store_id, [])
        menu_items = menu_by_store.get(store_id, [])

        store_name = first_non_empty(row.get("store_name"), row.get("name"), google_row.get("name"), google_row.get("query_name"), foody_row.get("name"))
        store_address = first_non_empty(row.get("address"), foody_row.get("input_address"), foody_row.get("address"), google_row.get("query_address"), google_row.get("address"))
        store_lat = first_non_empty(row.get("latitude"), google_row.get("latitude"), foody_row.get("lat"))
        store_lng = first_non_empty(row.get("longitude"), google_row.get("longitude"), foody_row.get("lng"))
        location_parts = parse_location_parts(store_address)
        ward = location_parts["ward"]
        district = location_parts["district"]
        city = location_parts["city"]
        if not ward and first_non_empty(foody_row.get("area")):
            ward = first_non_empty(foody_row.get("area"))
        if not district and first_non_empty(foody_row.get("district")):
            district = first_non_empty(foody_row.get("district"))
        if not city and first_non_empty(foody_row.get("city")):
            city = first_non_empty(foody_row.get("city"))

        google_open_state, google_open_time, google_close_time = parse_google_open_close(parse_json_blob(google_row.get("opening_hours")))
        top5_open_time = extract_time_component(row.get("next_slot_time"))
        top5_close_time = extract_time_component(row.get("end_time"))
        score_value = as_float(row.get("score"))
        google_rating = as_float(google_row.get("rating"))
        foody_rating = as_float(foody_row.get("avg_rating"))
        be_rating_avg = google_rating if google_rating is not None else foody_rating if foody_rating is not None else (round(score_value * 5, 2) if score_value is not None else None)
        be_rating_count = as_int(google_row.get("review_count")) or as_int(foody_row.get("total_review")) or len(feedback_items)

        store_master.append({
            "store_id": store_id,
            "canonical_name": store_name,
            "address": store_address,
            "lat": store_lat,
            "lng": store_lng,
            "ward": ward,
            "district": district,
            "city": city,
            "primary_category": first_non_empty(row.get("merchant_category_name"), foody_row.get("categories"), google_row.get("type")),
            "status": first_non_empty(row.get("status"), foody_row.get("crawl_status"), "active"),
            "be_open_time": top5_open_time,
            "be_close_time": top5_close_time,
            "median_price_vnd": as_int(row.get("median_price")) or as_int(foody_row.get("median_price")) or 0,
            "be_rating_avg": be_rating_avg if be_rating_avg is not None else 0,
            "be_rating_count": be_rating_count if be_rating_count is not None else 0,
            "google_name": first_non_empty(google_row.get("name"), google_row.get("query_name"), store_name),
            "google_query_name": first_non_empty(row.get("store_name"), google_row.get("query_name"), store_name),
            "google_query_address": first_non_empty(row.get("address"), google_row.get("query_address"), store_address),
            "google_address": first_non_empty(google_row.get("address"), foody_row.get("address"), store_address),
            "google_lat": first_non_empty(google_row.get("latitude"), foody_row.get("lat"), store_lat),
            "google_lng": first_non_empty(google_row.get("longitude"), foody_row.get("lng"), store_lng),
            "google_rating": first_non_empty(google_row.get("rating"), foody_row.get("avg_rating"), be_rating_avg),
            "google_review_count": first_non_empty(google_row.get("review_count"), foody_row.get("total_review"), be_rating_count),
            "google_phone": first_non_empty(google_row.get("phone"), foody_row.get("phone")),
            "google_website": first_non_empty(google_row.get("website"), foody_row.get("website")),
            "google_place_id": first_non_empty(google_row.get("id")),
            "google_data_id": first_non_empty(foody_row.get("restaurant_id"), row.get("merchant_id"), row.get("store_id")),
            "google_open_state": google_open_state,
            "google_open_time": google_open_time,
            "google_close_time": google_close_time,
            "google_match_method": "google_unique" if google_row else ("foody_hust" if foody_row else "top5_recommendation"),
            "google_match_confidence": "high" if google_row and foody_row else "medium" if (google_row or foody_row) else "low",
            "google_match_score": 1.0 if google_row and foody_row else 0.85 if google_row else 0.75 if foody_row else 0.6,
        })

        store_source_map.append({
            "store_id": store_id,
            "source_name": "top5_recommendation",
            "source_store_key": store_id,
            "source_url": "",
            "source_name_raw": store_name,
            "address_raw": store_address,
            "match_confidence": "high",
            "note": f"rank={row.get('rank')}; score={row.get('score')}",
        })
        if google_row:
            store_source_map.append({
                "store_id": store_id,
                "source_name": "google_maps_unique",
                "source_store_key": first_non_empty(google_row.get("id")),
                "source_url": first_non_empty(google_row.get("website")),
                "source_name_raw": first_non_empty(google_row.get("name"), google_row.get("query_name")),
                "address_raw": first_non_empty(google_row.get("query_address"), google_row.get("address")),
                "match_confidence": "high",
                "note": f"google_rating={google_row.get('rating')}; review_count={google_row.get('review_count')}",
            })
        if foody_row:
            store_source_map.append({
                "store_id": store_id,
                "source_name": "foody_hust_places",
                "source_store_key": first_non_empty(foody_row.get("restaurant_id"), foody_row.get("matched_foody_url")),
                "source_url": first_non_empty(foody_row.get("matched_foody_url"), foody_row.get("url")),
                "source_name_raw": first_non_empty(foody_row.get("name"), foody_row.get("brand_name")),
                "address_raw": first_non_empty(foody_row.get("input_address"), foody_row.get("address")),
                "match_confidence": "high" if first_non_empty(foody_row.get("crawl_status")) == "ok" else "medium",
                "note": first_non_empty(foody_row.get("error"), foody_row.get("crawl_status")),
            })

        categories = [
            ("primary", row.get("merchant_category_name"), "store_from_top5_json"),
            ("google_type", google_row.get("type"), "google_maps_unique"),
        ]
        for category_type, category_value, source_name in categories:
            for value in split_multi_values(category_value):
                key = (store_id, category_type, value)
                if value and key not in seen_store_categories:
                    seen_store_categories.add(key)
                    store_category.append({"store_id": store_id, "category_type": category_type, "category_value": value, "source_name": source_name})

        foody_categories = split_multi_values(foody_row.get("categories"))
        foody_cuisines = split_multi_values(foody_row.get("cuisines"))
        foody_audiences = split_multi_values(foody_row.get("audiences"))
        foody_wifi = first_non_empty(foody_row.get("wifi"))
        for value in foody_categories:
            key = (store_id, "foody_category", value)
            if key not in seen_store_categories:
                seen_store_categories.add(key)
                store_category.append({"store_id": store_id, "category_type": "foody_category", "category_value": value, "source_name": "foody_hust_places"})
        for value in foody_cuisines:
            key = (store_id, "foody_cuisine", value)
            if key not in seen_store_categories:
                seen_store_categories.add(key)
                store_category.append({"store_id": store_id, "category_type": "foody_cuisine", "category_value": value, "source_name": "foody_hust_places"})
        for value in foody_audiences:
            key = (store_id, "foody_audience", value)
            if key not in seen_store_categories:
                seen_store_categories.add(key)
                store_category.append({"store_id": store_id, "category_type": "foody_audience", "category_value": value, "source_name": "foody_hust_places"})
        if foody_wifi:
            key = (store_id, "wifi", foody_wifi)
            if key not in seen_store_categories:
                seen_store_categories.add(key)
                store_category.append({"store_id": store_id, "category_type": "wifi", "category_value": foody_wifi, "source_name": "foody_hust_places"})

        for menu_row in menu_items:
            category_name = first_non_empty(menu_row.get("category_name"), menu_row.get("category_id"))
            if category_name:
                key = (store_id, "menu_category", category_name)
                if key not in seen_store_categories:
                    seen_store_categories.add(key)
                    store_category.append({"store_id": store_id, "category_type": "menu_category", "category_value": category_name, "source_name": "menu_item_from_top5_json"})

        for tag_type, raw_value in [
            ("highlight", google_row.get("highlights")),
            ("popular_for", google_row.get("popular_for")),
            ("atmosphere", google_row.get("atmosphere")),
            ("crowd", google_row.get("crowd")),
            ("dining_option", google_row.get("dining_options")),
            ("offering", google_row.get("offerings")),
            ("foody_category", foody_row.get("categories")),
            ("foody_cuisine", foody_row.get("cuisines")),
            ("foody_audience", foody_row.get("audiences")),
        ]:
            for value in split_multi_values(raw_value):
                key = (store_id, tag_type, value)
                if value and key not in seen_store_tags:
                    seen_store_tags.add(key)
                    store_context_tag.append({"store_id": store_id, "tag_type": tag_type, "tag_value": value, "source_name": "google_maps_unique" if tag_type not in {"foody_category", "foody_cuisine", "foody_audience"} else "foody_hust_places"})

        service_options = parse_json_blob(google_row.get("service_options"))
        for service_name, service_value in service_options.items():
            key = (store_id, service_name)
            if key not in seen_store_services:
                seen_store_services.add(key)
                store_service_option.append({"store_id": store_id, "service_option": service_name, "value": service_value, "source_name": "google_maps_unique"})
        for service_name in split_multi_values(google_row.get("dining_options")):
            key = (store_id, service_name)
            if key not in seen_store_services:
                seen_store_services.add(key)
                store_service_option.append({"store_id": store_id, "service_option": service_name, "value": True, "source_name": "google_maps_unique"})

        feedback_texts = [first_non_empty(item.get("feedback"), item.get("review_text")) for item in feedback_items]
        evidence_blob = norm_text(" ".join(text for text in feedback_texts if text))
        google_blob = norm_text(" ".join(filter(None, [google_row.get("highlights"), google_row.get("popular_for"), google_row.get("atmosphere"), google_row.get("crowd"), google_row.get("dining_options"), google_row.get("offerings"), foody_row.get("categories"), foody_row.get("cuisines"), foody_row.get("audiences")])) )
        all_blob = f"{evidence_blob} {google_blob}"

        aspect_specs = {
            "taste": ["ngon", "thom", "de uong", "chat luong", "danh dong tien", "rat ngon"],
            "staff_service": ["phuc vu", "than thien", "nhanh", "tiet chot", "dong goi tot", "sach se"],
            "value_for_money": ["dang dong tien", "re", "gia tot", "gia mem", "phu hop"],
            "space": ["am cung", "thong thuong", "yen tinh", "thoang", "work friendly"],
            "cleanliness": ["sach se", "gon gang", "khong ban"],
            "work_friendly": ["lam viec", "may tinh xach tay", "work", "hoc tap", "phu hop de lam viec"],
        }
        negative_specs = {
            "taste": ["nhat", "khong hop khau vi", "te", "uoc"],
            "staff_service": ["cham", "thieu", "sai", "khong", "cau"],
            "value_for_money": ["dat", "khong dang", "khong hop gia"],
            "space": ["chat", "on", "nong"],
            "cleanliness": ["ban", "nham", "mau"],
            "work_friendly": ["on", "chat", "khong phu hop"],
        }
        numeric_signals = {
            "taste": be_rating_avg or 0,
            "staff_service": as_float(foody_row.get("rating_service")) or 0,
            "value_for_money": as_float(foody_row.get("rating_price")) or 0,
            "space": as_float(foody_row.get("rating_space")) or 0,
            "cleanliness": as_float(foody_row.get("rating_quality")) or 0,
            "work_friendly": 5 if "phu hop de lam viec" in all_blob or "may tinh xach tay" in all_blob else 0,
        }
        if as_int(row.get("median_price")) is not None and as_int(row.get("median_price")) <= 50000:
            numeric_signals["value_for_money"] = max(numeric_signals["value_for_money"], 4.5)
        elif as_int(row.get("median_price")) is not None and as_int(row.get("median_price")) > 80000:
            numeric_signals["value_for_money"] = min(numeric_signals["value_for_money"], 2.5)

        evidence_sources = [name for name, source in [("store_feedback_crawled", feedback_items), ("google_maps_unique", google_row), ("foody_hust_places", foody_row)] if source]
        for aspect_name in aspect_specs:
            positive_hits = sum(all_blob.count(token) for token in aspect_specs[aspect_name])
            negative_hits = sum(all_blob.count(token) for token in negative_specs.get(aspect_name, []))
            numeric = numeric_signals.get(aspect_name, 0)
            if numeric >= 4:
                positive_hits += 1
            elif numeric and numeric < 3:
                negative_hits += 1

            if positive_hits > negative_hits:
                sentiment = "positive"
            elif negative_hits > positive_hits:
                sentiment = "negative"
            else:
                sentiment = "neutral"
            mention_count = max(1, positive_hits + negative_hits + (0 if sentiment != "neutral" else 1))
            store_aspect_agg.append({
                "store_id": store_id,
                "aspect_name": aspect_name,
                "aspect_sentiment": sentiment,
                "mention_count": mention_count,
                "positive_mentions": positive_hits,
                "negative_mentions": negative_hits,
                "neutral_mentions": 1 if sentiment == "neutral" else 0,
                "evidence_sources": "|".join(evidence_sources),
            })

        for index, feedback_row in enumerate(feedback_items, start=1):
            review_text = first_non_empty(feedback_row.get("feedback"), feedback_row.get("review_text"))
            review_fact.append({
                "review_id": first_non_empty(feedback_row.get("review_id"), f"fb_{store_id}_{index}"),
                "store_id": store_id,
                "source_name": first_non_empty(feedback_row.get("source"), "store_feedback_crawled"),
                "rated_at": first_non_empty(feedback_row.get("rated_at")),
                "rating_5": first_non_empty(feedback_row.get("rating")),
                "review_text": review_text,
                "sentiment": classify_review_sentiment(feedback_row.get("rating"), review_text),
            })

        normalized_location.append({
            "location_id": f"loc_{store_id}",
            "store_id": store_id,
            "canonical_address": store_address,
            "street": parse_location_parts(store_address)["street"],
            "ward": ward,
            "district": district,
            "city": city,
            "lat": first_non_empty(google_row.get("latitude"), row.get("latitude"), foody_row.get("lat")),
            "lng": first_non_empty(google_row.get("longitude"), row.get("longitude"), foody_row.get("lng")),
            "area_tag": slugify(f"{ward}_{district}"),
        })

        if google_row:
            google_distance = haversine_m(row.get("latitude"), row.get("longitude"), google_row.get("latitude"), google_row.get("longitude"))
            name_similarity = token_similarity(row.get("store_name"), google_row.get("name"))
            address_similarity = token_similarity(row.get("address"), google_row.get("query_address"))
            confidence = "high" if (google_distance is not None and google_distance <= 250 and name_similarity >= 0.25) else "medium"
            source_match_quality.append({
                "store_id": store_id,
                "source_name": "google_maps_unique",
                "match_confidence_recomputed": confidence,
                "geo_distance_m": "" if google_distance is None else round(google_distance, 1),
                "name_similarity": round(name_similarity, 3),
                "address_similarity": round(address_similarity, 3),
                "is_suspect": int(google_distance is not None and google_distance > 800),
                "note": "generated_from_google_unique",
            })
        if foody_row:
            foody_distance = haversine_m(row.get("latitude"), row.get("longitude"), foody_row.get("lat"), foody_row.get("lng"))
            name_similarity = token_similarity(row.get("store_name"), foody_row.get("name"))
            address_similarity = token_similarity(row.get("address"), foody_row.get("input_address"))
            confidence = "high" if first_non_empty(foody_row.get("crawl_status")) == "ok" else "medium"
            source_match_quality.append({
                "store_id": store_id,
                "source_name": "foody_hust_places",
                "match_confidence_recomputed": confidence,
                "geo_distance_m": "" if foody_distance is None else round(foody_distance, 1),
                "name_similarity": round(name_similarity, 3),
                "address_similarity": round(address_similarity, 3),
                "is_suspect": int(foody_distance is not None and foody_distance > 800),
                "note": first_non_empty(foody_row.get("crawl_status"), foody_row.get("error"), "generated_from_foody_places"),
            })

        user_id = first_non_empty(row.get("user_id"))
        scenario_row = scenario_by_user.get(user_id, {})
        if scenario_row:
            display_name = f"{user_id} - {scenario_row.get('area_id', '')}"
            budget_max = price_range_to_budget(scenario_row.get("desired_price_range_id"))
            budget_min = 0 if budget_max <= 50000 else 50000
            open_clock, close_clock = time_slot_to_clock(scenario_row.get("time_slot_id"))
            user_profile.append({
                "user_id": user_id,
                "display_name": display_name,
                "home_district": first_non_empty(scenario_row.get("area_id")),
                "dietary_profile": first_non_empty(scenario_row.get("term")),
                "default_budget_vnd": budget_max,
            })
            user_session_context.append({
                "session_id": f"session:{user_id}",
                "user_id": user_id,
                "query_time": f"2026-04-19 {open_clock}:00",
                "current_lat": scenario_row.get("query_lat"),
                "current_lng": scenario_row.get("query_lng"),
                "radius_m": scenario_row.get("distance_tolerance_m"),
                "party_size": 1,
                "budget_min_vnd": budget_min,
                "budget_max_vnd": budget_max,
                "intent": first_non_empty(scenario_row.get("term")),
                "desired_service": "dine_in" if scenario_row.get("time_slot_id") in {"lunch", "dinner"} else "cafe_work" if scenario_row.get("time_slot_id") == "afternoon" else "dine_in",
                "weather_context": "normal",
                "time_context": first_non_empty(scenario_row.get("time_slot_id")),
            })

            for aspect_value in split_multi_values(scenario_row.get("preferred_aspects")):
                key = (user_id, "aspect", aspect_value)
                if key not in seen_user_preferences:
                    seen_user_preferences.add(key)
                    user_preference.append({
                        "user_id": user_id,
                        "preference_type": "aspect",
                        "preference_value": aspect_value,
                        "weight": 0.85,
                        "source": "user_scenarios_1",
                    })
            for term_value in split_multi_values(scenario_row.get("term")):
                key = (user_id, "term", term_value)
                if key not in seen_user_preferences:
                    seen_user_preferences.add(key)
                    user_preference.append({
                        "user_id": user_id,
                        "preference_type": "term",
                        "preference_value": term_value,
                        "weight": 0.65,
                        "source": "user_scenarios_1",
                    })

        score_rating = round((score_value * 5), 2) if score_value is not None else first_non_empty(row.get("median_price"))
        interaction_key = (user_id, store_id)
        if user_id and interaction_key not in seen_user_interactions:
            seen_user_interactions.add(interaction_key)
            user_store_interaction.append({
                "interaction_id": f"ui_{user_id}_{store_id}",
                "user_id": user_id,
                "store_id": store_id,
                "interaction_type": "recommended",
                "interacted_at": first_non_empty(row.get("next_slot_time"), row.get("end_time")),
                "rating_5": score_rating,
                "session_id": f"session:{user_id}",
                "source": "store_from_top5_json",
            })

            for menu_index, menu_row in enumerate(menu_items, start=1):
                item_id = first_non_empty(menu_row.get("restaurant_item_id"), f"item_{store_id}_{menu_index}")
                item_name = first_non_empty(menu_row.get("item_name"))
                if not item_name:
                    continue
                price_vnd = as_int(menu_row.get("price")) or as_int(menu_row.get("old_price")) or 0
                order_count = as_int(menu_row.get("order_count")) or 0
                item_details = first_non_empty(menu_row.get("item_details"))
                menu_group = first_non_empty(menu_row.get("category_name"), menu_row.get("category_id"))
                store_menu_item.append({
                    "menu_item_id": item_id,
                    "store_id": store_id,
                    "item_name": item_name,
                    "item_category": menu_group,
                    "price_vnd": price_vnd,
                    "is_signature": 1 if menu_index == 1 or order_count >= 1000 else 0,
                    "spicy_level": 1 if "cay" in norm_text(item_name) else 0,
                    "dietary_tag": "seafood" if any(token in norm_text(item_name) for token in ["tom", "hai san", "muc", "bach tuoc"]) else "vegetarian" if any(token in norm_text(item_name) for token in ["tra", "nuoc", "cafe", "soda", "pepsi", "sting", "coca"]) else "mixed",
                    "source_name": "menu_item_from_top5_json",
                })

    for row in scenario_rows:
        user_id = first_non_empty(row.get("user_id"))
        if not user_id:
            continue
        if user_id not in scenario_by_user:
            continue

    return {
        "store_master": store_master,
        "store_category": store_category,
        "store_context_tag": store_context_tag,
        "store_service_option": store_service_option,
        "store_aspect_agg": store_aspect_agg,
        "review_fact": review_fact,
        "store_source_map": store_source_map,
        "user_profile": user_profile,
        "user_session_context": user_session_context,
        "user_preference": user_preference,
        "store_menu_item": store_menu_item,
        "user_store_interaction": user_store_interaction,
        "normalized_location": normalized_location,
        "source_match_quality": source_match_quality,
    }


def category_to_menu_templates(category: str) -> list[tuple[str, str, int, int, str]]:
    category_slug = slugify(category)
    if "cafe" in category_slug or "tra" in category_slug or "sua" in category_slug:
        return [
            ("Trà sữa trân châu", "milk_tea", 35000, 0, "vegetarian"),
            ("Cà phê sữa đá", "coffee", 32000, 0, "vegetarian"),
            ("Trà đào cam sả", "fruit_tea", 39000, 0, "vegetarian"),
            ("Bánh ngọt dùng kèm", "dessert", 29000, 0, "vegetarian"),
        ]
    if "noodle" in category_slug or "bun" in category_slug or "pho" in category_slug:
        return [
            ("Mì cay cấp độ nhẹ", "noodle", 45000, 1, "meat"),
            ("Mì cay hải sản", "noodle", 59000, 3, "seafood"),
            ("Bún/phở phần nhỏ", "noodle", 35000, 0, "meat"),
            ("Nước giải khát", "drink", 15000, 0, "vegetarian"),
        ]
    if "an_vat" in category_slug:
        return [
            ("Bánh tráng nướng", "snack", 25000, 1, "meat"),
            ("Bánh tráng cuốn", "snack", 30000, 1, "meat"),
            ("Nem chua rán", "snack", 35000, 0, "meat"),
            ("Trà tắc", "drink", 15000, 0, "vegetarian"),
        ]
    return [
        ("Gà tần", "traditional_food", 55000, 0, "meat"),
        ("Bánh cuốn nóng", "traditional_food", 35000, 0, "meat"),
        ("Cháo/súp nóng", "traditional_food", 30000, 0, "meat"),
        ("Rau ăn kèm", "side_dish", 10000, 0, "vegetarian"),
    ]


def build_mock_tables(stores: list[dict[str, str]]) -> dict[str, list[dict[str, object]]]:
    users = [
        {"user_id": "u001", "display_name": "Sinh viên Bách Khoa", "home_district": "Hai Bà Trưng", "dietary_profile": "no_restriction", "default_budget_vnd": 45000},
        {"user_id": "u002", "display_name": "Nhân viên văn phòng", "home_district": "Đống Đa", "dietary_profile": "low_spicy", "default_budget_vnd": 65000},
        {"user_id": "u003", "display_name": "Người dùng ăn nhẹ", "home_district": "Hai Bà Trưng", "dietary_profile": "vegetarian_flexible", "default_budget_vnd": 40000},
    ]
    sessions = [
        {"session_id": "s001", "user_id": "u001", "query_time": "2026-04-14 11:45:00", "current_lat": 21.0058, "current_lng": 105.8458, "radius_m": 900, "party_size": 2, "budget_min_vnd": 25000, "budget_max_vnd": 50000, "intent": "quick_lunch", "desired_service": "dine_in", "weather_context": "hot", "time_context": "lunch"},
        {"session_id": "s002", "user_id": "u002", "query_time": "2026-04-14 15:30:00", "current_lat": 21.0062, "current_lng": 105.8460, "radius_m": 700, "party_size": 1, "budget_min_vnd": 30000, "budget_max_vnd": 70000, "intent": "work_friendly_cafe", "desired_service": "dine_in", "weather_context": "normal", "time_context": "afternoon"},
        {"session_id": "s003", "user_id": "u003", "query_time": "2026-04-14 20:15:00", "current_lat": 21.0045, "current_lng": 105.8465, "radius_m": 1200, "party_size": 4, "budget_min_vnd": 20000, "budget_max_vnd": 45000, "intent": "group_snack", "desired_service": "takeaway", "weather_context": "rain", "time_context": "evening"},
    ]
    preferences = [
        {"user_id": "u001", "preference_type": "category", "preference_value": "noodle_rice_fastcasual", "weight": 0.9, "source": "mock_profile"},
        {"user_id": "u001", "preference_type": "aspect", "preference_value": "value_for_money", "weight": 0.8, "source": "mock_profile"},
        {"user_id": "u002", "preference_type": "aspect", "preference_value": "work_friendly", "weight": 0.95, "source": "mock_profile"},
        {"user_id": "u002", "preference_type": "service", "preference_value": "seating", "weight": 0.75, "source": "mock_profile"},
        {"user_id": "u003", "preference_type": "category", "preference_value": "an_vat", "weight": 0.85, "source": "mock_profile"},
        {"user_id": "u003", "preference_type": "service", "preference_value": "đo_an_mang_đi", "weight": 0.7, "source": "mock_profile"},
    ]

    menu_items: list[dict[str, object]] = []
    locations: list[dict[str, object]] = []
    source_quality: list[dict[str, object]] = []
    interactions: list[dict[str, object]] = []

    for store in stores:
        store_id = store["store_id"]
        loc = parse_location_parts(store.get("address", ""))
        locations.append({
            "location_id": f"loc_{store_id}",
            "store_id": store_id,
            "canonical_address": store.get("address", ""),
            "street": loc["street"],
            "ward": loc["ward"],
            "district": loc["district"],
            "city": loc["city"],
            "lat": store.get("lat", ""),
            "lng": store.get("lng", ""),
            "area_tag": slugify(f'{loc["ward"]}_{loc["district"]}'),
        })

        for idx, (name, item_category, price, spicy_level, dietary_tag) in enumerate(category_to_menu_templates(store.get("primary_category", "")), start=1):
            menu_items.append({
                "menu_item_id": f"mi_{store_id}_{idx}",
                "store_id": store_id,
                "item_name": name,
                "item_category": item_category,
                "price_vnd": price,
                "is_signature": 1 if idx == 1 else 0,
                "spicy_level": spicy_level,
                "dietary_tag": dietary_tag,
                "source_name": "mock_menu",
            })

        distance = haversine_m(store.get("lat"), store.get("lng"), store.get("google_lat"), store.get("google_lng"))
        name_sim = token_similarity(store.get("canonical_name"), store.get("google_name"))
        addr_sim = token_similarity(store.get("address"), store.get("google_address"))
        suspect = int(distance is not None and distance > 150)
        confidence = "low" if suspect else ("medium" if name_sim < 0.25 else "high")
        source_quality.append({
            "store_id": store_id,
            "source_name": "google",
            "match_confidence_recomputed": confidence,
            "geo_distance_m": "" if distance is None else round(distance, 1),
            "name_similarity": round(name_sim, 3),
            "address_similarity": round(addr_sim, 3),
            "is_suspect": suspect,
            "note": "mock_recheck: verify Google place manually" if suspect else "mock_recheck: looks usable",
        })

    seed_interactions = [
        ("u001", "32441", "clicked", "s001", 4.0),
        ("u001", "86930", "ordered", "s001", 4.5),
        ("u002", "28819", "saved", "s002", 4.0),
        ("u002", "5117", "visited", "s002", 3.5),
        ("u003", "27878", "ordered", "s003", 4.5),
        ("u003", "118304", "clicked", "s003", 4.0),
    ]
    for idx, (user_id, store_id, action, session_id, rating) in enumerate(seed_interactions, start=1):
        interactions.append({
            "interaction_id": f"ui_{idx:03d}",
            "user_id": user_id,
            "store_id": store_id,
            "interaction_type": action,
            "interacted_at": f"2026-04-14 {10 + idx:02d}:10:00",
            "rating_5": rating,
            "session_id": session_id,
            "source": "mock_interaction",
        })

    return {
        "user_profile": users,
        "user_session_context": sessions,
        "user_preference": preferences,
        "store_menu_item": menu_items,
        "user_store_interaction": interactions,
        "normalized_location": locations,
        "source_match_quality": source_quality,
    }


def node(node_id: str, label: str, name: object = "", **props: object) -> dict[str, object]:
    return {"node_id": node_id, "label": label, "name": name, "properties": json.dumps(props, ensure_ascii=False, sort_keys=True)}


def edge(source_id: str, relation: str, target_id: str, **props: object) -> dict[str, object]:
    return {"source_id": source_id, "relation": relation, "target_id": target_id, "properties": json.dumps(props, ensure_ascii=False, sort_keys=True)}


def build_graph(tables: dict[str, list[dict[str, object]]]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    stores = read_csv(KG_DIR / "store_master.csv")
    categories = read_csv(KG_DIR / "store_category.csv")
    context_tags = read_csv(KG_DIR / "store_context_tag.csv")
    service_options = read_csv(KG_DIR / "store_service_option.csv")
    aspects = read_csv(KG_DIR / "store_aspect_agg.csv")
    reviews = read_csv(KG_DIR / "review_fact.csv")
    source_map = read_csv(KG_DIR / "store_source_map.csv")

    nodes: dict[str, dict[str, object]] = {}
    edges: list[dict[str, object]] = []

    def add_node(row: dict[str, object]) -> None:
        nodes[str(row["node_id"])] = row

    for s in stores:
        sid = f"store:{s['store_id']}"
        add_node(node(sid, "Store", s["canonical_name"], store_id=s["store_id"], rating=s.get("be_rating_avg"), price=s.get("median_price_vnd")))
        if s.get("primary_category"):
            cid = f"category:{slugify(s['primary_category'])}"
            add_node(node(cid, "Category", s["primary_category"]))
            edges.append(edge(sid, "HAS_PRIMARY_CATEGORY", cid))
        if s.get("ward"):
            area_id = f"area:{slugify(s.get('ward'))}:{slugify(s.get('district'))}"
            add_node(node(area_id, "Area", f"{s.get('ward')}, {s.get('district')}", city=s.get("city")))
            edges.append(edge(sid, "LOCATED_IN", area_id))

    for row in categories:
        cid = f"category:{slugify(row['category_value'])}"
        add_node(node(cid, "Category", row["category_value"], category_type=row.get("category_type")))
        edges.append(edge(f"store:{row['store_id']}", "HAS_CATEGORY", cid, source=row.get("source_name"), category_type=row.get("category_type")))

    for row in context_tags:
        tag_id = f"context:{slugify(row['tag_type'])}:{slugify(row['tag_value'])}"
        add_node(node(tag_id, "ContextTag", row["tag_value"], tag_type=row.get("tag_type")))
        edges.append(edge(f"store:{row['store_id']}", "MATCHES_CONTEXT", tag_id, source=row.get("source_name")))

    for row in service_options:
        service_id = f"service:{slugify(row['service_option'])}"
        add_node(node(service_id, "ServiceOption", row["service_option"]))
        edges.append(edge(f"store:{row['store_id']}", "OFFERS_SERVICE", service_id, value=row.get("value"), source=row.get("source_name")))

    for row in aspects:
        aspect_id = f"aspect:{slugify(row['aspect_name'])}"
        add_node(node(aspect_id, "Aspect", row["aspect_name"]))
        edges.append(edge(
            f"store:{row['store_id']}",
            "HAS_ASPECT_SENTIMENT",
            aspect_id,
            sentiment=row.get("aspect_sentiment"),
            mention_count=row.get("mention_count"),
            positive=row.get("positive_mentions"),
            negative=row.get("negative_mentions"),
            neutral=row.get("neutral_mentions"),
            evidence_sources=row.get("evidence_sources"),
        ))

    for row in reviews:
        review_id = f"review:{row['review_id']}"
        add_node(node(review_id, "Review", row.get("review_text", "")[:80], source=row.get("source_name"), rating=row.get("rating_5"), sentiment=row.get("sentiment")))
        edges.append(edge(review_id, "REVIEWS", f"store:{row['store_id']}", rated_at=row.get("rated_at")))

    for row in source_map:
        source_id = f"source:{slugify(row['source_name'])}"
        add_node(node(source_id, "DataSource", row["source_name"]))
        edges.append(edge(f"store:{row['store_id']}", "HAS_SOURCE_RECORD", source_id, source_key=row.get("source_store_key"), match_confidence=row.get("match_confidence")))

    for row in tables["user_profile"]:
        add_node(node(f"user:{row['user_id']}", "User", row["display_name"], dietary_profile=row.get("dietary_profile"), default_budget_vnd=row.get("default_budget_vnd")))

    for row in tables["user_session_context"]:
        session_id = f"session:{row['session_id']}"
        add_node(node(session_id, "UserSession", row["session_id"], intent=row.get("intent"), time_context=row.get("time_context"), budget_max_vnd=row.get("budget_max_vnd")))
        edges.append(edge(f"user:{row['user_id']}", "HAS_SESSION", session_id))
        intent_id = f"context:intent:{slugify(row['intent'])}"
        add_node(node(intent_id, "ContextTag", row["intent"], tag_type="intent"))
        edges.append(edge(session_id, "REQUESTS_CONTEXT", intent_id, desired_service=row.get("desired_service"), radius_m=row.get("radius_m")))

    for row in tables["user_preference"]:
        pref_id = f"preference:{row['user_id']}:{slugify(row['preference_type'])}:{slugify(row['preference_value'])}"
        add_node(node(pref_id, "UserPreference", row["preference_value"], preference_type=row.get("preference_type"), weight=row.get("weight")))
        edges.append(edge(f"user:{row['user_id']}", "HAS_PREFERENCE", pref_id, source=row.get("source")))

    for row in tables["store_menu_item"]:
        item_id = f"menu_item:{row['menu_item_id']}"
        add_node(node(item_id, "MenuItem", row["item_name"], category=row.get("item_category"), price_vnd=row.get("price_vnd"), dietary_tag=row.get("dietary_tag"), spicy_level=row.get("spicy_level")))
        edges.append(edge(f"store:{row['store_id']}", "SELLS_MENU_ITEM", item_id, source=row.get("source_name")))

    for row in tables["user_store_interaction"]:
        edges.append(edge(f"user:{row['user_id']}", "INTERACTED_WITH", f"store:{row['store_id']}", interaction_type=row.get("interaction_type"), rating_5=row.get("rating_5"), session_id=row.get("session_id"), interacted_at=row.get("interacted_at")))

    for row in tables["normalized_location"]:
        loc_id = f"location:{row['location_id']}"
        add_node(node(loc_id, "Location", row["canonical_address"], street=row.get("street"), ward=row.get("ward"), district=row.get("district"), city=row.get("city"), lat=row.get("lat"), lng=row.get("lng")))
        edges.append(edge(f"store:{row['store_id']}", "HAS_NORMALIZED_LOCATION", loc_id))

    for row in tables["source_match_quality"]:
        quality_id = f"match_quality:{row['store_id']}:{slugify(row['source_name'])}"
        add_node(node(quality_id, "SourceMatchQuality", f"{row['store_id']}:{row['source_name']}", confidence=row.get("match_confidence_recomputed"), is_suspect=row.get("is_suspect"), geo_distance_m=row.get("geo_distance_m")))
        edges.append(edge(f"store:{row['store_id']}", "HAS_SOURCE_MATCH_QUALITY", quality_id))

    return list(nodes.values()), edges


def main() -> None:
    fields_by_table = {
        "store_master": ["store_id", "canonical_name", "address", "lat", "lng", "ward", "district", "city", "primary_category", "status", "be_open_time", "be_close_time", "median_price_vnd", "be_rating_avg", "be_rating_count", "google_name", "google_query_name", "google_query_address", "google_address", "google_lat", "google_lng", "google_rating", "google_review_count", "google_phone", "google_website", "google_place_id", "google_data_id", "google_open_state", "google_open_time", "google_close_time", "google_match_method", "google_match_confidence", "google_match_score"],
        "store_category": ["store_id", "category_type", "category_value", "source_name"],
        "store_context_tag": ["store_id", "tag_type", "tag_value", "source_name"],
        "store_service_option": ["store_id", "service_option", "value", "source_name"],
        "store_aspect_agg": ["store_id", "aspect_name", "aspect_sentiment", "mention_count", "positive_mentions", "negative_mentions", "neutral_mentions", "evidence_sources"],
        "review_fact": ["review_id", "store_id", "source_name", "rated_at", "rating_5", "review_text", "sentiment"],
        "store_source_map": ["store_id", "source_name", "source_store_key", "source_url", "source_name_raw", "address_raw", "match_confidence", "note"],
        "user_profile": ["user_id", "display_name", "home_district", "dietary_profile", "default_budget_vnd"],
        "user_session_context": ["session_id", "user_id", "query_time", "current_lat", "current_lng", "radius_m", "party_size", "budget_min_vnd", "budget_max_vnd", "intent", "desired_service", "weather_context", "time_context"],
        "user_preference": ["user_id", "preference_type", "preference_value", "weight", "source"],
        "store_menu_item": ["menu_item_id", "store_id", "item_name", "item_category", "price_vnd", "is_signature", "spicy_level", "dietary_tag", "source_name"],
        "user_store_interaction": ["interaction_id", "user_id", "store_id", "interaction_type", "interacted_at", "rating_5", "session_id", "source"],
        "normalized_location": ["location_id", "store_id", "canonical_address", "street", "ward", "district", "city", "lat", "lng", "area_tag"],
        "source_match_quality": ["store_id", "source_name", "match_confidence_recomputed", "geo_distance_m", "name_similarity", "address_similarity", "is_suspect", "note"],
    }

    tables = build_tables_from_sources()

    for table_name, rows in tables.items():
        write_csv(KG_DIR / f"{table_name}.csv", rows, fields_by_table[table_name])

    nodes, edges = build_graph(tables)
    write_csv(GRAPH_DIR / "nodes.csv", nodes, ["node_id", "label", "name", "properties"])
    write_csv(GRAPH_DIR / "edges.csv", edges, ["source_id", "relation", "target_id", "properties"])

    triples = [
        {"subject": row["source_id"], "predicate": row["relation"], "object": row["target_id"], "properties": row["properties"]}
        for row in edges
    ]
    write_csv(GRAPH_DIR / "triples.csv", triples, ["subject", "predicate", "object", "properties"])

    print(f"Wrote {len(tables)} KG tables from source files to {KG_DIR}")
    print(f"Wrote KG graph with {len(nodes)} nodes and {len(edges)} edges to {GRAPH_DIR}")


if __name__ == "__main__":
    main()
