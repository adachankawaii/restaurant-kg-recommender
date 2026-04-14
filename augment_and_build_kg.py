from __future__ import annotations

import csv
import json
import math
import re
import unicodedata
from pathlib import Path


BASE_DIR = Path(".")
KG_DIR = BASE_DIR / "kg_tables_all"
GRAPH_DIR = KG_DIR / "kg_graph"


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
    stores = read_csv(KG_DIR / "store_master.csv")
    mock_tables = build_mock_tables(stores)

    fields_by_table = {
        "user_profile": ["user_id", "display_name", "home_district", "dietary_profile", "default_budget_vnd"],
        "user_session_context": ["session_id", "user_id", "query_time", "current_lat", "current_lng", "radius_m", "party_size", "budget_min_vnd", "budget_max_vnd", "intent", "desired_service", "weather_context", "time_context"],
        "user_preference": ["user_id", "preference_type", "preference_value", "weight", "source"],
        "store_menu_item": ["menu_item_id", "store_id", "item_name", "item_category", "price_vnd", "is_signature", "spicy_level", "dietary_tag", "source_name"],
        "user_store_interaction": ["interaction_id", "user_id", "store_id", "interaction_type", "interacted_at", "rating_5", "session_id", "source"],
        "normalized_location": ["location_id", "store_id", "canonical_address", "street", "ward", "district", "city", "lat", "lng", "area_tag"],
        "source_match_quality": ["store_id", "source_name", "match_confidence_recomputed", "geo_distance_m", "name_similarity", "address_similarity", "is_suspect", "note"],
    }

    for table_name, rows in mock_tables.items():
        write_csv(KG_DIR / f"{table_name}.csv", rows, fields_by_table[table_name])

    nodes, edges = build_graph(mock_tables)
    write_csv(GRAPH_DIR / "nodes.csv", nodes, ["node_id", "label", "name", "properties"])
    write_csv(GRAPH_DIR / "edges.csv", edges, ["source_id", "relation", "target_id", "properties"])

    triples = [
        {"subject": row["source_id"], "predicate": row["relation"], "object": row["target_id"], "properties": row["properties"]}
        for row in edges
    ]
    write_csv(GRAPH_DIR / "triples.csv", triples, ["subject", "predicate", "object", "properties"])

    print(f"Wrote {len(mock_tables)} mock tables to {KG_DIR}")
    print(f"Wrote KG graph with {len(nodes)} nodes and {len(edges)} edges to {GRAPH_DIR}")


if __name__ == "__main__":
    main()
