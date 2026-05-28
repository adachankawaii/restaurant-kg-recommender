from __future__ import annotations

import ast
import hashlib
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


def _is_nan(x: Any) -> bool:
    try:
        return bool(pd.isna(x))
    except ValueError:
        return False


def repair_mojibake(x: str) -> str:
    try:
        repaired = x.encode("latin1").decode("utf-8")
    except UnicodeError:
        return x
    markers = ("Ã", "Â", "Æ", "º", "»")
    if sum(x.count(marker) for marker in markers) > sum(repaired.count(marker) for marker in markers):
        return repaired
    return x


def normalize_text(x: Any) -> str:
    if x is None or _is_nan(x):
        return ""
    x = repair_mojibake(str(x).strip()).lower()
    x = unicodedata.normalize("NFKC", x)
    return re.sub(r"\s+", " ", x)


def slugify_vn(x: Any) -> str:
    x = normalize_text(x)
    x = "".join(c for c in unicodedata.normalize("NFD", x) if unicodedata.category(c) != "Mn")
    x = x.replace("\u0111", "d").replace("\u0110", "D")
    x = x.replace("đ", "d")
    return re.sub(r"[^a-z0-9]+", "-", x).strip("-")


def to_float(x: Any) -> Optional[float]:
    if x is None or _is_nan(x) or str(x).strip() == "":
        return None
    try:
        return float(x)
    except Exception:
        s = str(x).replace(".", "").replace(",", ".")
        s = re.sub(r"[^0-9.\-]", "", s)
        try:
            return float(s)
        except Exception:
            return None


def to_int(x: Any) -> Optional[int]:
    v = to_float(x)
    return None if v is None else int(v)


def parse_jsonish(x: Any) -> Any:
    if x is None or _is_nan(x):
        return None
    if isinstance(x, (list, dict)):
        return x
    s = str(x).strip()
    if not s:
        return None
    for fn in (json.loads, ast.literal_eval):
        try:
            return fn(s)
        except Exception:
            pass
    return s


def split_semi(x: Any) -> list[str]:
    if x is None or _is_nan(x):
        return []
    if isinstance(x, list):
        return [str(i).strip() for i in x if str(i).strip()]
    parsed = parse_jsonish(x)
    if isinstance(parsed, list):
        return [str(i).strip() for i in parsed if str(i).strip()]
    return [p.strip() for p in re.split(r"[;|,/]", str(x)) if p and p.strip()]


HANOI_DISTRICTS = [
    "Ba Dinh", "Hoan Kiem", "Tay Ho", "Long Bien", "Cau Giay", "Dong Da", "Hai Ba Trung",
    "Hoang Mai", "Thanh Xuan", "Nam Tu Liem", "Bac Tu Liem", "Ha Dong", "Son Tay",
    "Ba Vi", "Chuong My", "Dan Phuong", "Dong Anh", "Gia Lam", "Hoai Duc", "Me Linh",
    "My Duc", "Phu Xuyen", "Phuc Tho", "Quoc Oai", "Soc Son", "Thach That", "Thanh Oai",
    "Thanh Tri", "Thuong Tin", "Ung Hoa",
]
URBAN_DISTRICTS = {
    "Ba Dinh", "Hoan Kiem", "Tay Ho", "Long Bien", "Cau Giay", "Dong Da", "Hai Ba Trung",
    "Hoang Mai", "Thanh Xuan", "Nam Tu Liem", "Bac Tu Liem", "Ha Dong",
}
DISTRICT_ALIASES = {slugify_vn(x): (f"Quan {x}" if x in URBAN_DISTRICTS else x) for x in HANOI_DISTRICTS}
URBAN_DISTRICT_KEYS = {slugify_vn(x) for x in URBAN_DISTRICTS}


def infer_district(address: Any) -> Optional[str]:
    s = slugify_vn(address)
    aliases = sorted(DISTRICT_ALIASES.items(), key=lambda item: len(item[0]), reverse=True)
    for key, label in aliases:
        if key and re.search(rf"(^|-)(quan|q|huyen|thi-xa)-{re.escape(key)}($|-)", s):
            return label
    for key, label in aliases:
        if key in URBAN_DISTRICT_KEYS and re.search(rf"(^|-){re.escape(key)}($|-)", s):
            return label
    return None


def price_band_from_bounds(price_min: Any, price_max: Any) -> Optional[str]:
    vals = [to_float(price_min), to_float(price_max)]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    hi = max(vals)
    if hi <= 50000:
        return "budget"
    if hi <= 120000:
        return "mid"
    return "premium"


def price_band_from_menu_prices(prices: Any) -> Optional[str]:
    if prices is None:
        raw_values = []
    else:
        raw_values = list(prices)
    vals = [to_float(x) for x in raw_values]
    vals = [v for v in vals if v is not None and v > 0]
    if not vals:
        return None
    median = float(np.median(vals))
    budget_ratio = sum(v <= 50000 for v in vals) / len(vals)
    premium_ratio = sum(v > 120000 for v in vals) / len(vals)
    if median <= 50000 or budget_ratio >= 0.60:
        return "budget"
    if median <= 120000 and premium_ratio < 0.35:
        return "mid"
    return "premium"


def normalize_dish_family(name: Any) -> str:
    slug = slugify_vn(name)
    if not slug:
        return ""
    rules = [
        (["ga-ran"], "gà rán"),
        (["com-tam"], "cơm tấm"),
        (["com-ga"], "cơm gà"),
        (["com-rang", "com-chien"], "cơm rang"),
        (["com"], "cơm"),
        (["bun-cha"], "bún chả"),
        (["bun-bo"], "bún bò"),
        (["bun-ca"], "bún cá"),
        (["bun-rieu"], "bún riêu"),
        (["bun"], "bún"),
        (["pho"], "phở"),
        (["banh-cuon"], "bánh cuốn"),
        (["banh-mi"], "bánh mì"),
        (["ga"], "gà"),
        (["mi-cay"], "mì cay"),
        (["mi"], "mì"),
        (["mien"], "miến"),
        (["chao"], "cháo"),
        (["xoi"], "xôi"),
        (["lau"], "lẩu"),
        (["nuong"], "nướng"),
        (["tra-sua"], "trà sữa"),
        (["cafe", "ca-phe"], "cà phê"),
        (["nuoc-ep"], "nước ép"),
    ]
    for needles, family in rules:
        if any(n in slug for n in needles):
            return family
    stop = {"combo", "set", "size", "phan", "them", "dac", "biet", "full", "mix", "coca", "cola", "pepsi", "sprite", "dasani", "lon", "chai"}
    toks = [t for t in slug.split("-") if t and t not in stop and not t.isdigit()]
    return " ".join(toks[:2]) if toks else slug.replace("-", " ")


def haversine_km(lat1: Any, lng1: Any, lat2: Any, lng2: Any) -> Optional[float]:
    lat1, lng1, lat2, lng2 = map(to_float, [lat1, lng1, lat2, lng2])
    if None in (lat1, lng1, lat2, lng2):
        return None
    radius = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return round(2 * radius * math.asin(math.sqrt(a)), 3)


def distance_score(distance_km: Any, decay_km: float = 3.0) -> float:
    d = to_float(distance_km)
    if d is None:
        return 0.0
    return 1.0 / (1.0 + max(d, 0.0) / max(decay_km, 1e-9))


def load_raw_data(restaurants_path: Path, menu_path: Path, foody_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return pd.read_csv(restaurants_path), pd.read_csv(menu_path), pd.read_csv(foody_path)


def canonicalize_befood_restaurants(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["store_id"] = df["restaurant_id"].astype("Int64").astype(str)
    out["store_key"] = out["store_id"]
    out["store_name"] = df["restaurant_name"].fillna("").astype(str)
    out["query_name"] = out["store_name"]
    out["address"] = df.get("address", pd.Series([None] * len(df)))
    out["query_address"] = out["address"]
    out["district"] = out["address"].apply(infer_district)
    out["city"] = "Ha Noi"
    out["lat"] = df.get("latitude", pd.Series([None] * len(df))).apply(to_float)
    out["lng"] = df.get("longitude", pd.Series([None] * len(df))).apply(to_float)
    out["gmaps_rating"] = df.get("rating", pd.Series([None] * len(df))).apply(to_float)
    out["gmaps_review_count"] = df.get("review_count", pd.Series([None] * len(df))).apply(to_int)
    out["price_min"] = df.get("price_min", pd.Series([None] * len(df))).apply(to_float)
    out["price_max"] = df.get("price_max", pd.Series([None] * len(df))).apply(to_float)
    out["price_band"] = [price_band_from_bounds(a, b) for a, b in zip(out["price_min"], out["price_max"])]
    out["categories"] = df.get("categories_text", pd.Series([None] * len(df))).apply(split_semi)
    out["matched_terms"] = df.get("matched_terms_text", pd.Series([None] * len(df))).apply(split_semi)
    out["opening_hours_raw"] = df.get("opening_hours")
    out["delivery_time"] = df.get("delivery_time", pd.Series([None] * len(df))).apply(to_float)
    out["image_url"] = df.get("image_url")
    out["menu_count"] = df.get("menu_count", pd.Series([None] * len(df))).apply(to_int)
    out["comment_count"] = df.get("comment_count", pd.Series([None] * len(df))).apply(to_int)
    out["source"] = df.get("source", pd.Series(["befood"] * len(df))).fillna("befood")
    out["atmosphere"] = [[] for _ in range(len(out))]
    out["crowd"] = [[] for _ in range(len(out))]
    out["name_norm"] = out["store_name"].apply(slugify_vn)
    out["addr_norm"] = out["address"].apply(slugify_vn)
    return out


def canonicalize_feedback_from_befood(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        store_id = str(int(r["restaurant_id"])) if pd.notna(r.get("restaurant_id")) else ""
        store_name = str(r.get("restaurant_name") or "")
        comments = parse_jsonish(r.get("comments_list"))
        if isinstance(comments, str):
            comments = [comments]
        if not isinstance(comments, list):
            comments = []
        for i, comment in enumerate(comments):
            feedback = str(comment or "").strip()
            if not feedback:
                continue
            rows.append({
                "store_id": store_id,
                "store_key": store_id,
                "store_name": store_name,
                "rated_at": None,
                "rating": to_float(r.get("rating")) or 3.0,
                "feedback": feedback,
                "source": "befood_comment",
                "review_id": hashlib.md5(f"{store_id}|{i}|{feedback}".encode("utf-8")).hexdigest()[:16],
                "name_norm": slugify_vn(store_name),
            })
    return pd.DataFrame(rows)


def canonicalize_menu_items(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["store_id"] = df["restaurant_id"].astype("Int64").astype(str)
    out["store_key"] = out["store_id"]
    out["store_name"] = df["restaurant_name"].fillna("").astype(str)
    out["category_id"] = df.get("category_id", pd.Series([None] * len(df))).astype("Int64").astype(str)
    out["category_name"] = df.get("category_name", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    out["menu_item_id"] = df["restaurant_item_id"].astype("Int64").astype(str)
    out["item_name"] = df["item_name"].fillna("").astype(str).str.strip()
    out["item_details"] = df.get("item_details", pd.Series([""] * len(df))).fillna("").astype(str).str.strip()
    out["price"] = df.get("price", pd.Series([None] * len(df))).apply(to_float)
    out["old_price"] = df.get("old_price", pd.Series([None] * len(df))).apply(to_float)
    out["order_count"] = df.get("order_count", pd.Series([0] * len(df))).apply(to_int).fillna(0).astype(int)
    out["like_count"] = df.get("like_count", pd.Series([0] * len(df))).apply(to_int).fillna(0).astype(int)
    out["dislike_count"] = df.get("dislike_count", pd.Series([0] * len(df))).apply(to_int).fillna(0).astype(int)
    out["category_position"] = df.get("category_position", pd.Series([None] * len(df))).apply(to_int)
    out["item_position"] = df.get("item_position", pd.Series([None] * len(df))).apply(to_int)
    out["item_image"] = df.get("item_image")
    out["item_norm"] = out["item_name"].apply(slugify_vn)
    return out[out["item_name"].ne("")].copy()


def canonicalize_foody(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()
    out["input_store_id"] = df["input_store_id"].astype("Int64").astype(str)
    out["input_store_name"] = df["input_store_name"]
    out["foody_name"] = df.get("name")
    out["foody_address"] = df.get("address")
    out["district"] = df.get("district")
    out["area"] = df.get("area")
    out["city"] = df.get("city")
    out["foody_lat"] = df.get("lat").apply(to_float)
    out["foody_lng"] = df.get("lng").apply(to_float)
    out["foody_rating"] = df.get("avg_rating").apply(to_float)
    out["foody_review_count"] = df.get("total_review").apply(to_int)
    out["foody_price_band"] = [price_band_from_bounds(a, b) for a, b in zip(df.get("price_min"), df.get("price_max"))]
    out["categories"] = df.get("categories", pd.Series([None] * len(df))).apply(split_semi)
    out["cuisines"] = df.get("cuisines", pd.Series([None] * len(df))).apply(split_semi)
    out["audiences"] = df.get("audiences", pd.Series([None] * len(df))).apply(split_semi)
    out["opening_hours_foody"] = df.get("opening_hours")
    out["store_key"] = out["input_store_id"].astype(str)
    out["name_norm"] = out["input_store_name"].apply(slugify_vn)
    return out


def merge_list_cols(*cols: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for col in cols:
        vals = col if isinstance(col, list) else []
        for x in vals:
            x = str(x).strip()
            key = x.lower()
            if x and key not in seen:
                out.append(x)
                seen.add(key)
    return out


def canonicalize_district_value(value: Any) -> Optional[str]:
    if value is None or _is_nan(value) or str(value).strip() == "":
        return None
    return infer_district(value) or str(value).strip()


@dataclass
class PreparedData:
    summary: pd.DataFrame
    feedback: pd.DataFrame
    menu_items: pd.DataFrame
    dish_families: pd.DataFrame


def build_menu_dish_families(menu_df: pd.DataFrame) -> pd.DataFrame:
    if menu_df.empty:
        return pd.DataFrame(columns=["store_key", "dish_family", "total_menu_items", "avg_price", "order_count", "like_count", "menu_item_ids", "example_items"])
    df = menu_df.copy()
    df["dish_family"] = df["item_name"].apply(normalize_dish_family)
    df = df[df["dish_family"].ne("")].copy()
    return df.groupby(["store_key", "dish_family"]).agg(
        total_menu_items=("menu_item_id", "count"),
        avg_price=("price", "mean"),
        order_count=("order_count", "sum"),
        like_count=("like_count", "sum"),
        menu_item_ids=("menu_item_id", lambda s: [str(x) for x in s]),
        example_items=("item_name", lambda s: list(dict.fromkeys([str(x) for x in s if str(x).strip()]))[:5]),
    ).reset_index()


def prepare_data(
    raw_befood: pd.DataFrame,
    raw_menu: pd.DataFrame,
    raw_foody: pd.DataFrame,
    user_lat: Optional[float] = None,
    user_lng: Optional[float] = None,
    distance_decay_km: float = 3.0,
) -> PreparedData:
    restaurants_base = canonicalize_befood_restaurants(raw_befood)
    feedback = canonicalize_feedback_from_befood(raw_befood)
    menu_items = canonicalize_menu_items(raw_menu)
    foody = canonicalize_foody(raw_foody)

    restaurants = restaurants_base.merge(foody.drop_duplicates("store_key"), on="store_key", how="left", suffixes=("", "_foody"))
    restaurants["name"] = restaurants["store_name"].fillna(restaurants["foody_name"]).fillna(restaurants["query_name"])
    restaurants["address_final"] = restaurants["address"].fillna(restaurants["foody_address"]).fillna(restaurants["query_address"])
    restaurants["district_final"] = restaurants["district_foody"].apply(canonicalize_district_value).fillna(restaurants["district"])
    restaurants["city_final"] = restaurants["city_foody"].fillna(restaurants["city"]).fillna("Ha Noi")
    restaurants["lat_final"] = restaurants["lat"].fillna(restaurants["foody_lat"])
    restaurants["lng_final"] = restaurants["lng"].fillna(restaurants["foody_lng"])
    restaurants["source_price_band_final"] = restaurants["price_band"].fillna(restaurants["foody_price_band"])

    menu_categories_by_store = menu_items.groupby("store_key")["category_name"].apply(lambda s: sorted({x for x in s if x})).to_dict()
    top_items = (
        menu_items.sort_values(["store_key", "order_count", "like_count"], ascending=[True, False, False])
        .groupby("store_key").head(12).groupby("store_key")["item_name"].apply(list).to_dict()
    )
    price_stats = menu_items.groupby("store_key").agg(
        menu_item_count=("menu_item_id", "count"),
        menu_price_min=("price", "min"),
        menu_price_max=("price", "max"),
        menu_price_median=("price", "median"),
        menu_budget_item_ratio=("price", lambda s: float((pd.to_numeric(s, errors="coerce") <= 50000).mean())),
        menu_price_band=("price", price_band_from_menu_prices),
    ).reset_index()
    dish_families = build_menu_dish_families(menu_items)
    dish_families_by_store = dish_families.groupby("store_key")["dish_family"].apply(list).to_dict() if not dish_families.empty else {}

    restaurants["categories_final"] = [
        merge_list_cols(a, b, menu_categories_by_store.get(k, []), terms)
        for a, b, k, terms in zip(restaurants["categories"], restaurants["categories_foody"], restaurants["store_key"], restaurants["matched_terms"])
    ]
    restaurants["cuisines_final"] = restaurants["cuisines"].apply(lambda x: x if isinstance(x, list) else [])
    restaurants["audiences_final"] = restaurants["audiences"].apply(lambda x: x if isinstance(x, list) else [])

    summary = pd.DataFrame({
        "store_key": restaurants["store_key"],
        "name": restaurants["name"],
        "address": restaurants["address_final"],
        "district": restaurants["district_final"],
        "city": restaurants["city_final"],
        "gmaps_rating": restaurants["gmaps_rating"],
        "foody_rating": restaurants["foody_rating"],
        "review_count": restaurants["gmaps_review_count"].fillna(restaurants["foody_review_count"]),
        "source_price_band": restaurants["source_price_band_final"],
        "price_min": restaurants["price_min"],
        "price_max": restaurants["price_max"],
        "categories": restaurants["categories_final"],
        "cuisines": restaurants["cuisines_final"],
        "atmosphere": restaurants["atmosphere"],
        "audiences": restaurants["audiences_final"],
        "opening_hours": restaurants["opening_hours_raw"].fillna(restaurants["opening_hours_foody"]),
        "delivery_time": restaurants["delivery_time"],
        "image_url": restaurants["image_url"],
        "top_menu_items": restaurants["store_key"].map(top_items).apply(lambda x: x if isinstance(x, list) else []),
        "dish_families": restaurants["store_key"].map(dish_families_by_store).apply(lambda x: x if isinstance(x, list) else []),
        "lat": restaurants["lat_final"],
        "lng": restaurants["lng_final"],
    }).merge(price_stats, on="store_key", how="left")

    summary["menu_item_count"] = summary["menu_item_count"].fillna(0).astype(int)
    summary["price_band"] = summary["menu_price_band"].fillna(summary["source_price_band"])
    summary["rating"] = summary["gmaps_rating"].fillna(summary["foody_rating"])
    if user_lat is not None and user_lng is not None:
        summary["distance_km"] = [haversine_km(user_lat, user_lng, lat, lng) for lat, lng in zip(summary["lat"], summary["lng"])]
    else:
        summary["distance_km"] = None
    summary["distance_score"] = summary["distance_km"].apply(lambda x: distance_score(x, distance_decay_km))

    return PreparedData(summary=summary, feedback=feedback, menu_items=menu_items, dish_families=dish_families)
