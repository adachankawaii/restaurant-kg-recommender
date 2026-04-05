from pathlib import Path
import json
import math
import re
import unicodedata
from difflib import SequenceMatcher

import pandas as pd


# =========================================================
# CONFIG
# =========================================================
BASE_DIR = Path(".")  # đổi nếu cần
BE_STORES_FILE = BASE_DIR / "be_stores.csv"
BE_RATINGS_FILE = BASE_DIR / "be_ratings.csv"
FOODY_FILE = BASE_DIR / "foody_top10_comments_flat.csv"
GOOGLE_FILE = BASE_DIR / "be_google_maps.json"

OUTPUT_DIR = BASE_DIR / "kg_tables_all"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# None = build tất cả store
# ví dụ: ONLY_STORE_IDS = [28819, 32441]
ONLY_STORE_IDS = None


# =========================================================
# TEXT / NORMALIZE HELPERS
# =========================================================
def safe_str(x):
    if pd.isna(x) or x is None:
        return None
    s = str(x).strip()
    return s if s else None


def strip_accents(text: str) -> str:
    if text is None:
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in text if not unicodedata.combining(ch))


def norm_text(text: str) -> str:
    text = strip_accents(text).lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def slugify(text: str) -> str:
    return norm_text(text).replace(" ", "_")


def string_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, norm_text(a), norm_text(b)).ratio()


def token_jaccard(a: str, b: str) -> float:
    ta = set(norm_text(a).split())
    tb = set(norm_text(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


# =========================================================
# DATE / TIME HELPERS
# =========================================================
def parse_mixed_datetime(x):
    if pd.isna(x) or str(x).strip() == "":
        return pd.NaT

    s = str(x).strip()
    fmts = [
        "%H:%M %d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d/%m/%y",
        "%m/%d/%y",
        "%d/%m/%Y %I:%M",
    ]
    for fmt in fmts:
        try:
            return pd.to_datetime(s, format=fmt, dayfirst=True, errors="raise")
        except Exception:
            pass

    return pd.to_datetime(s, dayfirst=True, errors="coerce")


def parse_be_time_only(x):
    dt = parse_mixed_datetime(x)
    if pd.isna(dt):
        return None
    return dt.strftime("%H:%M")


def parse_google_open_close(opening_hours):
    """
    Cố lấy khung giờ mở/đóng phổ biến từ google opening_hours.
    - Nếu tất cả các ngày cùng khung giờ, trả về open_time/close_time đó
    - Nếu 'Mở cửa cả ngày', trả 00:00 / 23:59
    """
    if not isinstance(opening_hours, dict):
        return None, None, None

    open_state = safe_str(opening_hours.get("open_state"))
    hours = opening_hours.get("hours", [])

    if open_state and "mở cả ngày" in norm_text(open_state):
        return open_state, "00:00", "23:59"

    parsed_ranges = []
    for item in hours:
        if not isinstance(item, dict):
            continue
        for _, val in item.items():
            if val is None:
                continue
            v = str(val).strip()
            if "mở cửa cả ngày" in norm_text(v):
                parsed_ranges.append(("00:00", "23:59"))
                continue

            m = re.match(r"^\s*(\d{1,2}:\d{2})\s*[–-]\s*(\d{1,2}:\d{2})\s*$", v)
            if m:
                parsed_ranges.append((m.group(1), m.group(2)))

    if not parsed_ranges:
        return open_state, None, None

    most_common = pd.Series(parsed_ranges).value_counts().index[0]
    return open_state, most_common[0], most_common[1]


# =========================================================
# ADDRESS / PHONE / GEO HELPERS
# =========================================================
def normalize_phone(phone):
    if phone is None:
        return None
    digits = re.sub(r"\D", "", str(phone))
    if not digits:
        return None
    if digits.startswith("84") and len(digits) >= 10:
        digits = "0" + digits[2:]
    return digits


def extract_house_number(address):
    if address is None:
        return None

    a = norm_text(address)
    tokens = a.split()

    skip_words = {
        "so", "số", "kiot", "kiôt", "ngo", "ng", "hem",
        "pho", "phố", "p", "duong", "đường"
    }

    for tok in tokens[:6]:
        if tok in skip_words:
            continue
        if any(ch.isdigit() for ch in tok):
            return tok

    return None


def strip_house_number(address):
    if address is None:
        return ""
    a = norm_text(address)
    house = extract_house_number(a)
    if house:
        a = re.sub(rf"\b{re.escape(house)}\b", " ", a, count=1)
    a = re.sub(r"\s+", " ", a).strip()
    return a


def extract_location_parts(address: str):
    if pd.isna(address) or str(address).strip() == "":
        return None, None, None

    parts = [p.strip() for p in str(address).split(",") if p.strip()]
    parts_n = [norm_text(p) for p in parts]

    ward = None
    district = None
    city = None

    if len(parts) >= 1:
        city = parts[-1]

    for p_raw, p in zip(parts, parts_n):
        if any(k in p for k in [
            "quan ", "district", "huyen ", "thi xa",
            "hai ba trung", "dong da", "cau giay", "ba dinh",
            "hoan kiem", "thanh xuan", "ha dong", "bac tu liem",
            "nam tu liem", "bach mai"
        ]):
            district = p_raw
        if any(k in p for k in [
            "phuong ", "ward", "bach khoa", "dong tam", "la khe", "mo lao", "van quan"
        ]):
            ward = p_raw

    if district is None and len(parts) >= 2:
        district = parts[-2]
    if ward is None and len(parts) >= 3:
        ward = parts[-3]

    return ward, district, city


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    p1 = math.radians(float(lat1))
    p2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))

    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def geo_score_from_distance(dist_m):
    if dist_m is None:
        return 0.0
    if dist_m <= 40:
        return 1.00
    if dist_m <= 80:
        return 0.95
    if dist_m <= 150:
        return 0.88
    if dist_m <= 300:
        return 0.75
    if dist_m <= 500:
        return 0.55
    if dist_m <= 800:
        return 0.35
    if dist_m <= 1200:
        return 0.18
    return 0.0


# =========================================================
# REVIEW / ASPECT HELPERS
# =========================================================
def detect_promo(text: str) -> int:
    t = norm_text(text)
    promo_patterns = [
        r"\buu dai\b", r"\bvoucher\b", r"\bgiam\b", r"\btang\b",
        r"\bkhuyen mai\b", r"\bcombo\b", r"\bmua 2 tang 1\b",
        r"\bshorten asia\b", r"\blink de nhan\b"
    ]
    return int(any(re.search(p, t) for p in promo_patterns))


def rating_to_sentiment(rating_5):
    if pd.isna(rating_5):
        return "unknown"
    if rating_5 >= 4:
        return "positive"
    if rating_5 <= 2:
        return "negative"
    return "neutral"


ASPECT_RULES = {
    "taste": [
        "ngon", "dở", "do uong", "ca phe", "tra", "freeze", "matcha", "dam vi",
        "vua mieng", "qua ngot", "nhat", "thom", "mi cay", "banh", "nuoc dung"
    ],
    "space": [
        "khong gian", "rong", "rong rai", "thoang", "view", "cho ngoi", "on ao", "dong", "chill"
    ],
    "cleanliness": [
        "sach", "sach se", "ban", "ve sinh", "do rac", "boi ban"
    ],
    "staff_service": [
        "nhan vien", "phuc vu", "thai do", "chu dao", "nhiet tinh", "chao khach", "ho tro"
    ],
    "delivery_accuracy": [
        "giao nham", "thieu do", "dong goi", "phan hoi", "ship", "don hang"
    ],
    "value_for_money": [
        "dang dong tien", "gia", "hop ly", "dat", "re", "phai chang"
    ],
    "work_friendly": [
        "wifi", "o dien", "laptop", "hoc tap", "lam viec", "deadline", "ngoi hoc"
    ],
}

def extract_aspects(text: str):
    t = norm_text(text)
    found = []
    for aspect, kws in ASPECT_RULES.items():
        if any(kw in t for kw in kws):
            found.append(aspect)
    return found


def norm_category_be(cat: str) -> str:
    c = norm_text(cat)
    if any(k in c for k in ["ca phe", "tra"]):
        return "cafe_tea"
    if any(k in c for k in ["bun", "pho", "my", "mi", "chao"]):
        return "noodle_rice_fastcasual"
    return slugify(cat)


def flatten_google_bool_map(d: dict):
    if not d:
        return []
    rows = []
    for k, v in d.items():
        rows.append((slugify(k), int(bool(v))))
    return rows


# =========================================================
# LOAD DATA
# =========================================================
def load_data():
    be_stores = pd.read_csv(BE_STORES_FILE)
    be_ratings = pd.read_csv(BE_RATINGS_FILE)
    foody = pd.read_csv(FOODY_FILE)

    with open(GOOGLE_FILE, "r", encoding="utf-8") as f:
        google_records = json.load(f)

    # map chính: join thẳng theo id == store_id
    google_by_id = {}
    for rec in google_records:
        rid = rec.get("id")
        if rid is None:
            continue
        try:
            google_by_id[int(rid)] = rec
        except Exception:
            pass

    return be_stores, be_ratings, foody, google_records, google_by_id


# =========================================================
# FALLBACK MATCHER
# Chỉ dùng khi JSON mới không có đúng id tương ứng
# =========================================================
def fallback_choose_google_candidate(google_records, be_name, be_address, be_lat, be_lng):
    be_house = extract_house_number(be_address)
    be_street = strip_house_number(be_address)

    candidates = []

    for rec in google_records:
        g_name = safe_str(rec.get("name")) or ""
        g_query_name = safe_str(rec.get("query_name")) or ""
        g_addr = safe_str(rec.get("address")) or ""
        g_query_addr = safe_str(rec.get("query_address")) or ""

        loc = rec.get("location", {}) or {}
        g_lat = loc.get("latitude")
        g_lng = loc.get("longitude")
        dist_m = None
        if g_lat is not None and g_lng is not None:
            dist_m = haversine_m(be_lat, be_lng, g_lat, g_lng)

        geo_score = geo_score_from_distance(dist_m)

        g_house = extract_house_number(g_addr) or extract_house_number(g_query_addr)
        house_score = 1.0 if (be_house and g_house and be_house == g_house) else 0.0

        name_score = max(
            string_similarity(be_name, g_name),
            string_similarity(be_name, g_query_name)
        )
        name_token = max(
            token_jaccard(be_name, g_name),
            token_jaccard(be_name, g_query_name)
        )

        street_score = max(
            string_similarity(be_street, strip_house_number(g_addr)),
            string_similarity(be_street, strip_house_number(g_query_addr))
        )

        score = (
            0.55 * geo_score
            + 0.20 * house_score
            + 0.17 * name_score
            + 0.05 * name_token
            + 0.03 * street_score
        )

        if be_house and g_house and be_house != g_house:
            score -= 0.30

        if name_score < 0.35 and name_token < 0.10:
            score -= 0.45

        meta = {
            "score": round(score, 4),
            "dist_m": None if dist_m is None else round(dist_m, 2),
            "house_be": be_house,
            "house_google": g_house,
            "name_score": round(name_score, 4),
            "name_token": round(name_token, 4),
            "street_score": round(street_score, 4),
        }
        candidates.append((score, dist_m if dist_m is not None else 10**12, rec, meta))

    if not candidates:
        return None, "none", None, {}

    candidates.sort(key=lambda x: (-x[0], x[1]))
    best_score, best_dist, best_rec, best_meta = candidates[0]

    if best_score >= 0.75:
        conf = "high"
    elif best_score >= 0.50:
        conf = "medium"
    else:
        conf = "low"

    if conf == "low":
        return None, conf, best_meta.get("dist_m"), best_meta

    return best_rec, conf, best_meta.get("dist_m"), best_meta


# =========================================================
# BUILD TABLES FOR ONE STORE
# =========================================================
def build_kg_tables_for_store(store_id, be_stores, be_ratings, foody, google_records, google_by_id):
    be_store_df = be_stores.loc[be_stores["store_id"] == store_id].copy()
    if be_store_df.empty:
        raise ValueError(f"Không tìm thấy store_id={store_id} trong be_stores.csv")

    be_store = be_store_df.iloc[0]

    be_name = safe_str(be_store["name"]) or f"store_{store_id}"
    be_address = safe_str(be_store["address"]) or ""
    be_lat = float(be_store["latitude"])
    be_lng = float(be_store["longitude"])

    ward, district, city = extract_location_parts(be_address)

    be_reviews = be_ratings.loc[be_ratings["store_id"] == store_id].copy()
    if not be_reviews.empty:
        # bỏ các dòng hoàn toàn rỗng
        be_reviews = be_reviews.loc[~(be_reviews["rating"].isna() & be_reviews["feedback"].isna())].copy()

    foody_reviews = foody.loc[foody["be_restaurant_id"] == store_id].copy()

    # ---------- Google: direct join theo id trước ----------
    google_place = google_by_id.get(int(store_id))
    google_match_method = "direct_id"
    google_conf = "high"
    google_meta = {}

    if google_place is None:
        google_place, google_conf, _, google_meta = fallback_choose_google_candidate(
            google_records=google_records,
            be_name=be_name,
            be_address=be_address,
            be_lat=be_lat,
            be_lng=be_lng,
        )
        google_match_method = "fallback" if google_place is not None else "none"

    google_open_state = None
    google_open_time = None
    google_close_time = None
    google_lat = None
    google_lng = None
    google_place_id = None
    google_data_id = None

    if google_place:
        google_open_state, google_open_time, google_close_time = parse_google_open_close(
            google_place.get("opening_hours")
        )

        loc = google_place.get("location", {}) or {}
        google_lat = loc.get("latitude")
        google_lng = loc.get("longitude")

        google_ids = google_place.get("google_maps_ids", {}) or {}
        google_place_id = google_ids.get("place_id")
        google_data_id = google_ids.get("data_id")

    # ---------- store_master ----------
    store_master = pd.DataFrame([{
        "store_id": int(store_id),
        "canonical_name": re.sub(r"\s+", " ", be_name).strip(),
        "address": re.sub(r"\s+", " ", be_address).strip(),
        "lat": be_lat,
        "lng": be_lng,
        "ward": ward,
        "district": district,
        "city": city,
        "primary_category": norm_category_be(be_store.get("merchant_category")),
        "status": safe_str(be_store.get("status")),
        "be_open_time": parse_be_time_only(be_store.get("next_slot_time")),
        "be_close_time": parse_be_time_only(be_store.get("end_time")),
        "median_price_vnd": pd.to_numeric(be_store.get("median_price"), errors="coerce"),
        "be_rating_avg": pd.to_numeric(be_store.get("avg_rating"), errors="coerce"),
        "be_rating_count": pd.to_numeric(be_store.get("total_rating"), errors="coerce"),

        "google_name": safe_str(google_place.get("name")) if google_place else None,
        "google_query_name": safe_str(google_place.get("query_name")) if google_place else None,
        "google_query_address": safe_str(google_place.get("query_address")) if google_place else None,
        "google_address": safe_str(google_place.get("address")) if google_place else None,
        "google_lat": google_lat,
        "google_lng": google_lng,
        "google_rating": google_place.get("rating") if google_place else None,
        "google_review_count": google_place.get("review_count") if google_place else None,
        "google_phone": safe_str(google_place.get("phone")) if google_place else None,
        "google_website": safe_str(google_place.get("website")) if google_place else None,
        "google_place_id": google_place_id,
        "google_data_id": google_data_id,
        "google_open_state": google_open_state,
        "google_open_time": google_open_time,
        "google_close_time": google_close_time,

        "google_match_method": google_match_method,
        "google_match_confidence": google_conf,
        "google_match_score": google_meta.get("score") if google_meta else None,
    }])

    # ---------- store_source_map ----------
    source_rows = [{
        "store_id": store_id,
        "source_name": "befood",
        "source_store_key": str(store_id),
        "source_url": None,
        "source_name_raw": be_name,
        "address_raw": be_address,
        "match_confidence": "high",
        "note": "id gốc từ beFood"
    }]

    if not foody_reviews.empty:
        f0 = foody_reviews.iloc[0]
        source_rows.append({
            "store_id": store_id,
            "source_name": "foody",
            "source_store_key": safe_str(f0.get("foody_url")),
            "source_url": safe_str(f0.get("foody_url")),
            "source_name_raw": safe_str(f0.get("be_name")),
            "address_raw": safe_str(f0.get("be_address")),
            "match_confidence": "high" if safe_str(f0.get("match_method")) == "manual_map" else "medium",
            "note": f"match_method={safe_str(f0.get('match_method'))}; crawl_status={safe_str(f0.get('crawl_status'))}"
        })

    if google_place:
        source_rows.append({
            "store_id": store_id,
            "source_name": "google",
            "source_store_key": google_place_id or f"{safe_str(google_place.get('name'))}|{safe_str(google_place.get('address'))}",
            "source_url": safe_str(google_place.get("website")),
            "source_name_raw": safe_str(google_place.get("name")),
            "address_raw": safe_str(google_place.get("address")),
            "match_confidence": google_conf,
            "note": f"method={google_match_method}; place_id={google_place_id}; score={google_meta.get('score') if google_meta else None}"
        })

    store_source_map = pd.DataFrame(source_rows)

    # ---------- store_category ----------
    category_rows = [{
        "store_id": store_id,
        "category_type": "primary",
        "category_value": norm_category_be(be_store.get("merchant_category")),
        "source_name": "befood"
    }]

    if google_place:
        for x in (google_place.get("type", []) or []):
            category_rows.append({
                "store_id": store_id,
                "category_type": "place_type",
                "category_value": slugify(x),
                "source_name": "google"
            })
        for x in (google_place.get("offerings", []) or []):
            category_rows.append({
                "store_id": store_id,
                "category_type": "offering",
                "category_value": slugify(x),
                "source_name": "google"
            })
        for x in (google_place.get("highlights", []) or []):
            category_rows.append({
                "store_id": store_id,
                "category_type": "highlight",
                "category_value": slugify(x),
                "source_name": "google"
            })

    store_category = pd.DataFrame(category_rows).drop_duplicates().reset_index(drop=True)

    # ---------- store_service_option ----------
    service_rows = []

    if google_place:
        for key, value in flatten_google_bool_map(google_place.get("service_options", {})):
            service_rows.append({
                "store_id": store_id,
                "service_option": key,
                "value": value,
                "source_name": "google"
            })

        extra_service_flags = {
            "phu_hop_de_lam_viec_tren_may_tinh_xach_tay": "laptop_friendly",
            "cho_ngoi": "seating",
            "phuc_vu_tai_ban": "table_service",
            "dich_vu_tai_quay": "counter_service",
        }

        for item in (google_place.get("popular_for", []) or []) + (google_place.get("dining_options", []) or []):
            k = slugify(item)
            if k in extra_service_flags:
                service_rows.append({
                    "store_id": store_id,
                    "service_option": extra_service_flags[k],
                    "value": 1,
                    "source_name": "google"
                })

    store_service_option = pd.DataFrame(service_rows).drop_duplicates().reset_index(drop=True)
    if store_service_option.empty:
        store_service_option = pd.DataFrame(columns=["store_id", "service_option", "value", "source_name"])

    # ---------- store_context_tag ----------
    context_rows = []

    if google_place:
        for item in (google_place.get("crowd", []) or []):
            context_rows.append({
                "store_id": store_id,
                "tag_type": "crowd",
                "tag_value": slugify(item),
                "source_name": "google"
            })

        for item in (google_place.get("popular_for", []) or []):
            context_rows.append({
                "store_id": store_id,
                "tag_type": "popular_for",
                "tag_value": slugify(item),
                "source_name": "google"
            })

        for item in (google_place.get("dining_options", []) or []):
            context_rows.append({
                "store_id": store_id,
                "tag_type": "dining_time_or_option",
                "tag_value": slugify(item),
                "source_name": "google"
            })

        for item in (google_place.get("atmosphere", []) or []):
            context_rows.append({
                "store_id": store_id,
                "tag_type": "atmosphere",
                "tag_value": slugify(item),
                "source_name": "google"
            })

    store_context_tag = pd.DataFrame(context_rows).drop_duplicates().reset_index(drop=True)
    if store_context_tag.empty:
        store_context_tag = pd.DataFrame(columns=["store_id", "tag_type", "tag_value", "source_name"])

    # ---------- review_fact ----------
    review_rows = []

    if not be_reviews.empty:
        be_reviews["rated_at_parsed"] = be_reviews["rated_at"].apply(parse_mixed_datetime)
        for i, row in be_reviews.iterrows():
            review_rows.append({
                "review_id": f"be_{store_id}_{i}",
                "store_id": store_id,
                "source_name": "befood",
                "rated_at": row["rated_at_parsed"],
                "rating_5": pd.to_numeric(row.get("rating"), errors="coerce"),
                "review_text": safe_str(row.get("feedback")),
                "is_promo": 0,
                "reviewer": None,
                "raw_rating_scale": 5
            })

    if not foody_reviews.empty:
        foody_reviews["created_at_parsed"] = foody_reviews["created_at"].apply(parse_mixed_datetime)

        for _, row in foody_reviews.iterrows():
            raw_text = " ".join([
                "" if pd.isna(row.get("title")) else str(row.get("title")),
                "" if pd.isna(row.get("content")) else str(row.get("content")),
            ]).strip()

            review_rows.append({
                "review_id": f"foody_{safe_str(row.get('review_id')) or len(review_rows)}",
                "store_id": store_id,
                "source_name": "foody",
                "rated_at": row["created_at_parsed"],
                "rating_5": None if pd.isna(row.get("rating")) else float(row.get("rating")) / 2.0,
                "review_text": raw_text if raw_text else None,
                "is_promo": detect_promo(raw_text),
                "reviewer": safe_str(row.get("author")),
                "raw_rating_scale": 10
            })

    if google_place:
        for idx, r in enumerate(google_place.get("reviews", []) or []):
            review_rows.append({
                "review_id": f"google_{store_id}_{idx}",
                "store_id": store_id,
                "source_name": "google",
                "rated_at": pd.NaT,
                "rating_5": pd.to_numeric(r.get("rating"), errors="coerce"),
                "review_text": safe_str(r.get("description")),
                "is_promo": 0,
                "reviewer": safe_str(r.get("username")),
                "raw_rating_scale": 5
            })

    review_fact = pd.DataFrame(review_rows)
    if review_fact.empty:
        review_fact = pd.DataFrame(columns=[
            "review_id", "store_id", "source_name", "rated_at",
            "rating_5", "review_text", "is_promo", "reviewer", "raw_rating_scale"
        ])

    review_fact["sentiment"] = review_fact["rating_5"].apply(rating_to_sentiment)

    # ---------- store_aspect_agg ----------
    aspect_rows = []
    if not review_fact.empty:
        review_for_aspect = review_fact.loc[
            ~((review_fact["source_name"] == "foody") & (review_fact["is_promo"] == 1))
        ].copy()

        for _, row in review_for_aspect.iterrows():
            text = row.get("review_text")
            if pd.isna(text) or str(text).strip() == "":
                continue
            aspects = extract_aspects(str(text))
            for aspect in aspects:
                aspect_rows.append({
                    "store_id": store_id,
                    "source_name": row["source_name"],
                    "aspect_name": aspect,
                    "sentiment": row["sentiment"]
                })

    if not aspect_rows:
        store_aspect_agg = pd.DataFrame(columns=[
            "store_id", "aspect_name", "mention_count",
            "positive_mentions", "negative_mentions", "neutral_mentions",
            "aspect_sentiment", "evidence_sources"
        ])
    else:
        aspect_df = pd.DataFrame(aspect_rows)

        tmp = (
            aspect_df.groupby(["store_id", "aspect_name", "sentiment"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        for col in ["positive", "negative", "neutral", "unknown"]:
            if col not in tmp.columns:
                tmp[col] = 0

        evidence = (
            aspect_df.groupby(["store_id", "aspect_name"])["source_name"]
            .apply(lambda x: "|".join(sorted(set(x))))
            .reset_index(name="evidence_sources")
        )

        store_aspect_agg = tmp.merge(evidence, on=["store_id", "aspect_name"], how="left")
        store_aspect_agg["mention_count"] = (
            store_aspect_agg["positive"] + store_aspect_agg["negative"]
            + store_aspect_agg["neutral"] + store_aspect_agg["unknown"]
        )

        def decide_aspect_sentiment(r):
            score = r["positive"] - r["negative"]
            if score >= 2:
                return "positive"
            if score <= -2:
                return "negative"
            if r["negative"] > 0 and r["positive"] > 0:
                return "mixed"
            if r["neutral"] > 0 and r["positive"] == 0 and r["negative"] == 0:
                return "neutral"
            if score > 0:
                return "mixed_positive"
            if score < 0:
                return "mixed_negative"
            return "mixed"

        store_aspect_agg["aspect_sentiment"] = store_aspect_agg.apply(decide_aspect_sentiment, axis=1)
        store_aspect_agg = store_aspect_agg.rename(columns={
            "positive": "positive_mentions",
            "negative": "negative_mentions",
            "neutral": "neutral_mentions",
        })

        store_aspect_agg = store_aspect_agg[[
            "store_id", "aspect_name", "mention_count",
            "positive_mentions", "negative_mentions", "neutral_mentions",
            "aspect_sentiment", "evidence_sources"
        ]].sort_values(["mention_count", "aspect_name"], ascending=[False, True])

    return {
        "store_master": store_master,
        "store_source_map": store_source_map,
        "store_category": store_category,
        "store_service_option": store_service_option,
        "store_context_tag": store_context_tag,
        "review_fact": review_fact,
        "store_aspect_agg": store_aspect_agg,
    }


# =========================================================
# SAVE HELPERS
# =========================================================
def save_store_tables(store_id, tables, output_dir):
    store_dir = output_dir / str(store_id)
    store_dir.mkdir(parents=True, exist_ok=True)

    for name, df in tables.items():
        df.to_csv(store_dir / f"{name}.csv", index=False, encoding="utf-8-sig")


def concat_or_empty(dfs):
    dfs = [x for x in dfs if x is not None and not x.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# =========================================================
# BUILD ALL
# =========================================================
def build_all_stores():
    be_stores, be_ratings, foody, google_records, google_by_id = load_data()

    all_ids = sorted(be_stores["store_id"].dropna().unique().tolist())
    if ONLY_STORE_IDS is not None:
        only_set = set(int(x) for x in ONLY_STORE_IDS)
        all_ids = [sid for sid in all_ids if int(sid) in only_set]

    all_store_master = []
    all_store_source_map = []
    all_store_category = []
    all_store_service_option = []
    all_store_context_tag = []
    all_review_fact = []
    all_store_aspect_agg = []
    error_rows = []

    for sid in all_ids:
        try:
            tables = build_kg_tables_for_store(
                store_id=int(sid),
                be_stores=be_stores,
                be_ratings=be_ratings,
                foody=foody,
                google_records=google_records,
                google_by_id=google_by_id,
            )

            save_store_tables(int(sid), tables, OUTPUT_DIR)

            all_store_master.append(tables["store_master"])
            all_store_source_map.append(tables["store_source_map"])
            all_store_category.append(tables["store_category"])
            all_store_service_option.append(tables["store_service_option"])
            all_store_context_tag.append(tables["store_context_tag"])
            all_review_fact.append(tables["review_fact"])
            all_store_aspect_agg.append(tables["store_aspect_agg"])

            print(f"[OK] store_id={sid}")

        except Exception as e:
            error_rows.append({"store_id": sid, "error": str(e)})
            print(f"[SKIP] store_id={sid}: {e}")

    combined = {
        "store_master": concat_or_empty(all_store_master),
        "store_source_map": concat_or_empty(all_store_source_map),
        "store_category": concat_or_empty(all_store_category),
        "store_service_option": concat_or_empty(all_store_service_option),
        "store_context_tag": concat_or_empty(all_store_context_tag),
        "review_fact": concat_or_empty(all_review_fact),
        "store_aspect_agg": concat_or_empty(all_store_aspect_agg),
        "build_errors": pd.DataFrame(error_rows),
    }

    for name, df in combined.items():
        df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False, encoding="utf-8-sig")

    print("\nSaved combined tables to:", OUTPUT_DIR.resolve())
    return combined


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    combined_tables = build_all_stores()

    print("\n=== store_master preview ===")
    print(combined_tables["store_master"].head(10).to_string(index=False))

    print("\n=== store_source_map preview ===")
    print(combined_tables["store_source_map"].head(10).to_string(index=False))

    print("\n=== store_aspect_agg preview ===")
    print(combined_tables["store_aspect_agg"].head(20).to_string(index=False))