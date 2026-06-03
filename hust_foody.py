from __future__ import annotations

import json
import math
import re
import time
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE = "https://www.foody.vn"
OUTPUT_DIR = Path("foody_hust_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# Tâm HUST gần Trần Đại Nghĩa - Tạ Quang Bửu
HUST_LAT = 21.0056
HUST_LNG = 105.8436
MAX_DISTANCE_M = 2200  # giữ quán trong bán kính ~2.2km quanh HUST

# Listing seeds quanh HUST + các đường / khu lân cận
SEED_URLS = [
    "https://www.foody.vn/ha-noi/quan-an-tai-bach-khoa%2Cquan-hai-ba-trung",
    "https://www.foody.vn/ha-noi/quan-an-tai-bach-khoa%2Cgiai-phong%2Cquan-hai-ba-trung",
    "https://www.foody.vn/ha-noi/khu-vuc-bach-khoa",
    "https://www.foody.vn/ha-noi/khu-vuc-quan-hai-ba-trung/tren-duong-ta-quang-buu",
    "https://www.foody.vn/ha-noi/khu-vuc-quan-hai-ba-trung/tren-duong-tran-dai-nghia",
    "https://www.foody.vn/ha-noi/khu-vuc-quan-hai-ba-trung/tren-duong-le-thanh-nghi",
    "https://www.foody.vn/ha-noi/khu-vuc-quan-hai-ba-trung/tren-duong-dai-la",
    "https://www.foody.vn/ha-noi/khu-vuc-quan-hai-ba-trung/tren-duong-bach-mai",
    "https://www.foody.vn/ha-noi/khu-vuc-quan-hai-ba-trung/tren-duong-giai-phong",
]

# dùng thêm từ khóa địa phương như một lớp lọc mềm
HUST_KEYWORDS = [
    "bách khoa", "bach khoa",
    "tạ quang bửu", "ta quang buu",
    "trần đại nghĩa", "tran dai nghia",
    "lê thanh nghị", "le thanh nghi",
    "đại la", "dai la",
    "bạch mai", "bach mai",
    "giải phóng", "giai phong",
    "hai bà trưng", "hai ba trung",
    "kinh tế quốc dân", "xây dựng",
]


def norm_space(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def norm_text(text: str | None) -> str:
    if not text:
        return ""
    return norm_space(text).lower()


def safe_get(d: dict | None, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def update_query_param(url: str, key: str, value: Any) -> str:
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = str(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    p1 = math.radians(float(lat1))
    p2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def is_candidate_place_url(href: str) -> bool:
    if not href:
        return False

    full = urljoin(BASE, href)
    p = urlparse(full)
    if p.netloc not in {"www.foody.vn", "foody.vn"}:
        return False

    path = p.path.strip("/")
    parts = path.split("/")
    if len(parts) != 2:
        return False
    if parts[0] != "ha-noi":
        return False

    bad_keywords = [
        "album-anh", "binh-luan", "video", "thuc-don", "bai-dau-xe",
        "nearby", "nearBy", "khuyen-mai", "bo-suu-tap", "hinh-anh",
        "o-dau", "dia-diem", "khu-vuc-", "thuong-hieu", "food/",
        "coupon", "su-kien", "top-thanh-vien",
    ]
    if any(x in full for x in bad_keywords):
        return False

    return True


def is_obvious_listing_page(url: str, restaurant_id: Any, name: str, title_page: str) -> bool:
    text = " | ".join([str(url), str(name), str(title_page)]).lower()
    if restaurant_id in (None, "", 0):
        return True
    listing_markers = [
        "địa điểm quán ăn tại", "dia diem quan an tai",
        "khu vực", "khu vuc", "trên đường", "tren duong",
    ]
    return any(m in text for m in listing_markers)


def html_mentions_hust(text: str) -> bool:
    t = norm_text(text)
    return any(k in t for k in HUST_KEYWORDS)


def extract_js_object(html: str, var_name: str) -> dict[str, Any] | None:
    marker = f"var {var_name} ="
    start = html.find(marker)
    if start < 0:
        return None

    brace_start = html.find("{", start)
    if brace_start < 0:
        return None

    depth = 0
    in_str = False
    quote_char = ""
    escaped = False
    end = None

    for i in range(brace_start, len(html)):
        ch = html[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote_char:
                in_str = False
        else:
            if ch in ('"', "'"):
                in_str = True
                quote_char = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break

    if end is None:
        return None

    raw = unescape(html[brace_start:end + 1])
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        cleaned = re.sub(r"\bundefined\b", "null", raw)
        cleaned = re.sub(r",(\s*[}\]])", r"\1", cleaned)
        return json.loads(cleaned)


def extract_place_links_from_listing(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href")
        full = urljoin(BASE, href)
        if is_candidate_place_url(full) and full not in seen:
            seen.add(full)
            links.append(full)

    # hỗ trợ thêm nếu listing nhúng data-href hoặc url trong script
    for m in re.finditer(r'https://www\.foody\.vn/ha-noi/[^"\'\s<>]+', html):
        full = m.group(0)
        if is_candidate_place_url(full) and full not in seen:
            seen.add(full)
            links.append(full)

    return links


def collect_place_links_from_seed(seed_url: str, max_pages: int = 8) -> list[str]:
    all_links = []
    seen = set()
    empty_streak = 0

    for page_no in range(1, max_pages + 1):
        page_url = seed_url if page_no == 1 else update_query_param(seed_url, "page", page_no)
        try:
            html = fetch_html(page_url)
            links = extract_place_links_from_listing(html)
            new_links = [x for x in links if x not in seen]
            for x in new_links:
                seen.add(x)
                all_links.append(x)

            print(f"[listing] page={page_no} {page_url} -> +{len(new_links)}")

            if not new_links:
                empty_streak += 1
            else:
                empty_streak = 0

            if empty_streak >= 2:
                break

            time.sleep(0.8)
        except Exception as e:
            print(f"[listing fail] {page_url} -> {e}")
            break

    return all_links


def parse_place_detail(html: str, url: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    init_data = extract_js_object(html, "initData")

    title_tag = soup.find("title")
    page_title = norm_space(title_tag.get_text()) if title_tag else ""

    meta_desc_tag = soup.find("meta", attrs={"name": "description"})
    meta_desc = meta_desc_tag.get("content", "") if meta_desc_tag else ""

    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    canonical = canonical_tag.get("href", "") if canonical_tag else url

    h1 = soup.find("h1")
    h1_text = norm_space(h1.get_text()) if h1 else ""

    lat_meta = soup.find("meta", attrs={"property": "place:location:latitude"})
    lng_meta = soup.find("meta", attrs={"property": "place:location:longitude"})

    place = {
        "restaurant_id": safe_get(init_data, "RestaurantID"),
        "name": safe_get(init_data, "Name") or h1_text,
        "url": canonical or url,
        "title_page": page_title,
        "meta_description": meta_desc,
        "address": safe_get(init_data, "Address"),
        "city": safe_get(init_data, "City"),
        "district": safe_get(init_data, "District"),
        "area": safe_get(init_data, "Area"),
        "lat": safe_get(init_data, "Latitude"),
        "lng": safe_get(init_data, "Longtitude"),
        "phone": safe_get(init_data, "Phone"),
        "website": safe_get(init_data, "Website"),
        "price_min": safe_get(init_data, "PriceMin"),
        "price_max": safe_get(init_data, "PriceMax"),
        "avg_rating": safe_get(init_data, "AvgRating"),
        "total_review": safe_get(init_data, "TotalReview"),
        "total_view": safe_get(init_data, "TotalView"),
        "brand_name": safe_get(init_data, "BrandName"),
        "access_guide": safe_get(init_data, "AccessGuide"),
        "meta_keywords": safe_get(init_data, "MetaKeywords"),
    }

    if place["lat"] is None and lat_meta:
        place["lat"] = lat_meta.get("content")
    if place["lng"] is None and lng_meta:
        place["lng"] = lng_meta.get("content")

    lst_category = safe_get(init_data, "LstCategory", default=[]) or []
    cuisines = safe_get(init_data, "Cuisines", default=[]) or []
    audiences = safe_get(init_data, "LstTargetAudience", default=[]) or []
    wifi = safe_get(init_data, "Wifi", default=[]) or []
    opening_time = safe_get(init_data, "OpeningTime", default=[]) or []
    avg_point_list = safe_get(init_data, "AvgPointList", default=[]) or []

    place["categories"] = "|".join([x.get("Name", "") for x in lst_category if isinstance(x, dict)])
    place["cuisines"] = "|".join([x.get("Name", "") for x in cuisines if isinstance(x, dict)])
    place["audiences"] = "|".join([x.get("Name", "") for x in audiences if isinstance(x, dict)])
    place["wifi"] = "|".join([f'{x.get("Name","")}::{x.get("Password","")}' for x in wifi if isinstance(x, dict)])

    opening_rows = []
    for x in opening_time:
        if isinstance(x, dict):
            dow = x.get("DayOfWeek")
            th = safe_get(x, "TimeOpen", "Hours", default="")
            tm = safe_get(x, "TimeOpen", "Minutes", default="")
            ch = safe_get(x, "TimeClose", "Hours", default="")
            cm = safe_get(x, "TimeClose", "Minutes", default="")
            opening_rows.append(f"DOW={dow} {th}:{tm} - {ch}:{cm}")
    place["opening_hours"] = " | ".join(opening_rows)

    score_map = {}
    for x in avg_point_list:
        if isinstance(x, dict):
            score_map[norm_space(x.get("Label"))] = x.get("Point")
    place["rating_quality"] = score_map.get("Chất lượng")
    place["rating_position"] = score_map.get("Vị trí")
    place["rating_service"] = score_map.get("Phục vụ")
    place["rating_price"] = score_map.get("Giá cả")
    place["rating_space"] = score_map.get("Không gian")

    # Loại trang listing / trang không phải quán thật
    if is_obvious_listing_page(url, place["restaurant_id"], place["name"], page_title):
        return None

    # Lọc quanh HUST bằng địa lý, nếu chưa có tọa độ thì fallback sang keyword
    try:
        lat = float(place["lat"])
        lng = float(place["lng"])
        dist = haversine_m(HUST_LAT, HUST_LNG, lat, lng)
        place["distance_to_hust_m"] = round(dist, 2)
        if dist > MAX_DISTANCE_M:
            return None
    except Exception:
        joined_text = " | ".join([
            str(place.get("name", "")),
            str(place.get("address", "")),
            str(place.get("district", "")),
            str(place.get("area", "")),
            str(place.get("meta_description", "")),
            str(place.get("meta_keywords", "")),
            str(place.get("access_guide", "")),
            page_title,
        ])
        if not html_mentions_hust(joined_text):
            return None
        place["distance_to_hust_m"] = None

    return place


def crawl_foody_hust_places_only() -> pd.DataFrame:
    place_links = []
    seen_links = set()

    for seed in SEED_URLS:
        links = collect_place_links_from_seed(seed, max_pages=8)
        for url in links:
            if url not in seen_links:
                seen_links.add(url)
                place_links.append(url)

    print(f"\nCollected {len(place_links)} candidate detail links.\n")

    places = []
    seen_restaurants = set()

    for i, url in enumerate(place_links, start=1):
        try:
            html = fetch_html(url)
            place = parse_place_detail(html, url)
            if place is None:
                print(f"[{i}/{len(place_links)}] SKIP")
            else:
                dedup_key = place.get("restaurant_id") or place.get("url")
                if dedup_key not in seen_restaurants:
                    seen_restaurants.add(dedup_key)
                    places.append(place)
                    print(f"[{i}/{len(place_links)}] OK - {place['name']}")
                else:
                    print(f"[{i}/{len(place_links)}] DUP - {place['name']}")
            time.sleep(0.7)
        except Exception as e:
            print(f"[{i}/{len(place_links)}] FAIL - {url} - {e}")

    return pd.DataFrame(places).sort_values(["distance_to_hust_m", "name"], na_position="last").reset_index(drop=True)


def main() -> None:
    places_df = crawl_foody_hust_places_only()

    out_csv = OUTPUT_DIR / "foody_hust_places_only_v2.csv"
    places_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print("\n=== DONE ===")
    print(f"Places: {len(places_df)} -> {out_csv}")
    if not places_df.empty:
        cols = [
            "restaurant_id", "name", "address", "district", "area",
            "price_min", "price_max", "avg_rating", "total_review", "distance_to_hust_m",
        ]
        cols = [c for c in cols if c in places_df.columns]
        print(places_df[cols].head(15).to_string(index=False))


if __name__ == "__main__":
    main()
