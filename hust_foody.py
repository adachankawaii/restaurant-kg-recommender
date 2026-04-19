from __future__ import annotations

import argparse
import json
import math
import re
import time
import unicodedata
from difflib import SequenceMatcher
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin, urlparse, urlsplit, urlunsplit, parse_qsl, urlencode

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

FOODY_AUTOCOMPLETE_URL = "https://www.foody.vn/__get/AutoComplete/Keywords?provinceId=218"
FOODY_SEARCH_URL = "https://www.foody.vn/ha-noi/food/dia-diem"
BAD_FOODY_PATH_TOKENS = [
    "/binh-luan-",
    "/thuc-don",
    "/album",
    "/check-in",
    "/ban-do",
    "/nearby",
    "/sameby",
    "/thuong-hieu/",
    "/hashtag/",
    "/bai-viet/",
]
ADDRESS_STOPWORDS = {
    "ha", "noi", "hanoi", "viet", "nam", "pho", "so", "ngo", "ngach", "duong",
    "phuong", "quan", "tp", "thanhpho", "khu", "tap", "the", "to", "dan", "cu",
    "tttm", "tang", "tang1", "tang2", "tang3", "p", "q",
}
NAME_MATCH_STOPWORDS = {
    "quan", "an", "nha", "hang", "do", "uong", "shop", "store", "tai", "ha", "noi",
    "p", "q", "ngo", "ngach", "duong", "pho", "so", "chi", "nhanh", "cn",
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


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def text_clean(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", unescape(str(value))).strip()


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def get(session: requests.Session, url: str, timeout: int = 30, retries: int = 3) -> requests.Response:
    last_err = None
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            print(f"[RETRY] {attempt + 1}/{retries} failed :: {url} :: {exc}")
            time.sleep(1.5)
    raise last_err


def fetch_html_session(session: requests.Session, url: str) -> str:
    response = get(session, url, timeout=35, retries=3)
    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


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


def looks_like_place_url(url: str) -> bool:
    if not url.startswith("https://www.foody.vn/"):
        return False
    if any(tok in url for tok in BAD_FOODY_PATH_TOKENS):
        return False
    parsed = urlparse(url)
    if parsed.query or parsed.fragment:
        return False
    parts = parsed.path.strip("/").split("/")
    return len(parts) == 2


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


def normalize_for_match(value: str) -> str:
    value = text_clean(value).lower()
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def token_set(value: str) -> set[str]:
    normalized = normalize_for_match(value)
    return {token for token in normalized.split() if len(token) > 1}


def name_match_token_set(value: str) -> set[str]:
    return {token for token in token_set(value) if token not in NAME_MATCH_STOPWORDS}


def url_slug_name(url: str) -> str:
    try:
        parts = urlparse(url).path.strip("/").split("/")
        if len(parts) != 2:
            return ""
        slug = parts[1]
        slug = re.sub(r"-p\d+$", "", slug)
        return slug.replace("-", " ")
    except Exception:  # noqa: BLE001
        return ""


def is_url_name_consistent(store_name: str, url: str, min_overlap: float = 0.35) -> bool:
    slug_name = url_slug_name(url)
    if not slug_name:
        return False

    store_tokens = name_match_token_set(store_name)
    slug_tokens = name_match_token_set(slug_name)
    if not store_tokens or not slug_tokens:
        return False

    overlap = len(store_tokens & slug_tokens)
    ratio = overlap / len(store_tokens)
    return overlap >= 1 and ratio >= min_overlap


def address_token_set(value: str) -> set[str]:
    normalized = normalize_for_match(value)
    return {
        token
        for token in normalized.split()
        if len(token) > 1 and token not in ADDRESS_STOPWORDS
    }


def build_foody_query_variants(name: str, address: str = "") -> list[str]:
    raw_name = text_clean(name)
    raw_address = text_clean(address)

    variants: list[str] = []
    parts = [part.strip() for part in re.split(r"\s*[-–—]\s*", raw_name) if part.strip()]
    if parts:
        variants.append(parts[0])
    if len(parts) >= 2:
        variants.append(f"{parts[0]} {parts[-1]}".strip())
    if raw_address and parts:
        address_head = raw_address.split(",", 1)[0].strip()
        if address_head:
            variants.append(f"{parts[0]} {address_head}".strip())
    if raw_name:
        variants.append(raw_name)
    if raw_address:
        address_head = raw_address.split(",", 1)[0].strip()
        if address_head:
            variants.append(address_head)

    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        cleaned = re.sub(r"\s+", " ", variant).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
    return deduped


def parse_autocomplete_candidates(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "Items", "data", "Data", "results", "Results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def score_candidate(candidate: dict[str, Any], store_name: str, address: str) -> float:
    candidate_name = safe_str(candidate.get("name") or candidate.get("title") or candidate.get("Name"))
    candidate_link = safe_str(candidate.get("link") or candidate.get("url") or candidate.get("Link"))
    candidate_address = safe_str(candidate.get("address") or candidate.get("Address"))

    target_name = normalize_for_match(store_name)
    candidate_name_norm = normalize_for_match(candidate_name)
    target_addr = normalize_for_match(address)
    candidate_addr_norm = normalize_for_match(candidate_address)

    if not candidate_name_norm:
        return -1.0

    if target_addr and not candidate_addr_norm:
        return -1.0

    if address:
        address_head = normalize_for_match(text_clean(address).split(",", 1)[0])
        head_tokens = {
            token for token in address_head.split()
            if len(token) > 1 and token not in ADDRESS_STOPWORDS and not token.isdigit()
        }
        if head_tokens:
            candidate_tokens = token_set(candidate_address)
            if not (head_tokens & candidate_tokens):
                return -1.0

    if target_addr:
        target_addr_tokens = address_token_set(address)
        candidate_addr_tokens = address_token_set(candidate_address)
        address_overlap = target_addr_tokens & candidate_addr_tokens
        address_in_name = target_addr in candidate_name_norm
        address_in_address = target_addr in candidate_addr_norm
        if len(target_addr_tokens) >= 2 and len(address_overlap) < 2 and not address_in_name and not address_in_address:
            return -1.0
        if not address_overlap and not address_in_name and not address_in_address:
            return -1.0

    score = SequenceMatcher(None, target_name, candidate_name_norm).ratio() * 100.0

    target_tokens = token_set(store_name)
    candidate_tokens = token_set(candidate_name)
    if target_tokens:
        overlap = len(target_tokens & candidate_tokens) / len(target_tokens)
        score += overlap * 55.0

    if target_addr and candidate_addr_norm:
        addr_overlap = len(address_token_set(address) & address_token_set(candidate_address))
        if addr_overlap:
            score += min(addr_overlap * 18.0, 54.0)

    if target_name and target_name == candidate_name_norm:
        score += 120.0

    if target_name and target_name in candidate_name_norm:
        score += 30.0

    if candidate_link and looks_like_place_url(
        candidate_link
        if candidate_link.startswith("https://")
        else f"https://www.foody.vn{candidate_link}"
        if candidate_link.startswith("/")
        else candidate_link
    ):
        score += 8.0

    return score


def resolve_foody_url_from_autocomplete(session: requests.Session, name: str, address: str = "") -> str | None:
    seen_queries: set[str] = set()
    for query in build_foody_query_variants(name, address):
        query = text_clean(query)
        if not query or query in seen_queries:
            continue
        seen_queries.add(query)

        for param_name in ("term", "keyword", "q", "query"):
            try:
                response = get(
                    session,
                    f"{FOODY_AUTOCOMPLETE_URL}&{param_name}={quote_plus(query)}",
                    timeout=30,
                    retries=2,
                )
                payload = response.json()
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] foody autocomplete failed :: {param_name}={query} :: {exc}")
                continue

            candidates = parse_autocomplete_candidates(payload)
            best_url = None
            best_score = -1.0

            for candidate in candidates:
                candidate_url = safe_str(candidate.get("link") or candidate.get("url") or candidate.get("Link"))
                if candidate_url.startswith("//"):
                    candidate_url = "https:" + candidate_url
                elif candidate_url.startswith("/"):
                    candidate_url = "https://www.foody.vn" + candidate_url

                if not looks_like_place_url(candidate_url):
                    continue

                if not is_url_name_consistent(name, candidate_url):
                    continue

                candidate_type = safe_str(candidate.get("type") or candidate.get("Type")).lower()
                if candidate_type and candidate_type != "restaurant":
                    continue

                candidate_score = score_candidate(candidate, name, address)
                if candidate_score > best_score:
                    best_score = candidate_score
                    best_url = candidate_url

            if best_url and best_score >= 85.0:
                return best_url

    return None


def search_foody_url_native(session: requests.Session, name: str, address: str = "") -> str | None:
    resolved = resolve_foody_url_from_autocomplete(session, name, address)
    if resolved:
        return resolved

    def score_url_by_detail(url: str) -> float:
        try:
            html = fetch_html_session(session, url)
            place = parse_place_detail(html, url, apply_hust_filter=False)
            if not place:
                return -1.0

            if not is_url_name_consistent(name, url):
                return -1.0

            candidate = {
                "name": safe_str(place.get("name")),
                "address": safe_str(place.get("address")),
                "link": url,
            }
            return score_candidate(candidate, name, address)
        except Exception:  # noqa: BLE001
            return -1.0

    for query in build_foody_query_variants(name, address):
        if not query:
            continue

        search_url = f"{FOODY_SEARCH_URL}?q={quote_plus(query)}&ds=Restaurant"
        try:
            response = get(session, search_url, timeout=30, retries=2)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] foody search page failed :: {query} :: {exc}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        candidate_urls: list[str] = []
        seen_urls: set[str] = set()
        for anchor in soup.select("a[href]"):
            href = anchor.get("href", "").strip()
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = "https://www.foody.vn" + href

            if not looks_like_place_url(href):
                continue

            if href in seen_urls:
                continue
            seen_urls.add(href)
            candidate_urls.append(href)
            if len(candidate_urls) >= 8:
                break

        best_url = None
        best_score = -1.0
        for candidate_url in candidate_urls:
            candidate_score = score_url_by_detail(candidate_url)
            if candidate_score > best_score:
                best_score = candidate_score
                best_url = candidate_url

        if best_url and best_score >= 85.0:
            return best_url

    return None


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


def parse_place_detail(html: str, url: str, apply_hust_filter: bool = True) -> dict[str, Any] | None:
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
    if not apply_hust_filter:
        return place

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


def read_unique_stores_from_csv(input_csv: Path) -> list[dict[str, str]]:
    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    required_cols = {"store_id", "store_name", "address"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {sorted(missing)}")

    stores = []
    seen_ids = set()
    for _, row in df.iterrows():
        store_id = safe_str(row.get("store_id")).strip()
        if not store_id or store_id in seen_ids:
            continue
        seen_ids.add(store_id)
        stores.append(
            {
                "store_id": store_id,
                "store_name": text_clean(safe_str(row.get("store_name"))),
                "address": text_clean(safe_str(row.get("address"))),
            }
        )
    return stores


def crawl_places_from_store_csv(input_csv: Path, sleep_sec: float = 0.7, limit: int | None = None) -> pd.DataFrame:
    stores = read_unique_stores_from_csv(input_csv)
    if limit is not None and limit > 0:
        stores = stores[:limit]
    print(f"Loaded {len(stores)} unique stores from {input_csv}")

    session = requests.Session()
    session.headers.update(HEADERS)

    rows: list[dict[str, Any]] = []

    for i, store in enumerate(stores, start=1):
        store_id = store["store_id"]
        store_name = store["store_name"]
        address = store["address"]
        print(f"[{i}/{len(stores)}] Resolving Foody URL for: {store_name} ({store_id})")

        foody_url = search_foody_url_native(session, store_name, address)
        if not foody_url:
            rows.append(
                {
                    "input_store_id": store_id,
                    "input_store_name": store_name,
                    "input_address": address,
                    "matched_foody_url": "",
                    "crawl_status": "foody_url_not_found",
                    "error": "",
                }
            )
            print("  -> URL not found")
            time.sleep(sleep_sec)
            continue

        print(f"  -> URL: {foody_url}")
        try:
            if not is_url_name_consistent(store_name, foody_url):
                rows.append(
                    {
                        "input_store_id": store_id,
                        "input_store_name": store_name,
                        "input_address": address,
                        "matched_foody_url": foody_url,
                        "crawl_status": "foody_url_name_mismatch",
                        "error": "url_slug_not_consistent_with_store_name",
                    }
                )
                print("  -> SKIP: URL slug mismatch store name")
                time.sleep(sleep_sec)
                continue

            html = fetch_html_session(session, foody_url)
            place = parse_place_detail(html, foody_url, apply_hust_filter=False)
            if place is None:
                rows.append(
                    {
                        "input_store_id": store_id,
                        "input_store_name": store_name,
                        "input_address": address,
                        "matched_foody_url": foody_url,
                        "crawl_status": "detail_parse_failed",
                        "error": "page_not_place_or_parse_failed",
                    }
                )
            else:
                row = {
                    "input_store_id": store_id,
                    "input_store_name": store_name,
                    "input_address": address,
                    "matched_foody_url": foody_url,
                    "crawl_status": "ok",
                    "error": "",
                }
                row.update(place)
                rows.append(row)
                print(f"  -> OK: {place.get('name', '')}")
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "input_store_id": store_id,
                    "input_store_name": store_name,
                    "input_address": address,
                    "matched_foody_url": foody_url,
                    "crawl_status": "failed",
                    "error": safe_str(exc),
                }
            )
            print(f"  -> FAIL: {exc}")

        time.sleep(sleep_sec)

    df_out = pd.DataFrame(rows)
    sort_cols = [c for c in ["crawl_status", "input_store_id"] if c in df_out.columns]
    if sort_cols:
        df_out = df_out.sort_values(sort_cols).reset_index(drop=True)
    return df_out


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["seed", "csv"], default="csv")
    parser.add_argument("--input", type=str, default="store_from_top5_json.csv")
    parser.add_argument("--output", type=str, default="foody_hust_places_from_store_csv.csv")
    parser.add_argument("--sleep", type=float, default=0.7)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if args.mode == "seed":
        places_df = crawl_foody_hust_places_only()
        out_csv = OUTPUT_DIR / "foody_hust_places_only_v2.csv"
    else:
        places_df = crawl_places_from_store_csv(Path(args.input), sleep_sec=args.sleep, limit=args.limit)
        out_csv = OUTPUT_DIR / args.output

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
