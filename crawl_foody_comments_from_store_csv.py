#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawl Foody comments for unique stores from a CSV file and append/merge them
into an existing store_feedback CSV.

Input
-----
Default input is `store_from_top5_json.csv`.

Output
------
Default output is `store_feedback_crawled.csv`.
If the output file already exists, the script merges new rows into it while
avoiding duplicate comment rows.

What it does
------------
1) Read unique stores by `store_id` from the input CSV.
2) Map each store to a Foody place page:
   - first by a verified manual map
    - then by Foody's native autocomplete/search results
3) Visit the Foody review page (`/binh-luan`).
4) Extract comments from:
   - `var initDataReviews = {...}` if available
   - otherwise HTML review blocks
5) Save one row per comment into the output CSV.
6) If the output CSV already exists, keep old rows and append only new ones.

Usage
-----
python crawl_foody_comments_from_store_csv.py
python crawl_foody_comments_from_store_csv.py --input store_from_top5_json.csv --output store_feedback_crawled.csv

Requirements
------------
pip install requests beautifulsoup4
"""

import argparse
import csv
import html
import json
import re
import unicodedata
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from difflib import SequenceMatcher
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
}

# Verified or strongly grounded Foody URLs gathered from current Foody search results.
MANUAL_MAP = {
    "Mì Cay Seoul - Trần Đại Nghĩa": "https://www.foody.vn/ha-noi/mi-cay-seoul-tran-dai-nghia",
    "Highlands Coffee - Trần Đại Nghĩa": "https://www.foody.vn/ha-noi/highlands-coffee-tra-ca-phe-banh-dh-bach-khoa",
    "Quán Ngon - Gà Tần & Bánh Cuốn Nóng - Tạ Quang Bửu": "https://www.foody.vn/ha-noi/banh-cuon-bun-cha-ga-tan-ta-quang-buu",
    "1000M - Trà & Trà Sữa Shan Tuyết Thượng Hạng - Trần Đại Nghĩa": "https://www.foody.vn/ha-noi/1000m-tra-tra-sua-shan-tuyet-thuong-hang-71b-tran-dai-nghia",
    "Phúc Long - Tạ Quang Bửu": "https://www.foody.vn/ha-noi/phuc-long-tran-dai-nghia",
    "Bún Cá Cô Minh - Tạ Quang Bửu": "https://www.foody.vn/ha-noi/bun-ca-co-minh-3-ngo-40-2-ta-quang-buu",
    "Bánh Tráng Nướng Thảo Tồ - Tạ Quang Bửu": "https://www.foody.vn/ha-noi/banh-trang-nuong-thao-to",
}

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

FOODY_AUTOCOMPLETE_URL = "https://www.foody.vn/__get/AutoComplete/Keywords?provinceId=218"
FOODY_SEARCH_URL = "https://www.foody.vn/ha-noi/food/dia-diem"
ADDRESS_STOPWORDS = {
    "ha",
    "noi",
    "hanoi",
    "viet",
    "nam",
    "pho",
    "so",
    "ngo",
    "ngach",
    "duong",
    "phuong",
    "quan",
    "tp",
    "thanhpho",
    "khu",
    "tap",
    "the",
    "to",
    "dan",
    "cu",
    "tttm",
    "tang",
    "tang1",
    "tang2",
    "tang3",
    "p",
    "q",
}


def text_clean(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def normalize_name(name: str) -> str:
    return text_clean(name)


def normalize_for_match(value: str) -> str:
    value = text_clean(value).lower()
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def token_set(value: str) -> set[str]:
    normalized = normalize_for_match(value)
    return {token for token in normalized.split() if len(token) > 1}


def address_token_set(value: str) -> set[str]:
    normalized = normalize_for_match(value)
    return {
        token
        for token in normalized.split()
        if len(token) > 1 and token not in ADDRESS_STOPWORDS
    }


def build_foody_query_variants(name: str, address: str = "") -> List[str]:
    raw_name = text_clean(name)
    raw_address = text_clean(address)

    variants: List[str] = []

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

    deduped: List[str] = []
    seen: Set[str] = set()
    for variant in variants:
        cleaned = re.sub(r"\s+", " ", variant).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)

    return deduped


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


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
            time.sleep(2)
    raise last_err


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


def parse_autocomplete_candidates(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "Items", "data", "Data", "results", "Results"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def score_candidate(candidate: Dict[str, Any], store_name: str, address: str) -> float:
    candidate_name = safe_str(candidate.get("name") or candidate.get("title") or candidate.get("Name"))
    candidate_link = safe_str(candidate.get("link") or candidate.get("url") or candidate.get("Link"))
    candidate_address = safe_str(candidate.get("address") or candidate.get("Address"))

    target_name = normalize_for_match(store_name)
    candidate_name_norm = normalize_for_match(candidate_name)
    target_addr = normalize_for_match(address)
    candidate_addr_norm = normalize_for_match(candidate_address)

    if not candidate_name_norm:
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

    if candidate_link and looks_like_place_url(candidate_link if candidate_link.startswith("https://") else f"https://www.foody.vn{candidate_link}" if candidate_link.startswith("/") else candidate_link):
        score += 8.0

    return score


def resolve_foody_url_from_autocomplete(session: requests.Session, name: str, address: str = "") -> Optional[str]:
    seen_queries: Set[str] = set()
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


def search_foody_url_native(session: requests.Session, name: str, address: str = "") -> Optional[str]:
    resolved = resolve_foody_url_from_autocomplete(session, name, address)
    if resolved:
        return resolved

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
        best_url = None
        best_score = -1.0
        for anchor in soup.select('a[href]'):
            href = anchor.get('href', '').strip()
            if href.startswith('//'):
                href = 'https:' + href
            elif href.startswith('/'):
                href = 'https://www.foody.vn' + href

            if not looks_like_place_url(href):
                continue

            candidate = {
                'name': anchor.get_text(' ', strip=True),
                'link': href,
                'address': '',
                'type': 'Restaurant',
            }
            candidate_score = score_candidate(candidate, name, address)
            if candidate_score > best_score:
                best_score = candidate_score
                best_url = href

        if best_url and best_score >= 60.0:
            return best_url

    return None


def extract_json_object_after_var(html_text: str, var_name: str) -> Optional[dict]:
    marker = f"var {var_name} ="
    start = html_text.find(marker)
    if start == -1:
        return None

    brace_start = html_text.find("{", start)
    if brace_start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    end = None

    for index in range(brace_start, len(html_text)):
        char = html_text[index]

        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = index + 1
                break

    if end is None:
        return None

    raw = html_text[brace_start:end]
    try:
        return json.loads(raw)
    except Exception:
        return None


def parse_comments_from_initdata(html_text: str, max_comments: int) -> List[Dict[str, Any]]:
    data = extract_json_object_after_var(html_text, "initDataReviews")
    if not data:
        return []

    comments: List[Dict[str, Any]] = []
    for item in (data.get("Items") or [])[:max_comments]:
        comments.append(
            {
                "review_id": item.get("Id"),
                "author": text_clean(((item.get("Owner") or {}).get("DisplayName"))),
                "title": text_clean(item.get("Title")),
                "rating": item.get("AvgRating"),
                "created_at": text_clean(item.get("CreatedOnTimeDiff")),
                "content": text_clean(item.get("Description")),
                "device": text_clean(item.get("DeviceName")),
                "url": text_clean(item.get("Url")),
                "likes": item.get("TotalLike"),
                "comment_count": item.get("TotalComment"),
            }
        )
    return comments


def parse_comments_from_html(soup: BeautifulSoup, max_comments: int) -> List[Dict[str, Any]]:
    comments: List[Dict[str, Any]] = []
    for li in soup.select("li.review-item")[:max_comments]:
        author = text_clean(li.select_one(".ru-username").get_text(" ", strip=True) if li.select_one(".ru-username") else "")
        title = text_clean(li.select_one(".rd-title").get_text(" ", strip=True) if li.select_one(".rd-title") else "")
        content = text_clean(li.select_one(".rd-des").get_text(" ", strip=True) if li.select_one(".rd-des") else "")
        rating = text_clean(li.select_one("[itemprop='ratingValue']").get_text(" ", strip=True) if li.select_one("[itemprop='ratingValue']") else "")
        created = text_clean(li.select_one(".ru-time").get_text(" ", strip=True) if li.select_one(".ru-time") else "")

        if author or title or content:
            comments.append(
                {
                    "review_id": None,
                    "author": author,
                    "title": title,
                    "rating": rating,
                    "created_at": created,
                    "content": content,
                    "device": "",
                    "url": "",
                    "likes": None,
                    "comment_count": None,
                }
            )
    return comments


def crawl_foody_comments(session: requests.Session, foody_url: str, max_comments: int) -> Dict[str, Any]:
    review_url = foody_url.rstrip("/") + "/binh-luan"
    response = get(session, review_url, timeout=35, retries=3)
    html_text = response.text
    soup = BeautifulSoup(html_text, "html.parser")

    comments = parse_comments_from_initdata(html_text, max_comments=max_comments)
    if not comments:
        comments = parse_comments_from_html(soup, max_comments=max_comments)

    summary: Dict[str, Any] = {}
    score_box = soup.select_one(".ratings-boxes-points b")
    if score_box:
        summary["overall_score"] = text_clean(score_box.get_text(" ", strip=True))

    review_count = soup.select_one(".summary b")
    if review_count:
        summary["review_count_text"] = text_clean(review_count.get_text(" ", strip=True))

    return {
        "review_url": review_url,
        "summary": summary,
        "comments": comments,
        "fetched_comment_count": len(comments),
    }


def read_unique_stores(input_csv: Path) -> List[Dict[str, str]]:
    unique_stores: List[Dict[str, str]] = []
    seen: Set[str] = set()

    with input_csv.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            store_id = safe_str(row.get("store_id")).strip()
            if not store_id or store_id in seen:
                continue
            seen.add(store_id)
            unique_stores.append(
                {
                    "store_id": store_id,
                    "store_name": safe_str(row.get("store_name")).strip(),
                    "address": safe_str(row.get("address")).strip(),
                }
            )

    return unique_stores


def load_existing_rows(output_csv: Path) -> List[Dict[str, str]]:
    if not output_csv.exists():
        return []
    with output_csv.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def row_signature(row: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    return (
        safe_str(row.get("store_id")),
        safe_str(row.get("rated_at")),
        safe_str(row.get("rating")),
        safe_str(row.get("feedback")),
        safe_str(row.get("crawl_status")),
    )


def merge_rows(existing_rows: List[Dict[str, str]], new_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = {row_signature(row) for row in existing_rows}
    merged = list(existing_rows)

    for row in new_rows:
        signature = row_signature(row)
        if signature in seen:
            continue
        seen.add(signature)
        merged.append(row)

    return merged


def crawl_and_append(
    input_csv: Path,
    output_csv: Path,
    max_comments: int,
    sleep_sec: float,
) -> List[Dict[str, str]]:
    stores = read_unique_stores(input_csv)
    print(f"Total unique stores to crawl: {len(stores)}")

    session = requests.Session()
    session.headers.update(HEADERS)

    existing_rows = load_existing_rows(output_csv)
    new_rows: List[Dict[str, str]] = []

    for index, store in enumerate(stores, start=1):
        store_id = store["store_id"]
        store_name = store["store_name"]
        address = store["address"]

        print(f"\n[{index}/{len(stores)}] {store_name} ({store_id})")

        foody_url = MANUAL_MAP.get(normalize_name(store_name))
        match_method = "manual_map" if foody_url else "foody_native_search"

        if not foody_url:
            foody_url = search_foody_url_native(session, store_name, address)

        if not foody_url:
            new_rows.append(
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "rated_at": "",
                    "rating": "",
                    "feedback": "",
                    "crawl_status": "foody_url_not_found",
                    "error": f"match_method={match_method}",
                }
            )
            print("  -> Foody URL not found")
            time.sleep(sleep_sec)
            continue

        print(f"  -> Foody URL: {foody_url}")

        try:
            crawled = crawl_foody_comments(session, foody_url, max_comments=max_comments)
            comments = crawled["comments"]
            if comments:
                for comment in comments:
                    new_rows.append(
                        {
                            "store_id": store_id,
                            "store_name": store_name,
                            "rated_at": safe_str(comment.get("created_at")),
                            "rating": safe_str(comment.get("rating")),
                            "feedback": safe_str(comment.get("content")),
                            "crawl_status": "ok",
                            "error": "",
                        }
                    )
            else:
                new_rows.append(
                    {
                        "store_id": store_id,
                        "store_name": store_name,
                        "rated_at": "",
                        "rating": "",
                        "feedback": "",
                        "crawl_status": "empty_or_failed",
                        "error": "no_comments_found",
                    }
                )
            print(f"  -> comments: {len(comments)}")
        except Exception as exc:  # noqa: BLE001
            new_rows.append(
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "rated_at": "",
                    "rating": "",
                    "feedback": "",
                    "crawl_status": "failed",
                    "error": safe_str(exc),
                }
            )
            print(f"  -> failed: {exc}")

        time.sleep(sleep_sec)

    merged_rows = merge_rows(existing_rows, new_rows)

    fieldnames = ["store_id", "store_name", "rated_at", "rating", "feedback", "crawl_status", "error"]
    with output_csv.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(merged_rows)

    print(f"\nCreated/updated: {output_csv.resolve()}")
    print(f"Existing rows: {len(existing_rows)}")
    print(f"New rows: {len(new_rows)}")
    print(f"Total rows: {len(merged_rows)}")
    return merged_rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="store_from_top5_json.csv")
    parser.add_argument("--output", type=str, default="store_feedback_crawled.csv")
    parser.add_argument("--max-comments", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=1.0)
    args = parser.parse_args()

    crawl_and_append(
        input_csv=Path(args.input),
        output_csv=Path(args.output),
        max_comments=args.max_comments,
        sleep_sec=args.sleep,
    )


if __name__ == "__main__":
    main()