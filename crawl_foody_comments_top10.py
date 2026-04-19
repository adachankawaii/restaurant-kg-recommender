#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawl Foody comments for the top 10 restaurants in restaurants_detail_raw.json.

What it does
------------
1) Read the first 10 restaurants from the input JSON.
2) Try to map each restaurant to a Foody place page:
   - first by a small verified manual map
   - then by DuckDuckGo site-search fallback
3) Visit the Foody review page (/binh-luan)
4) Extract comments from:
   - `var initDataReviews = {...}` if available
   - otherwise HTML review blocks
5) Export:
   - foody_top10_comments.json
   - foody_top10_comments_flat.csv  (one row per comment)

Usage
-----
python crawl_foody_comments_top10.py
python crawl_foody_comments_top10.py --input restaurants_detail_raw.json --topk 10 --max-comments 20

Requirements
------------
pip install requests beautifulsoup4
"""

import argparse
import csv
import html
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
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


def text_clean(x: Optional[str]) -> str:
    if not x:
        return ""
    return re.sub(r"\s+", " ", html.unescape(str(x))).strip()


def normalize_name(name: str) -> str:
    return text_clean(name)


def get(session: requests.Session, url: str, timeout: int = 30, retries: int = 3) -> requests.Response:
    last_err = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            print(f"[RETRY] {attempt+1}/{retries} failed :: {url} :: {e}")
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
    if len(parts) != 2:
        return False
    return True


def search_foody_url_ddg(session: requests.Session, name: str, address: str = "") -> Optional[str]:
    queries = [
        f'site:foody.vn "{name}"',
        f'site:foody.vn "{name}" "{address}"' if address else "",
        f'site:foody.vn "{address}"' if address else "",
    ]

    for q in queries:
        if not q:
            continue
        search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(q)}"
        try:
            r = get(session, search_url, timeout=30, retries=2)
        except Exception as e:
            print(f"[WARN] search failed :: {q} :: {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            if href.startswith("//"):
                href = "https:" + href
            if looks_like_place_url(href):
                return href

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

    for i in range(brace_start, len(html_text)):
        ch = html_text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end is None:
        return None

    raw = html_text[brace_start:end]
    try:
        return json.loads(raw)
    except Exception:
        return None


def parse_comments_from_initdata(html_text: str, max_comments: int) -> List[Dict]:
    data = extract_json_object_after_var(html_text, "initDataReviews")
    if not data:
        return []

    comments = []
    items = data.get("Items") or []
    for item in items[:max_comments]:
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


def parse_comments_from_html(soup: BeautifulSoup, max_comments: int) -> List[Dict]:
    comments = []
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


def crawl_foody_comments(session: requests.Session, foody_url: str, max_comments: int) -> Dict:
    review_url = foody_url.rstrip("/") + "/binh-luan"
    r = get(session, review_url, timeout=35, retries=3)
    html_text = r.text
    soup = BeautifulSoup(html_text, "html.parser")

    comments = parse_comments_from_initdata(html_text, max_comments=max_comments)
    if not comments:
        comments = parse_comments_from_html(soup, max_comments=max_comments)

    # Small review summary if visible
    summary = {}
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="restaurants_detail_raw.json")
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--max-comments", type=int, default=20)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--output-prefix", type=str, default="foody_top10_comments")
    args = parser.parse_args()

    input_path = Path(args.input)
    data = json.loads(input_path.read_text(encoding="utf-8"))

    top_items = data[: args.topk]

    session = requests.Session()
    session.headers.update(HEADERS)

    results = []

    for item in top_items:
        rank = item.get("rank")
        be_name = text_clean(item.get("name"))
        info = (((item.get("detail") or {}).get("data") or {}).get("restaurant_info") or {})
        address = text_clean(info.get("address") or info.get("display_address"))

        print(f"\n[TOP {rank}] {be_name}")

        foody_url = MANUAL_MAP.get(normalize_name(be_name))
        match_method = "manual_map" if foody_url else "search_fallback"

        if not foody_url:
            foody_url = search_foody_url_ddg(session, be_name, address)

        row = {
            "rank": rank,
            "be_restaurant_id": item.get("restaurant_id"),
            "be_name": be_name,
            "be_address": address,
            "foody_url": foody_url,
            "match_method": match_method,
            "crawl_status": "",
            "review_url": "",
            "fetched_comment_count": 0,
            "comments": [],
        }

        if not foody_url:
            row["crawl_status"] = "foody_url_not_found"
            print("  -> Foody URL not found")
            results.append(row)
            continue

        print(f"  -> Foody URL: {foody_url}")

        try:
            crawled = crawl_foody_comments(session, foody_url, max_comments=args.max_comments)
            row["crawl_status"] = "ok"
            row["review_url"] = crawled["review_url"]
            row["fetched_comment_count"] = crawled["fetched_comment_count"]
            row["comments"] = crawled["comments"]
            print(f"  -> comments: {row['fetched_comment_count']}")
        except Exception as e:
            row["crawl_status"] = f"failed: {e}"
            print(f"  -> failed: {e}")

        results.append(row)
        time.sleep(args.sleep)

    output_prefix = Path(args.output_prefix)
    json_path = output_prefix.with_suffix(".json")
    csv_path = output_prefix.with_name(output_prefix.name + "_flat").with_suffix(".csv")

    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    flat_rows = []
    for rec in results:
        if rec["comments"]:
            for c in rec["comments"]:
                flat_rows.append(
                    {
                        "rank": rec["rank"],
                        "be_restaurant_id": rec["be_restaurant_id"],
                        "be_name": rec["be_name"],
                        "be_address": rec["be_address"],
                        "foody_url": rec["foody_url"],
                        "review_url": rec["review_url"],
                        "match_method": rec["match_method"],
                        "crawl_status": rec["crawl_status"],
                        "review_id": c.get("review_id"),
                        "author": c.get("author"),
                        "title": c.get("title"),
                        "rating": c.get("rating"),
                        "created_at": c.get("created_at"),
                        "content": c.get("content"),
                        "device": c.get("device"),
                        "likes": c.get("likes"),
                        "comment_count": c.get("comment_count"),
                        "comment_url": c.get("url"),
                    }
                )
        else:
            flat_rows.append(
                {
                    "rank": rec["rank"],
                    "be_restaurant_id": rec["be_restaurant_id"],
                    "be_name": rec["be_name"],
                    "be_address": rec["be_address"],
                    "foody_url": rec["foody_url"],
                    "review_url": rec["review_url"],
                    "match_method": rec["match_method"],
                    "crawl_status": rec["crawl_status"],
                    "review_id": None,
                    "author": "",
                    "title": "",
                    "rating": "",
                    "created_at": "",
                    "content": "",
                    "device": "",
                    "likes": None,
                    "comment_count": None,
                    "comment_url": "",
                }
            )

    if flat_rows:
        with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(flat_rows[0].keys()))
            writer.writeheader()
            writer.writerows(flat_rows)

    print("\nDONE")
    print(f"JSON: {json_path.resolve()}")
    print(f"CSV : {csv_path.resolve()}")


if __name__ == "__main__":
    main()
