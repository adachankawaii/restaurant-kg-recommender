import argparse
import csv
import time
from typing import Any, Dict, List, Tuple

import requests

GET_RATING_URL = "https://gw.be.com.vn/api/v1/be-merchant-gateway/web/customer/restaurant/ratings"
DEFAULT_TOKEN = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJ1c2VyX2lkIjowLCJhdWQiOiJndWVzdCIsImV4cCI6MTc3NDUzOTA5NywiaWF0IjoxNzc0NDUyNjk3LCJpc3MiOiJiZS1kZWxpdmVyeS1nYXRld2F5In0."
    "-Osa8PHoT4h7FmBWKPKjdZLCsQs0KXDjzYlIg1TAmcU"
)


def build_headers(token: str) -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Origin": "https://food.be.com.vn",
        "Referer": "https://food.be.com.vn/",
        "User-Agent": "Mozilla/5.0",
    }


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def read_unique_stores(store_csv_path: str) -> List[Tuple[str, str]]:
    stores: List[Tuple[str, str]] = []
    seen = set()

    with open(store_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            store_id = safe_str(row.get("store_id")).strip()
            store_name = safe_str(row.get("store_name")).strip()
            if not store_id or store_id in seen:
                continue
            seen.add(store_id)
            stores.append((store_id, store_name))

    return stores


def extract_ratings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if isinstance(payload.get("ratings"), list):
        return payload["ratings"]

    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("ratings"), list):
        return data["ratings"]

    return []


def fetch_rating(store_id: str, token: str, timeout: int = 30) -> Dict[str, Any]:
    headers = build_headers(token)
    payload = {"restaurant_id": store_id}
    response = requests.post(GET_RATING_URL, headers=headers, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def crawl_feedback(
    stores: List[Tuple[str, str]],
    token: str,
    sleep_sec: float,
    retries: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for index, (store_id, store_name) in enumerate(stores, start=1):
        last_error = ""
        rating_items: List[Dict[str, Any]] = []

        for attempt in range(1, retries + 1):
            try:
                payload = fetch_rating(store_id, token)
                rating_items = extract_ratings(payload)
                break
            except Exception as exc:  # noqa: BLE001
                last_error = safe_str(exc)
                if attempt < retries:
                    time.sleep(min(1.0, sleep_sec))

        if rating_items:
            for item in rating_items:
                rows.append(
                    {
                        "store_id": store_id,
                        "store_name": store_name,
                        "rated_at": safe_str(item.get("rated_at")),
                        "rating": safe_str(item.get("rating")),
                        "feedback": safe_str(item.get("feedback")),
                        "crawl_status": "ok",
                        "error": "",
                    }
                )
        else:
            rows.append(
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "rated_at": "",
                    "rating": "",
                    "feedback": "",
                    "crawl_status": "empty_or_failed",
                    "error": last_error,
                }
            )

        print(f"[{index}/{len(stores)}] done store_id={store_id}")
        time.sleep(sleep_sec)

    return rows


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "store_id",
        "store_name",
        "rated_at",
        "rating",
        "feedback",
        "crawl_status",
        "error",
    ]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl feedback/rating for each store_id in a store CSV file."
    )
    parser.add_argument(
        "--input",
        default="store_from_top5_json.csv",
        help="Input CSV with store_id/store_name columns.",
    )
    parser.add_argument(
        "--output",
        default="store_feedback_crawled.csv",
        help="Output CSV file path.",
    )
    parser.add_argument(
        "--token",
        default=DEFAULT_TOKEN,
        help="Bearer token for BE rating API.",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.25,
        help="Delay between store requests.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=2,
        help="Retry attempts per store when request fails.",
    )
    parser.add_argument(
        "--max-stores",
        type=int,
        default=0,
        help="Optional cap on number of stores to crawl (0 = all).",
    )
    args = parser.parse_args()

    stores = read_unique_stores(args.input)
    if args.max_stores and args.max_stores > 0:
        stores = stores[: args.max_stores]

    print(f"Total unique stores to crawl: {len(stores)}")
    rows = crawl_feedback(
        stores=stores,
        token=args.token,
        sleep_sec=args.sleep_sec,
        retries=max(1, args.retries),
    )
    write_csv(args.output, rows)
    print(f"Created: {args.output} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
