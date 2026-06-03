import os
import math
import time
import json
import csv
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import requests


BASE_URL = "https://gw.be.com.vn/api/v1/be-marketplace/web"
SEARCH_DISHES_URL = f"{BASE_URL}/search/dishes"
GET_RESTAURANTS_URL = f"{BASE_URL}/merchant_category/get_restaurants"
DETAIL_URL = f"{BASE_URL}/restaurant/detail"
GET_RATING_URL = "https://gw.be.com.vn/api/v1/be-merchant-gateway/web/customer/restaurant/ratings"

BEARER_TOKEN = "eyJhbGciOiJIUzUxMiIsImtpZCI6IlQwVm9NazF0Y0c5YVYzUnJWMGN3ZVZNeU5YQldibFpUWlZWR2RWVkZVakJWU0ZwSFkwWk9iRlpWTlZCWk0wVjRVMFJXUW1GdGVFZGpSMHAzVW10M01tSnVjR3RUTUhoRVV6Rm9WMDVxU25GamF6bFZVbXRKTVZwR1drUlJhemx2VjFaS1dGbHJhekpaYmtreFlWZEdORkV5UmtWa1NGSkRXak5PYlZwRmNHOWhWM2haWTFWU1ZsWXhXVE5TYmswelUwaHdjMU5YWjNoT01IUk5aRWRHUzFsWGNGZFNNMDAxVVZad05GbHJWakZqYmxwRlltNW5OR0ZYWkRKaGJtczBVMGhaZVdGdGFHeGhNbEpaWWxSS1RHSnRiRmRrVmtvMSIsInR5cCI6IkpXVCJ9.eyJhdWQiOiIxIiwiZXhwIjoxNzc2OTMyNDc3LCJ1c2VyX2lkIjo1ODU1NTQ4LCJzZXNzaW9uX2lkIjoiMTJlZTMyNmJjNTIxNTVlZWY1ZWRiODVmZGJiZWU5MzUiLCJ2ZW5kb3IiOiJXZWIgRGVsaXZlcnkifQ.-OuYqjhK4oROG0xCkhzSUnWRgK8RsFUr70Zs_0uJqz6Co2FobvoUpdR4m6ZQWkcbfKEPDfteMX5pT51Yoxs2lg"
if not BEARER_TOKEN:
    raise ValueError("Missing BE_BEARER_TOKEN environment variable")

CLIENT_INFO = {
    "locale": "vi",
    "app_version": "11322",
    "version": "1.1.322",
    "device_type": 3,
    "customer_package_name": "xyz.be.food",
    "device_token": "b87543ecbc0ba610d9f06f9f2c432a46",
    "operator_token": "0b28e008bc323838f5ec84f718ef11e6",
    "screen_height": 640,
    "screen_width": 360,
    "latitude": 21.005118,
    "longitude": 105.845592,
    "ad_id": ""
}


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return r * c


def build_headers(token: str) -> Dict[str, str]:
    return {
        "accept": "*/*",
        "accept-language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5,zh-CN;q=0.4,zh;q=0.3",
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "origin": "https://food.be.com.vn",
        "referer": "https://food.be.com.vn/",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
        "app_version": CLIENT_INFO["app_version"],
        "version": CLIENT_INFO["version"],
    }


def post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        print("URL:", url)
        print("STATUS:", resp.status_code)
        print("RESPONSE:", resp.text[:2000])
        print("PAYLOAD:", json.dumps(payload, ensure_ascii=False))
    resp.raise_for_status()
    return resp.json()


def get_nested_value(d: Dict[str, Any], paths: List[List[str]]) -> Any:
    for path in paths:
        cur = d
        ok = True
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                ok = False
                break
            cur = cur[key]
        if ok:
            return cur
    return None


def find_first_list(obj: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(obj, list):
        if not obj or isinstance(obj[0], dict):
            return obj
        return None

    if isinstance(obj, dict):
        for value in obj.values():
            result = find_first_list(value)
            if result is not None:
                return result

    return None


def build_client_info(lat: float, lng: float) -> Dict[str, Any]:
    client_info = dict(CLIENT_INFO)
    client_info["latitude"] = lat
    client_info["longitude"] = lng
    return client_info


def extract_lat_lon(item: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    candidates = [
        (item.get("latitude"), item.get("longitude")),
        (item.get("lat"), item.get("lng")),
        (item.get("lat"), item.get("lon")),
        (
            get_nested_value(item, [["location", "latitude"]]),
            get_nested_value(item, [["location", "longitude"]]),
        ),
        (
            get_nested_value(item, [["location", "lat"]]),
            get_nested_value(item, [["location", "lng"]]),
        ),
        (
            get_nested_value(item, [["position", "lat"]]),
            get_nested_value(item, [["position", "lng"]]),
        ),
        (
            get_nested_value(item, [["restaurant", "latitude"]]),
            get_nested_value(item, [["restaurant", "longitude"]]),
        ),
        (
            get_nested_value(item, [["vendor", "latitude"]]),
            get_nested_value(item, [["vendor", "longitude"]]),
        ),
    ]

    for lat, lon in candidates:
        if lat is not None and lon is not None:
            try:
                return float(lat), float(lon)
            except (TypeError, ValueError):
                pass
    return None


def extract_restaurant_id(item: Dict[str, Any]) -> Optional[str]:
    candidates = [
        item.get("restaurant_id"),
        item.get("merchant_id"),
        item.get("vendor_id"),
        item.get("id"),
        get_nested_value(item, [["restaurant", "restaurant_id"]]),
        get_nested_value(item, [["restaurant", "id"]]),
        get_nested_value(item, [["vendor", "id"]]),
        get_nested_value(item, [["merchant", "id"]]),
    ]
    for x in candidates:
        if x is not None:
            return str(x)
    return None


def extract_name(item: Dict[str, Any]) -> str:
    candidates = [
        item.get("name"),
        item.get("restaurant_name"),
        item.get("vendor_name"),
        item.get("merchant_name"),
        get_nested_value(item, [["restaurant", "name"]]),
        get_nested_value(item, [["vendor", "name"]]),
        get_nested_value(item, [["merchant", "name"]]),
    ]
    for x in candidates:
        if isinstance(x, str) and x.strip():
            return x.strip()
    return "unknown"


def build_search_dishes_payload(search_text: str, lat: float, lng: float, page: int = 1, limit: int = 100) -> Dict[str, Any]:
    client_info = build_client_info(lat, lng)
    return {
        "search_text": search_text,
        "filters": [],
        "page": page,
        "limit": limit,
        "locale": client_info["locale"],
        "app_version": client_info["app_version"],
        "version": client_info["version"],
        "device_type": client_info["device_type"],
        "latitude": lat,
        "longitude": lng,
        "ad_id": client_info["ad_id"],
        "client_info": client_info,
    }


def build_get_restaurants_payload(lat: float, lng: float, page: int = 1, limit: int = 100) -> Dict[str, Any]:
    client_info = build_client_info(lat, lng)
    return {
        "page": page,
        "limit": limit,
        "locale": client_info["locale"],
        "app_version": client_info["app_version"],
        "version": client_info["version"],
        "device_type": client_info["device_type"],
        "latitude": lat,
        "longitude": lng,
        "ad_id": client_info["ad_id"],
        "client_info": client_info,
    }


def build_detail_payload(restaurant_id: str, lat: float, lng: float) -> Dict[str, Any]:
    client_info = build_client_info(lat, lng)
    return {
        "restaurant_id": str(restaurant_id),
        "locale": client_info["locale"],
        "app_version": client_info["app_version"],
        "version": client_info["version"],
        "device_type": client_info["device_type"],
        "ad_id": client_info["ad_id"],
        "latitude": lat,
        "longitude": lng,
        "client_info": client_info,
    }


def search_dishes(search_text: str, lat: float, lng: float, page: int = 1, limit: int = 100) -> Dict[str, Any]:
    headers = build_headers(BEARER_TOKEN)
    payload = build_search_dishes_payload(search_text, lat, lng, page, limit)
    return post_json(SEARCH_DISHES_URL, headers, payload)

def fetch_restaurants(lat: float, lng: float, page: int = 1, limit: int = 100) -> List[Dict[str, Any]]:
    headers = build_headers(BEARER_TOKEN)
    payload = build_get_restaurants_payload(lat, lng, page, limit)
    data = post_json(GET_RESTAURANTS_URL, headers, payload)

    items = find_first_list(data)
    if not items:
        return []

    results = []
    for item in items:
        if not isinstance(item, dict):
            continue

        restaurant_id = extract_restaurant_id(item)
        coords = extract_lat_lon(item)

        # vẫn giữ cả item dù thiếu coords, miễn có restaurant_id
        if not restaurant_id:
            continue

        item_lat = None
        item_lng = None
        distance_km = None

        if coords:
            item_lat, item_lng = coords
            distance_km = haversine_km(lat, lng, item_lat, item_lng)

        results.append({
            "restaurant_id": restaurant_id,
            "name": extract_name(item),
            "latitude": item_lat,
            "longitude": item_lng,
            "distance_km": distance_km,
            "raw": item,          # full object từ get_restaurants
        })

    results.sort(key=lambda x: x["distance_km"] if x["distance_km"] is not None else 10**9)
    return results

def fetch_restaurants_safe(lat: float, lng: float, page: int = 1, limit: int = 100) -> List[Dict[str, Any]]:
    try:
        return fetch_restaurants(lat, lng, page=page, limit=limit)
    except requests.HTTPError as e:
        print(f"fetch_restaurants failed: {e}")
        if e.response is not None:
            print("status:", e.response.status_code)
            print("body:", e.response.text[:1000])
        return []


def fetch_detail(restaurant_id: str, lat: float, lng: float) -> Dict[str, Any]:
    headers = build_headers(BEARER_TOKEN)
    payload = build_detail_payload(restaurant_id, lat, lng)
    return post_json(DETAIL_URL, headers, payload)


def fetch_rating(restaurant_id: str) -> Dict[str, Any]:
    headers = build_headers(BEARER_TOKEN)
    payload = {"restaurant_id": restaurant_id}
    return post_json(GET_RATING_URL, headers, payload)


def extract_dish_hits(obj: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            maybe_id = extract_restaurant_id(x)
            maybe_name = extract_name(x)
            if maybe_id or maybe_name != "unknown":
                out.append(x)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(obj)
    return out


def extract_dish_name(item: Dict[str, Any]) -> Optional[str]:
    candidates = [
        item.get("dish_name"),
        get_nested_value(item, [["dish", "name"]]),
        get_nested_value(item, [["item", "name"]]),
        get_nested_value(item, [["product", "name"]]),
    ]
    for x in candidates:
        if isinstance(x, str) and x.strip():
            return x.strip()
    return None


def extract_dish_record(item: Dict[str, Any], query_lat: float, query_lng: float, matched_term: str) -> Optional[Dict[str, Any]]:
    restaurant_id = extract_restaurant_id(item)
    if not restaurant_id:
        return None

    coords = extract_lat_lon(item)
    if not coords:
        coords = extract_lat_lon(get_nested_value(item, [["restaurant"]]) or {})
    lat, lng = coords if coords else (None, None)

    restaurant_name = (
        get_nested_value(item, [["restaurant", "name"]])
        or get_nested_value(item, [["vendor", "name"]])
        or item.get("restaurant_name")
        or item.get("vendor_name")
        or extract_name(item)
    )

    distance_m = None
    if lat is not None and lng is not None:
        distance_m = haversine_km(query_lat, query_lng, float(lat), float(lng)) * 1000

    return {
        "restaurant_id": restaurant_id,
        "restaurant_name": restaurant_name,
        "latitude": lat,
        "longitude": lng,
        "distance_m": distance_m,
        "matched_term": matched_term,
        "dish_name": extract_dish_name(item),
        "raw": item,
    }


def parse_terms(term_field: str) -> List[str]:
    return [x.strip() for x in str(term_field).split("|") if x.strip()]


def read_user_scenarios(csv_path: str) -> List[Dict[str, Any]]:
    rows = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(2048)
        f.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ","

        reader = csv.DictReader(f, delimiter=delimiter)
        reader.fieldnames = [fn.strip() if fn else fn for fn in reader.fieldnames]

        for idx, row in enumerate(reader, start=1):
            clean_row = {
                (k.strip() if k else k): (v.strip() if isinstance(v, str) else v)
                for k, v in row.items()
            }

            rows.append({
                "user_id": clean_row.get("user_id") or f"u{idx:03d}",
                "area_id": clean_row.get("area_id", ""),
                "query_lat": float(clean_row["query_lat"]),
                "query_lng": float(clean_row["query_lng"]),
                "time_slot_id": clean_row.get("time_slot_id", ""),
                "term": clean_row["term"],
                "desired_price_range_id": clean_row.get("desired_price_range_id", ""),
                "preferred_aspects": clean_row.get("preferred_aspects", ""),
                "distance_tolerance_m": float(clean_row["distance_tolerance_m"]),
            })

    return rows


def deduplicate_candidates(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = defaultdict(lambda: {
        "restaurant_id": None,
        "restaurant_name": None,
        "latitude": None,
        "longitude": None,
        "distance_m": None,
        "matched_terms": set(),
        "menu": set(),
        "term_hits": 0,
    })

    for rec in records:
        rid = rec["restaurant_id"]
        g = grouped[rid]
        g["restaurant_id"] = rid
        g["restaurant_name"] = rec["restaurant_name"]
        g["latitude"] = rec["latitude"]
        g["longitude"] = rec["longitude"]

        if rec["distance_m"] is not None:
            if g["distance_m"] is None or rec["distance_m"] < g["distance_m"]:
                g["distance_m"] = rec["distance_m"]

        if rec["matched_term"]:
            g["matched_terms"].add(rec["matched_term"])
            g["term_hits"] += 1

        if rec["dish_name"]:
            g["menu"].add(rec["dish_name"])

    final_items = []
    for item in grouped.values():
        item["matched_terms"] = sorted(item["matched_terms"])
        item["menu"] = sorted(item["menu"])
        final_items.append(item)

    return final_items


def score_candidate(item: Dict[str, Any], total_terms: int) -> float:
    match_ratio = len(item["matched_terms"]) / total_terms if total_terms else 0.0
    hit_bonus = min(item["term_hits"], 10) / 10.0
    distance_score = 0.0
    if item["distance_m"] is not None:
        distance_score = 1.0 / (1.0 + item["distance_m"])
    return 0.7 * match_ratio + 0.2 * hit_bonus + 0.1 * distance_score


def pick_top_k(candidates: List[Dict[str, Any]], total_terms: int, k: int = 5) -> List[Dict[str, Any]]:
    for item in candidates:
        item["score"] = score_candidate(item, total_terms)

    ranked = sorted(
        candidates,
        key=lambda x: (
            x["score"],
            -(x["distance_m"] if x["distance_m"] is not None else 10**9),
        ),
        reverse=True,
    )
    return ranked[:k]

def build_restaurant_map(restaurants: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out = {}
    for r in restaurants:
        rid = r.get("restaurant_id")
        if rid is not None:
            out[str(rid)] = r
    return out


def enrich_topk_with_full_detail(
    topk: List[Dict[str, Any]],
    lat: float,
    lng: float,
    sleep_sec: float = 0.2
) -> List[Dict[str, Any]]:
    enriched = []

    for idx, item in enumerate(topk, start=1):
        out = dict(item)
        out["rank"] = idx

        try:
            detail = fetch_detail(item["restaurant_id"], lat, lng)
            out["detail"] = detail
        except requests.RequestException as e:
            print(f"fetch_detail failed for restaurant_id={item['restaurant_id']}: {e}")
            out["detail"] = {}

        enriched.append(out)
        time.sleep(sleep_sec)

    return enriched

def enrich_topk_with_restaurant_info(
    topk: List[Dict[str, Any]],
    restaurant_map: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    enriched = []

    for item in topk:
        restaurant = restaurant_map.get(str(item["restaurant_id"]))
        out = dict(item)

        if restaurant:
            out["restaurant_name_final"] = restaurant.get("name") or item.get("restaurant_name")
            out["restaurant_latitude"] = restaurant.get("latitude")
            out["restaurant_longitude"] = restaurant.get("longitude")
            out["restaurant_distance_km"] = (
                round(restaurant["distance_km"], 3)
                if restaurant.get("distance_km") is not None else None
            )

            # full response object từ get_restaurants
            out["restaurant_detail_from_get_restaurants"] = restaurant.get("raw", {})
        else:
            out["restaurant_name_final"] = item.get("restaurant_name")
            out["restaurant_latitude"] = item.get("latitude")
            out["restaurant_longitude"] = item.get("longitude")
            out["restaurant_distance_km"] = (
                round(item["distance_m"] / 1000, 3)
                if item.get("distance_m") is not None else None
            )

            # fallback nếu không map được
            out["restaurant_detail_from_get_restaurants"] = {}

        enriched.append(out)

    return enriched

def process_one_user(user_row: Dict[str, Any], sleep_sec: float = 0.2, top_k: int = 5) -> Dict[str, Any]:
    user_id = user_row.get("user_id", "unknown")
    lat = user_row["query_lat"]
    lng = user_row["query_lng"]
    distance_tolerance_m = user_row["distance_tolerance_m"]
    terms = parse_terms(user_row["term"])

    all_records = []

    for term in terms:
        try:
            resp = search_dishes(term, lat, lng, page=1, limit=100)
            hits = extract_dish_hits(resp)

            for hit in hits:
                rec = extract_dish_record(hit, lat, lng, matched_term=term)
                if not rec:
                    continue
                if rec["distance_m"] is None:
                    continue
                if rec["distance_m"] <= distance_tolerance_m:
                    all_records.append(rec)

            time.sleep(sleep_sec)

        except requests.RequestException as e:
            print(f"[{user_id}] search_dishes failed for term={term}: {e}")
            continue

    candidates = deduplicate_candidates(all_records)
    topk = pick_top_k(candidates, total_terms=len(terms), k=top_k)

    restaurants = fetch_restaurants_safe(lat, lng, page=1, limit=100)
    restaurant_map = build_restaurant_map(restaurants)
    topk = enrich_topk_with_restaurant_info(topk, restaurant_map)

    # thêm full detail giống mẫu bạn muốn
    topk = enrich_topk_with_full_detail(topk, lat, lng, sleep_sec=sleep_sec)

    return {
        "user_id": user_id,
        "area_id": user_row["area_id"],
        "query_lat": lat,
        "query_lng": lng,
        "time_slot_id": user_row["time_slot_id"],
        "term": user_row["term"],
        "desired_price_range_id": user_row["desired_price_range_id"],
        "preferred_aspects": user_row["preferred_aspects"],
        "distance_tolerance_m": distance_tolerance_m,
        "candidate_count": len(candidates),
        "top_restaurants": topk,
    }

def main():
    input_csv = "user_scenarios_1.csv"
    output_json = "top5_restaurants.json"

    users = read_user_scenarios(input_csv)
    all_results = []

    for user_row in users:
        print(f"Processing user {user_row['user_id']} ...")
        result = process_one_user(user_row, sleep_sec=0.2, top_k=5)
        all_results.append(result)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)

    print(f"Saved to {output_json}")


if __name__ == "__main__":
    main()

