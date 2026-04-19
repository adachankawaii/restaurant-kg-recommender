import math
import time
from typing import Any, Dict, List, Optional
import json
import requests


BASE_URL = "https://gw.be.com.vn/api/v1/be-marketplace/web"
GET_VENDORS_URL = f"{BASE_URL}/get_vendors"
DETAIL_URL = f"{BASE_URL}/restaurant/detail"
GET_RATING_URL = f"https://gw.be.com.vn/api/v1/be-merchant-gateway/web/customer/restaurant/ratings"
BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjowLCJhdWQiOiJndWVzdCIsImV4cCI6MTc3NDUzOTA5NywiaWF0IjoxNzc0NDUyNjk3LCJpc3MiOiJiZS1kZWxpdmVyeS1nYXRld2F5In0.-Osa8PHoT4h7FmBWKPKjdZLCsQs0KXDjzYlIg1TAmcU"

CLIENT_INFO = {
    "locale": "vi",
    "app_version": "11322",
    "version": "1.1.322",
    "device_type": 3,
    "customer_package_name": "xyz.be.food",
    "device_token": "91e1a2a41c0741f7f47615ab9de2fb8a",
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
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Origin": "https://food.be.com.vn",
        "Referer": "https://food.be.com.vn/",
        "User-Agent": "Mozilla/5.0"
    }


def build_vendor_payload(page: int = 1, limit: int = 20) -> Dict[str, Any]:
    return {
        "page": page,
        "limit": limit,
        "locale": CLIENT_INFO["locale"],
        "app_version": CLIENT_INFO["app_version"],
        "version": CLIENT_INFO["version"],
        "device_type": CLIENT_INFO["device_type"],
        "latitude": CLIENT_INFO["latitude"],
        "longitude": CLIENT_INFO["longitude"],
        "ad_id": CLIENT_INFO["ad_id"],
        "client_info": CLIENT_INFO
    }


def build_detail_payload(restaurant_id: str) -> Dict[str, Any]:
    return {
        "restaurant_id": str(restaurant_id),
        "locale": CLIENT_INFO["locale"],
        "app_version": CLIENT_INFO["app_version"],
        "version": CLIENT_INFO["version"],
        "device_type": CLIENT_INFO["device_type"],
        "ad_id": CLIENT_INFO["ad_id"],
        "latitude": CLIENT_INFO["latitude"],
        "longitude": CLIENT_INFO["longitude"],
        "client_info": CLIENT_INFO
    }


def post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def find_first_list(obj: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(obj, list):
        if not obj or isinstance(obj[0], dict):
            return obj
        return None

    if isinstance(obj, dict):
        for _, value in obj.items():
            result = find_first_list(value)
            if result is not None:
                return result

    return None


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


def extract_lat_lon(item: Dict[str, Any]) -> Optional[tuple]:
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
        item.get("vendor_id"),
        item.get("id"),
        get_nested_value(item, [["restaurant", "id"]]),
        get_nested_value(item, [["vendor", "id"]]),
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
        get_nested_value(item, [["restaurant", "name"]]),
        get_nested_value(item, [["vendor", "name"]]),
    ]
    for x in candidates:
        if isinstance(x, str) and x.strip():
            return x.strip()
    return "unknown"

def fetch_rating(restaurant_id: str) -> Dict[str, Any]:
    headers = build_headers(BEARER_TOKEN)
    payload = {"restaurant_id": restaurant_id}
    return post_json(GET_RATING_URL, headers, payload)

def fetch_vendors(page: int = 1, limit: int = 20) -> List[Dict[str, Any]]:
    headers = build_headers(BEARER_TOKEN)
    payload = build_vendor_payload(page=page, limit=limit)
    data = post_json(GET_VENDORS_URL, headers, payload)

    items = find_first_list(data)
    if not items:
        return []

    valid_items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        restaurant_id = extract_restaurant_id(item)
        coords = extract_lat_lon(item)
        if restaurant_id and coords:
            lat, lon = coords
            distance_km = haversine_km(
                CLIENT_INFO["latitude"],
                CLIENT_INFO["longitude"],
                lat,
                lon
            )
            valid_items.append({
                "restaurant_id": restaurant_id,
                "name": extract_name(item),
                "latitude": lat,
                "longitude": lon,
                "distance_km": distance_km,
                "raw": item
            })

    valid_items.sort(key=lambda x: x["distance_km"])
    return valid_items


def fetch_detail(restaurant_id: str) -> Dict[str, Any]:
    headers = build_headers(BEARER_TOKEN)
    payload = build_detail_payload(restaurant_id)
    return post_json(DETAIL_URL, headers, payload)


def fetch_detail_of_x_nearest(x: int = 10, page: int = 1, limit: int = 50, sleep_sec: float = 0.2) -> List[Dict[str, Any]]:
    vendors = fetch_vendors(page=page, limit=limit)
    nearest = vendors[:x]

    results = []
    for idx, vendor in enumerate(nearest, start=1):
        restaurant_id = vendor["restaurant_id"]
        detail = fetch_detail(restaurant_id)
        if 'data' in detail:
            if 'currency_code' in detail['data']:
                del detail['data']['currency_code']
            if 'currency' in detail['data']:
                del detail['data']['currency']
            if 'categories' in detail['data']:
                del detail['data']['categories']
            if 'flash_sale_categories' in detail['data']:
                del detail['data']['flash_sale_categories']
        rating = fetch_rating(restaurant_id)
        rating_data = [] 
        for r in rating.get("ratings", []):
            rating_data.append({
                "rated_at": r.get("rated_at"),
                "rating": r.get("rating"),
                "feedback": r.get("feedback")
            })
        results.append({
            "rank": idx,
            "restaurant_id": restaurant_id,
            "name": vendor["name"],
            "distance_km": round(vendor["distance_km"], 3),
            "latitude": vendor["latitude"],
            "longitude": vendor["longitude"],
            "detail": detail,
            "rating": rating_data,
        })

        time.sleep(sleep_sec)

    return results


if __name__ == "__main__":
    try:
        x = 10
        details = fetch_detail_of_x_nearest(x=x, page=1, limit=50)

        # for item in details:
        #     print(
        #         f"[{item['rank']}] {item['name']} | "
        #         f"id={item['restaurant_id']} | "
        #         f"distance={item['distance_km']} km"
        #     )
        with open("restaurants_detail_raw5.json", "w", encoding="utf-8") as f:
            json.dump(details, f, ensure_ascii=False, indent=2)
        print("Đã lưu vào file restaurants_detail_raw5.json")
        # Muốn xem detail thô của nhà hàng đầu tiên:
        # import json
        # print(json.dumps(details[0]["detail"], ensure_ascii=False, indent=2))

    except requests.HTTPError as e:
        print("HTTP error:", e)
        if e.response is not None:
            print("Status code:", e.response.status_code)
            print("Response body:", e.response.text)
    except Exception as e:
        print("Error:", e)