"""Crawler Google Maps theo danh sách be_store trong CSV bằng SerpApi."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import requests


SERPAPI_SEARCH_URL = "https://serpapi.com/search.json"
ENV_API_KEY_NAME = "SERPAPI_API_KEY"


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _resolve_api_key(cli_value: str | None) -> str:
    if cli_value and cli_value.strip():
        return cli_value.strip()

    env_value = os.getenv(ENV_API_KEY_NAME, "").strip()
    if env_value:
        return env_value

    _load_env_file(Path(".env"))
    return os.getenv(ENV_API_KEY_NAME, "").strip()


@dataclass
class StoreQuery:
    store_id: str
    name: str
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass
class Restaurant:
    store_id: str
    name: str
    query_name: str
    query_address: str | None
    data_id: str | None
    place_id: str | None
    lat: float | None
    lon: float | None
    rating: float | None
    review_count: int | None
    details: dict[str, Any]
    source: str = "SerpApi/google_maps"


def _parse_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None



def _parse_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None



def _remove_image_fields(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned: dict[str, Any] = {}
        for key, value in data.items():
            k = key.lower()
            if "thumbnail" in k or "image" in k or "photo" in k or "picture" in k:
                continue
            cleaned[key] = _remove_image_fields(value)
        return cleaned
    if isinstance(data, list):
        return [_remove_image_fields(x) for x in data]
    return data



def _extract_lat_lon(result: dict[str, Any]) -> tuple[float | None, float | None]:
    gps = result.get("gps_coordinates") or {}
    lat = _parse_float(gps.get("latitude") or result.get("latitude"))
    lon = _parse_float(gps.get("longitude") or result.get("longitude"))
    return lat, lon



def _fetch_place_details(data_id: str | None, place_id: str | None, api_key: str) -> dict[str, Any]:
    candidate_params: list[dict[str, str]] = []
    if data_id:
        candidate_params.append(
            {
                "engine": "google_maps",
                "type": "place",
                "data_id": data_id,
                "hl": "vi",
                "gl": "vn",
                "api_key": api_key,
            }
        )
        candidate_params.append(
            {
                "engine": "google_maps",
                "data_id": data_id,
                "hl": "vi",
                "gl": "vn",
                "api_key": api_key,
            }
        )

    if place_id:
        candidate_params.append(
            {
                "engine": "google_maps",
                "place_id": place_id,
                "hl": "vi",
                "gl": "vn",
                "api_key": api_key,
            }
        )

    last_error = "Không có data_id/place_id để lấy chi tiết"
    for params in candidate_params:
        try:
            resp = requests.get(SERPAPI_SEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
            result = resp.json()
            if result.get("error"):
                last_error = str(result["error"])
                continue
            if result.get("place_results") or result.get("title") or result.get("name"):
                return _remove_image_fields(result)
            last_error = "Phản hồi không chứa place details"
        except Exception as exc:
            last_error = str(exc)

    raise RuntimeError(last_error)



def _search_store(query: StoreQuery, api_key: str) -> Restaurant:
    search_term = query.name.strip()
    if query.address:
        search_term = f"{search_term}, {query.address}"

    params = {
        "engine": "google_maps",
        "q": search_term,
        "hl": "vi",
        "gl": "vn",
        "api_key": api_key,
    }

    if query.latitude is not None and query.longitude is not None:
        params["ll"] = f"@{query.latitude},{query.longitude},17z"

    resp = requests.get(SERPAPI_SEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    results = resp.json()
    if results.get("error"):
        raise RuntimeError(str(results["error"]))

    local_results = results.get("local_results") or []
    if local_results:
        raw = local_results[0]
    else:
        place_result = results.get("place_results")
        if not isinstance(place_result, dict):
            raise RuntimeError("Không tìm thấy local_results/place_results phù hợp")
        raw = place_result

    local_clean = _remove_image_fields(raw)
    data_id = raw.get("data_id")
    place_id = raw.get("place_id") or data_id

    try:
        detail_payload = _fetch_place_details(data_id, place_id, api_key)
    except Exception as exc:
        detail_payload = {"detail_error": str(exc)}

    lat, lon = _extract_lat_lon(raw)
    return Restaurant(
        store_id=query.store_id,
        name=(raw.get("title") or raw.get("name") or query.name).strip(),
        query_name=query.name,
        query_address=query.address,
        data_id=data_id,
        place_id=place_id,
        lat=lat,
        lon=lon,
        rating=_parse_float(raw.get("rating")),
        review_count=_parse_int(raw.get("reviews")),
        details={
            "local_result": local_clean,
            "place_result": detail_payload,
        },
    )



def load_stores_from_csv(csv_path: Path) -> list[StoreQuery]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        name_column = "name" if "name" in fieldnames else "store_name" if "store_name" in fieldnames else None
        if "store_id" not in fieldnames or not name_column:
            missing = []
            if "store_id" not in fieldnames:
                missing.append("store_id")
            if not name_column:
                missing.append("name/store_name")
            raise ValueError(f"CSV thiếu cột bắt buộc: {', '.join(missing)}")

        stores: list[StoreQuery] = []
        seen_store_ids: set[str] = set()
        for row in reader:
            store_id = (row.get("store_id") or "").strip()
            name = (row.get(name_column) or "").strip()
            if not store_id or not name:
                continue
            if store_id in seen_store_ids:
                continue
            seen_store_ids.add(store_id)
            stores.append(
                StoreQuery(
                    store_id=store_id,
                    name=name,
                    address=(row.get("address") or "").strip() or None,
                    latitude=_parse_float(row.get("latitude")),
                    longitude=_parse_float(row.get("longitude")),
                )
            )
    return stores



def _pick_first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None



def _extract_opening_hours(pr: dict[str, Any], lr: dict[str, Any]) -> dict[str, Any]:
    hours = _pick_first_non_empty(
        pr.get("hours"),
        pr.get("opening_hours"),
        pr.get("hours_of_operation"),
        lr.get("hours"),
        lr.get("opening_hours"),
    )

    open_state = _pick_first_non_empty(
        pr.get("open_state"),
        pr.get("hours_state"),
        lr.get("open_state"),
    )

    operating_hours = _pick_first_non_empty(
        pr.get("operating_hours"),
        pr.get("weekly_hours"),
    )

    result = {
        "open_state": open_state,
        "hours": hours,
        "operating_hours": operating_hours,
    }
    return {k: v for k, v in result.items() if v not in (None, "", [], {})}



def _ext_map(extensions: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for ext in extensions or []:
        if isinstance(ext, dict):
            for k, v in ext.items():
                result[k] = v
    return result



def _extract_essential(record: dict[str, Any]) -> dict[str, Any]:
    details = record.get("details") or {}
    lr = details.get("local_result") or {}
    pr_root = details.get("place_result") or {}
    pr = pr_root.get("place_results") or pr_root

    lr_ext = _ext_map(lr.get("extensions"))
    pr_ext = _ext_map(pr.get("extensions"))

    raw_reviews = (pr.get("user_reviews") or {}).get("most_relevant") or []
    clean_reviews = [
        {k: v for k, v in r.items() if k in ("username", "rating", "description")}
        for r in raw_reviews
        if r.get("description")
    ] or None

    gps = pr.get("gps_coordinates") or {"latitude": record.get("lat"), "longitude": record.get("lon")}

    out: dict[str, Any] = {
        "id": record.get("store_id"),
        "name": record.get("name"),
        "query_name": record.get("query_name"),
        "query_address": record.get("query_address"),
        "address": pr.get("address") or lr.get("address"),
        "location": gps,
        "rating": record.get("rating"),
        "review_count": record.get("review_count"),
        "rating_summary": pr.get("rating_summary") or None,
        "price": pr.get("price") or lr.get("price"),
        "price_details": pr.get("price_details") or None,
        "type": pr.get("type") or lr.get("types") or ([lr["type"]] if lr.get("type") else None),
        "phone": pr.get("phone") or lr.get("phone"),
        "website": pr.get("website") or lr.get("website"),
        "service_options": pr.get("service_options") or lr.get("service_options"),
        "highlights": pr_ext.get("highlights") or lr_ext.get("highlights"),
        "popular_for": pr_ext.get("popular_for") or lr_ext.get("popular_for"),
        "offerings": pr_ext.get("offerings") or lr_ext.get("offerings"),
        "atmosphere": pr_ext.get("atmosphere") or lr_ext.get("atmosphere"),
        "crowd": pr_ext.get("crowd") or lr_ext.get("crowd"),
        "dining_options": pr_ext.get("dining_options") or lr_ext.get("dining_options"),
        "opening_hours": _extract_opening_hours(pr, lr) or None,
        "reviews": clean_reviews,
        "google_maps_ids": {
            "data_id": record.get("data_id"),
            "place_id": record.get("place_id"),
        },
    }
    return {k: v for k, v in out.items() if v is not None}



def save_json(data: list[Restaurant], output_path: Path) -> None:
    clean = [_extract_essential(asdict(x)) for x in data]
    output_path.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")



def _join_field(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("name") or str(item))
        return "; ".join(parts)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)



def save_csv(data: list[Restaurant], output_path: Path) -> None:
    records = [_extract_essential(asdict(x)) for x in data]
    if not records:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames = [
        "id", "name", "query_name", "query_address", "address", "latitude", "longitude",
        "rating", "review_count", "price", "type", "phone", "website", "opening_hours",
        "service_options", "highlights", "popular_for", "offerings", "atmosphere", "crowd",
        "dining_options",
    ]

    with output_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in records:
            loc = r.get("location") or {}
            writer.writerow(
                {
                    "id": r.get("id", ""),
                    "name": r.get("name", ""),
                    "query_name": r.get("query_name", ""),
                    "query_address": r.get("query_address", ""),
                    "address": r.get("address", ""),
                    "latitude": loc.get("latitude", ""),
                    "longitude": loc.get("longitude", ""),
                    "rating": r.get("rating", ""),
                    "review_count": r.get("review_count", ""),
                    "price": r.get("price", ""),
                    "type": _join_field(r.get("type")),
                    "phone": r.get("phone", ""),
                    "website": r.get("website", ""),
                    "opening_hours": _join_field(r.get("opening_hours")),
                    "service_options": _join_field(r.get("service_options")),
                    "highlights": _join_field(r.get("highlights")),
                    "popular_for": _join_field(r.get("popular_for")),
                    "offerings": _join_field(r.get("offerings")),
                    "atmosphere": _join_field(r.get("atmosphere")),
                    "crowd": _join_field(r.get("crowd")),
                    "dining_options": _join_field(r.get("dining_options")),
                }
            )



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cào Google Maps theo danh sách store trong CSV bằng SerpApi.")
    parser.add_argument("--api-key", default=None, help="SerpApi key. Nếu bỏ trống, script sẽ đọc từ SERPAPI_API_KEY hoặc file .env.")
    parser.add_argument("--stores-csv", default="store_from_top5_json.csv", help="CSV đầu vào chứa danh sách store.")
    parser.add_argument("--output", default="be_google_maps_unique.json", help="File đầu ra JSON.")
    parser.add_argument("--output-csv", default="be_google_maps.csv", help="File đầu ra CSV rút gọn.")
    return parser.parse_args()



def main() -> int:
    args = parse_args()

    api_key = _resolve_api_key(args.api_key)
    if not api_key:
        print("Lỗi: thiếu SerpApi key. Hãy truyền --api-key hoặc đặt SERPAPI_API_KEY trong môi trường/.env.", file=sys.stderr)
        return 1

    csv_path = Path(args.stores_csv)
    if not csv_path.exists():
        print(f"Lỗi: không tìm thấy file CSV: {csv_path}", file=sys.stderr)
        return 1

    try:
        stores = load_stores_from_csv(csv_path)
    except Exception as exc:
        print(f"Lỗi đọc CSV: {exc}", file=sys.stderr)
        return 1

    results: list[Restaurant] = []
    failures: list[dict[str, str]] = []

    for index, store in enumerate(stores, start=1):
        print(f"[{index}/{len(stores)}] Crawling {store.store_id} - {store.name}", flush=True)
        try:
            results.append(_search_store(store, api_key))
            save_json(results, Path(args.output))
            save_csv(results, Path(args.output_csv))
            print(f"  -> OK ({len(results)} saved)", flush=True)
        except Exception as exc:
            failures.append({"id": store.store_id, "name": store.name, "error": str(exc)})
            print(f"  -> FAILED: {exc}", flush=True)

    output_json = Path(args.output)
    output_csv = Path(args.output_csv)
    save_json(results, output_json)
    save_csv(results, output_csv)

    if failures:
        fail_path = output_json.with_name(output_json.stem + "_failures.json")
        fail_path.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Có {len(failures)} store lỗi. Đã lưu: {fail_path.resolve()}")

    print(f"Hoàn tất. Crawl thành công: {len(results)}/{len(stores)}")
    print(f"JSON: {output_json.resolve()}")
    print(f"CSV : {output_csv.resolve()}")
    print("Đã bỏ hoàn toàn logic crawl đệ quy; chỉ query theo từng dòng trong CSV.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
