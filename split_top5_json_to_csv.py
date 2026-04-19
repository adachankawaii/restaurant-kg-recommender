import argparse
import csv
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple


def to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def extract_store_rows(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for user in data:
        user_id = to_str(user.get("user_id"))
        area_id = to_str(user.get("area_id"))
        time_slot_id = to_str(user.get("time_slot_id"))
        desired_price_range_id = to_str(user.get("desired_price_range_id"))
        preferred_aspects = to_str(user.get("preferred_aspects"))
        distance_tolerance_m = user.get("distance_tolerance_m")

        for top in user.get("top_restaurants", []) or []:
            detail_info = ((top.get("detail") or {}).get("data") or {}).get("restaurant_info") or {}

            row = {
                "user_id": user_id,
                "area_id": area_id,
                "time_slot_id": time_slot_id,
                "desired_price_range_id": desired_price_range_id,
                "preferred_aspects": preferred_aspects,
                "distance_tolerance_m": distance_tolerance_m,
                "store_id": to_str(top.get("restaurant_id") or detail_info.get("restaurant_id")),
                "store_name": to_str(top.get("restaurant_name_final") or top.get("restaurant_name") or detail_info.get("name")),
                "address": to_str(detail_info.get("address") or detail_info.get("display_address")),
                "latitude": top.get("restaurant_latitude", top.get("latitude", detail_info.get("latitude"))),
                "longitude": top.get("restaurant_longitude", top.get("longitude", detail_info.get("longitude"))),
                "distance_m": top.get("distance_m"),
                "distance_km": top.get("restaurant_distance_km"),
                "rank": top.get("rank"),
                "score": top.get("score"),
                "merchant_id": detail_info.get("merchant_id"),
                "merchant_category_name": to_str(detail_info.get("merchant_category_name")),
                "status": to_str(detail_info.get("status")),
                "is_closed": detail_info.get("is_closed"),
                "next_slot_time": to_str(detail_info.get("next_slot_time")),
                "end_time": to_str(detail_info.get("end_time")),
                "median_price": detail_info.get("median_price"),
            }
            rows.append(row)

    return rows


def extract_rating_rows(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for user in data:
        user_id = to_str(user.get("user_id"))

        for top in user.get("top_restaurants", []) or []:
            detail_data = (top.get("detail") or {}).get("data") or {}
            detail_info = detail_data.get("restaurant_info") or {}

            feedback_text = ""
            # Most datasets do not include free-text feedback in this JSON.
            for key in ("feedback", "comment", "review", "review_text"):
                if detail_data.get(key):
                    feedback_text = to_str(detail_data.get(key))
                    break
                if detail_info.get(key):
                    feedback_text = to_str(detail_info.get(key))
                    break

            row = {
                "user_id": user_id,
                "store_id": to_str(top.get("restaurant_id") or detail_info.get("restaurant_id")),
                "store_name": to_str(top.get("restaurant_name_final") or top.get("restaurant_name") or detail_info.get("name")),
                "rating": detail_info.get("rating"),
                "review_count": detail_info.get("review_count"),
                "feedback_status": detail_info.get("feedback_status"),
                "feedback": feedback_text,
            }
            rows.append(row)

    return rows


def extract_menu_item_rows(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for user in data:
        user_id = to_str(user.get("user_id"))

        for top in user.get("top_restaurants", []) or []:
            detail_data = (top.get("detail") or {}).get("data") or {}
            categories = detail_data.get("categories") or []

            top_store_id = to_str(top.get("restaurant_id"))
            top_store_name = to_str(top.get("restaurant_name_final") or top.get("restaurant_name"))

            for category in categories:
                category_id = category.get("category_id")
                category_name = to_str(category.get("category_name"))
                category_active = category.get("category_active")

                for item in category.get("items", []) or []:
                    row = {
                        "user_id": user_id,
                        "store_id": to_str(item.get("restaurant_id") or top_store_id),
                        "store_name": to_str(item.get("restaurant_name") or top_store_name),
                        "category_id": category_id,
                        "category_name": category_name,
                        "category_active": category_active,
                        "restaurant_item_id": item.get("restaurant_item_id"),
                        "item_name": to_str(item.get("item_name")),
                        "item_details": to_str(item.get("item_details")),
                        "price": item.get("price"),
                        "old_price": item.get("old_price"),
                        "display_price": to_str(item.get("display_price")),
                        "display_old_price": to_str(item.get("display_old_price")),
                        "order_count": item.get("order_count"),
                        "is_active": item.get("is_active"),
                        "offers_discount": item.get("offers_discount"),
                    }
                    rows.append(row)

    return rows


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


def deduplicate(rows: List[Dict[str, Any]], keys: Tuple[str, ...]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, ...]] = set()
    deduped: List[Dict[str, Any]] = []

    for row in rows:
        sig = tuple(to_str(row.get(k)) for k in keys)
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(row)

    return deduped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split top5_restaurants.json into store, rating, and menu_item CSV files."
    )
    parser.add_argument(
        "--input",
        default="top5_restaurants.json",
        help="Path to input JSON file (default: top5_restaurants.json)",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Output directory for CSV files (default: current directory)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    store_rows = extract_store_rows(data)
    rating_rows = extract_rating_rows(data)
    menu_item_rows = extract_menu_item_rows(data)

    store_rows = deduplicate(store_rows, ("user_id", "store_id"))
    rating_rows = deduplicate(rating_rows, ("user_id", "store_id"))
    menu_item_rows = deduplicate(menu_item_rows, ("user_id", "store_id", "restaurant_item_id"))

    store_fields = [
        "user_id",
        "area_id",
        "time_slot_id",
        "desired_price_range_id",
        "preferred_aspects",
        "distance_tolerance_m",
        "store_id",
        "store_name",
        "address",
        "latitude",
        "longitude",
        "distance_m",
        "distance_km",
        "rank",
        "score",
        "merchant_id",
        "merchant_category_name",
        "status",
        "is_closed",
        "next_slot_time",
        "end_time",
        "median_price",
    ]

    rating_fields = [
        "user_id",
        "store_id",
        "store_name",
        "rating",
        "review_count",
        "feedback_status",
        "feedback",
    ]

    menu_item_fields = [
        "user_id",
        "store_id",
        "store_name",
        "category_id",
        "category_name",
        "category_active",
        "restaurant_item_id",
        "item_name",
        "item_details",
        "price",
        "old_price",
        "display_price",
        "display_old_price",
        "order_count",
        "is_active",
        "offers_discount",
    ]

    store_path = out_dir / "store_from_top5_json.csv"
    rating_path = out_dir / "rating_from_top5_json.csv"
    menu_item_path = out_dir / "menu_item_from_top5_json.csv"

    write_csv(store_path, store_rows, store_fields)
    write_csv(rating_path, rating_rows, rating_fields)
    write_csv(menu_item_path, menu_item_rows, menu_item_fields)

    print(f"Created: {store_path} ({len(store_rows)} rows)")
    print(f"Created: {rating_path} ({len(rating_rows)} rows)")
    print(f"Created: {menu_item_path} ({len(menu_item_rows)} rows)")


if __name__ == "__main__":
    main()
