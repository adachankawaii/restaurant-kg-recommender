from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))

from ingest import prepare_data


def normalize_text(value: str) -> str:
    value = "" if value is None else str(value).strip().lower()
    value = unicodedata.normalize("NFKC", value)
    return re.sub(r"\s+", " ", value)


def normalize_address(value: str) -> str:
    return normalize_text(value)


def normalize_restaurant_name(value: str) -> str:
    return normalize_text(value)


def normalize_price(value) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def build_canonical_restaurants(raw_befood: pd.DataFrame, raw_menu: pd.DataFrame, raw_foody: pd.DataFrame) -> pd.DataFrame:
    prepared = prepare_data(raw_befood, raw_menu, raw_foody)
    summary = prepared.summary.copy()
    summary["restaurant_id"] = summary["store_key"].astype(str)
    summary["name"] = summary["name"].astype(str)
    summary["normalized_name"] = summary["name"].map(normalize_restaurant_name)
    summary["address"] = summary["address"].fillna("").astype(str)
    summary["normalized_address"] = summary["address"].map(normalize_address)
    summary["latitude"] = summary["lat"]
    summary["longitude"] = summary["lng"]
    summary["phone"] = None
    summary["cuisine_type"] = summary["categories"].apply(lambda value: value[0] if isinstance(value, list) and value else None)
    summary["source_names"] = "befood|foody"
    summary["source_record_ids"] = summary["store_key"].astype(str)
    summary["created_at"] = None
    summary["updated_at"] = None
    summary["data_version"] = "offline"
    return summary[
        [
            "restaurant_id",
            "name",
            "normalized_name",
            "address",
            "normalized_address",
            "latitude",
            "longitude",
            "phone",
            "opening_hours",
            "cuisine_type",
            "price_min",
            "price_max",
            "rating",
            "source_names",
            "source_record_ids",
            "created_at",
            "updated_at",
            "data_version",
            "review_count",
            "distance_km",
            "categories",
        ]
    ].drop_duplicates(subset=["restaurant_id"])


def write_canonical_restaurants(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
