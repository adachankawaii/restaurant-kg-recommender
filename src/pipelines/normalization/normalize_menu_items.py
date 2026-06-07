from __future__ import annotations

from pathlib import Path

import pandas as pd

from .normalize_restaurants import normalize_price, normalize_text


def normalize_menu_item_name(value: str) -> str:
    return normalize_text(value)


def build_canonical_menu_items(raw_menu: pd.DataFrame) -> pd.DataFrame:
    df = raw_menu.copy()
    restaurant_id_col = "restaurant_id" if "restaurant_id" in df.columns else "store_id"
    name_col = "item_name" if "item_name" in df.columns else ("name" if "name" in df.columns else df.columns[0])
    price_col = "price" if "price" in df.columns else ("item_price" if "item_price" in df.columns else None)
    category_col = "category" if "category" in df.columns else ("menu_category" if "menu_category" in df.columns else None)
    item_id_col = "restaurant_item_id" if "restaurant_item_id" in df.columns else None

    result = pd.DataFrame()
    result["item_id"] = df[item_id_col].astype(str) if item_id_col else df.index.map(lambda index: f"menu_item_{index}")
    result["restaurant_id"] = df[restaurant_id_col].astype(str)
    result["name"] = df[name_col].astype(str)
    result["normalized_name"] = result["name"].map(normalize_menu_item_name)
    result["description"] = None
    result["price"] = df[price_col].map(normalize_price) if price_col else None
    result["category"] = df[category_col].astype(str) if category_col else None
    result["availability"] = True
    result["source_names"] = "befood_menu"
    result["source_record_ids"] = result["item_id"]
    result["created_at"] = None
    result["updated_at"] = None
    result["data_version"] = "offline"
    return result


def write_canonical_menu_items(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
