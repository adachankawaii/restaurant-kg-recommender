from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException

from common import latest_complete_dir
from apps.api.deps import get_event_logger, get_settings

router = APIRouter()


@lru_cache(maxsize=64)
def _read_csv_cached(path_text: str, mtime_ns: int) -> pd.DataFrame:
    del mtime_ns
    path = Path(path_text)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return _read_csv_cached(str(path), path.stat().st_mtime_ns)


def _parse_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        return [item.strip() for item in text.split("|") if item.strip()]
    if isinstance(parsed, list):
        return [str(item) for item in parsed if str(item).strip()]
    return [str(parsed)]


def _scalar(value: Any):
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def _row_dict(frame: pd.DataFrame, column: str, value: str) -> dict:
    if frame.empty or column not in frame.columns:
        return {}
    matched = frame[frame[column].astype(str) == value]
    if matched.empty:
        return {}
    return {str(key): _scalar(item) for key, item in matched.iloc[0].to_dict().items()}


def _records(frame: pd.DataFrame) -> list[dict]:
    return [
        {str(key): _scalar(value) for key, value in row.items()}
        for row in frame.to_dict("records")
    ]


def _latest_raw_dir(settings) -> Path | None:
    raw_mode_root = settings.paths.raw_root / settings.mode
    required = [
        "befood_bachkhoa_restaurants.csv",
        "befood_bachkhoa_menu_items.csv",
    ]
    try:
        return latest_complete_dir(raw_mode_root, required, "raw restaurant source data")
    except FileNotFoundError:
        return None


def _menu_payload(row: dict) -> dict:
    return {
        "menu_item_id": str(row.get("menu_item_id", row.get("restaurant_item_id", ""))),
        "name": str(row.get("item_name", "")),
        "details": str(row.get("item_details", "")),
        "price": _scalar(row.get("price")),
        "old_price": _scalar(row.get("old_price")),
        "order_count": _scalar(row.get("order_count")),
        "like_count": _scalar(row.get("like_count")),
        "dislike_count": _scalar(row.get("dislike_count")),
        "dish_family": str(row.get("dish_family", "")),
        "category": str(row.get("category_name", "")),
        "category_id": str(row.get("category_id", "")),
        "category_position": _scalar(row.get("category_position")),
        "item_position": _scalar(row.get("item_position")),
        "image_url": str(row.get("item_image", "")),
    }


def _befood_payload(row: dict) -> dict:
    if not row:
        return {}
    payload = dict(row)
    payload["matched_terms"] = _parse_list(str(row.get("matched_terms_text", "")).replace(" | ", "|"))
    payload["categories"] = _parse_list(str(row.get("categories_text", "")).replace(" | ", "|"))
    payload["comments"] = _parse_list(row.get("comments_list", ""))
    return payload


@router.get("/restaurants/{restaurant_id}")
def get_restaurant(
    restaurant_id: str,
    session_id: str | None = None,
    rank_position: int | None = None,
    debug: bool = False,
):
    settings = get_settings()
    try:
        processed_dir = latest_complete_dir(
            settings.paths.processed_root,
            ["canonical_restaurants.csv"],
            "processed restaurant data",
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Restaurant data is not available") from exc
    frame = pd.read_csv(Path(processed_dir) / "canonical_restaurants.csv").fillna("")
    matched = frame[frame["restaurant_id"].astype(str) == restaurant_id]
    if matched.empty:
        raise HTTPException(status_code=404, detail="Restaurant not found")
    row = matched.iloc[0].to_dict()
    menu_items = _read_optional_csv(Path(processed_dir) / "menu_items_enriched.csv")
    text_units = _read_optional_csv(Path(processed_dir) / "text_units.csv")
    entities = _read_optional_csv(Path(processed_dir) / "extracted_entities.csv")
    raw_dir = _latest_raw_dir(settings)
    raw_restaurants = _read_optional_csv(raw_dir / "befood_bachkhoa_restaurants.csv") if raw_dir else pd.DataFrame()
    raw_menu = _read_optional_csv(raw_dir / "befood_bachkhoa_menu_items.csv") if raw_dir else pd.DataFrame()
    raw_foody = _read_optional_csv(raw_dir / "foody_hust_places_from_store_csv.csv") if raw_dir else pd.DataFrame()
    raw_store_meta = _read_optional_csv(raw_dir / "store_metadata.csv") if raw_dir else pd.DataFrame()

    menu_rows = menu_items[menu_items["store_key"].astype(str) == restaurant_id] if "store_key" in menu_items.columns else pd.DataFrame()
    raw_menu_rows = raw_menu[raw_menu["restaurant_id"].astype(str) == restaurant_id] if "restaurant_id" in raw_menu.columns else pd.DataFrame()
    text_rows = text_units[text_units["store_key"].astype(str) == restaurant_id] if "store_key" in text_units.columns else pd.DataFrame()
    entity_rows = entities[entities["store_key"].astype(str) == restaurant_id] if "store_key" in entities.columns else pd.DataFrame()
    befood_row = _row_dict(raw_restaurants, "restaurant_id", restaurant_id)
    foody_row = _row_dict(raw_foody, "input_store_id", restaurant_id) or _row_dict(raw_foody, "restaurant_id", restaurant_id)
    store_meta_row = _row_dict(raw_store_meta, "store_id", restaurant_id)
    menu_source_rows = menu_rows if not menu_rows.empty else raw_menu_rows

    detail = {
        **row,
        "categories": _parse_list(row.get("categories", "")),
        "comments": _parse_list(befood_row.get("comments_list", "")),
        "menu_items": [
            _menu_payload(menu_row.to_dict())
            for _, menu_row in menu_source_rows.iterrows()
        ],
        "review_evidence": [
            {
                "text_unit_id": str(text_row.get("text_unit_id", "")),
                "rating": text_row.get("rating"),
                "sentiment": str(text_row.get("sentiment", "")),
                "feedback": str(text_row.get("feedback", "")),
                "chunk_text": str(text_row.get("chunk_text", "")),
            }
            for _, text_row in text_rows.iterrows()
        ],
        "extracted_entities": [
            {
                "name": str(entity_row.get("name", "")),
                "type": str(entity_row.get("entity_type", "")),
                "sentiment": str(entity_row.get("sentiment", "")),
                "confidence": entity_row.get("confidence"),
                "evidence": str(entity_row.get("evidence", "")),
            }
            for _, entity_row in entity_rows.iterrows()
        ],
    }
    if debug:
        detail.update({
            "source_details": {
                "befood": _befood_payload(befood_row),
                "foody": foody_row,
                "store_metadata": store_meta_row,
            },
            "data_lineage": {
                "raw_run": raw_dir.name if raw_dir else "",
                "processed_run": processed_dir.name,
            },
            "raw_menu_items": _records(raw_menu_rows),
            "available_source_counts": {
            "menu_items": int(len(menu_source_rows)),
            "raw_menu_items": int(len(raw_menu_rows)),
            "comments": int(len(_parse_list(befood_row.get("comments_list", "")))),
            "review_evidence": int(len(text_rows)),
            "extracted_entities": int(len(entity_rows)),
            },
        })
    if session_id:
        logger = get_event_logger()
        logger.log_event(
            {
                "session_id": session_id,
                "event_type": "restaurant_clicked",
                "clicked_restaurant_id": restaurant_id,
                "rank_position": rank_position,
            }
        )
        logger.label_clicked_scenario(
            session_id=session_id,
            restaurant_id=restaurant_id,
            rank_position=rank_position,
        )
    return detail
