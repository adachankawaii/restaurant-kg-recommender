#!/usr/bin/env python3
"""Append Google Maps review comments from a JSON export into a feedback CSV.

Default behavior:
- read `be_google_maps_unique.json`
- append review comments into `store_feedback_crawled.csv`

The script keeps existing rows, skips empty review text, and avoids duplicates.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


DEFAULT_INPUT_JSON = "be_google_maps_unique.json"
DEFAULT_OUTPUT_CSV = "store_feedback_crawled.csv"


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def load_json_records(json_path: Path) -> list[dict[str, Any]]:
    data = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list of restaurant records")
    return data


def build_comment_rows(records: list[dict[str, Any]], source_name: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    for record in records:
        store_id = safe_str(record.get("id") or record.get("store_id"))
        store_name = safe_str(record.get("query_name") or record.get("name") or record.get("store_name"))
        for review in record.get("reviews") or []:
            if not isinstance(review, dict):
                continue

            feedback = safe_str(review.get("description") or review.get("content") or review.get("text"))
            if not feedback:
                continue

            rows.append(
                {
                    "store_id": store_id,
                    "store_name": store_name,
                    "rated_at": "",
                    "rating": safe_str(review.get("rating")),
                    "feedback": feedback,
                    "crawl_status": "ok",
                    "error": "",
                    "source": source_name,
                }
            )

    return rows


def load_existing_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        return []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def row_signature(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        safe_str(row.get("store_id")),
        safe_str(row.get("store_name")),
        safe_str(row.get("rated_at")),
        safe_str(row.get("rating")),
        safe_str(row.get("feedback")),
        safe_str(row.get("crawl_status")),
    )


def merge_rows(existing_rows: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = {row_signature(row) for row in existing_rows}
    merged = list(existing_rows)

    for row in new_rows:
        signature = row_signature(row)
        if signature in seen:
            continue
        seen.add(signature)
        merged.append(row)

    return merged


def write_rows(csv_path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = ["store_id", "store_name", "rated_at", "rating", "feedback", "crawl_status", "error", "source"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append Google Maps comments from a JSON export into the feedback CSV."
    )
    parser.add_argument(
        "--input-json",
        default=DEFAULT_INPUT_JSON,
        help=f"Input JSON file (default: {DEFAULT_INPUT_JSON})",
    )
    parser.add_argument(
        "--output-csv",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV file (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--source-name",
        default="google_maps",
        help="Source label written into the `source` column.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    json_path = Path(args.input_json)
    csv_path = Path(args.output_csv)

    if not json_path.exists():
        print(f"Lỗi: không tìm thấy file JSON: {json_path}")
        return 1

    records = load_json_records(json_path)
    new_rows = build_comment_rows(records, args.source_name)
    existing_rows = load_existing_rows(csv_path)
    merged_rows = merge_rows(existing_rows, new_rows)

    write_rows(csv_path, merged_rows)

    print(f"JSON records: {len(records)}")
    print(f"Google Maps comments found: {len(new_rows)}")
    print(f"Existing CSV rows: {len(existing_rows)}")
    print(f"Total CSV rows after merge: {len(merged_rows)}")
    print(f"Updated: {csv_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())