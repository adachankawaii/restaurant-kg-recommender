from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[3]))

from common import dump_json, utc_now_iso
from ingest import prepare_data
from pipelines.normalization.deduplicate import deduplicate_restaurants
from pipelines.normalization.offline_graph_artifacts import (
    apply_cached_dish_families,
    build_text_units_with_chunking,
    community_reports_to_df,
    enrich_feedback_with_cached_aspects,
    extract_entities_from_checkpoint,
)
from pipelines.normalization.normalize_menu_items import build_canonical_menu_items, write_canonical_menu_items
from pipelines.normalization.normalize_restaurants import build_canonical_restaurants, write_canonical_restaurants
from pipelines.storage.lake import copy_to_lake
from pipelines.validation.validate_csv import validate_csv_against_schema
from settings import Settings


SCHEMA_MAP = {
    "befood_restaurants": "befood_restaurants.schema.yaml",
    "befood_menu_items": "befood_menu_items.schema.yaml",
    "foody_places": "foody_places.schema.yaml",
}


def process_restaurant_sources(
    settings: Settings,
    *,
    run_id: str,
    mode: str,
    sources: dict[str, Path],
) -> dict[str, object]:
    raw_dir = settings.paths.raw_root / mode / run_id
    processed_dir = settings.paths.processed_root / run_id

    raw_copies = {name: str(copy_to_lake(path, raw_dir)) for name, path in sources.items()}

    validations = {}
    for source_name, schema_file in SCHEMA_MAP.items():
        validations[source_name] = validate_csv_against_schema(
            Path(raw_copies[source_name]),
            settings.root / "data_contracts" / schema_file,
        )
        if not validations[source_name]["valid"]:
            raise ValueError(f"Validation failed for {source_name}: {validations[source_name]}")

    raw_befood = pd.read_csv(sources["befood_restaurants"])
    raw_menu = pd.read_csv(sources["befood_menu_items"])
    raw_foody = pd.read_csv(sources["foody_places"])
    prepared = prepare_data(raw_befood, raw_menu, raw_foody)

    canonical_restaurants = build_canonical_restaurants(raw_befood, raw_menu, raw_foody)
    canonical_restaurants, aliases = deduplicate_restaurants(canonical_restaurants)
    canonical_menu_items = build_canonical_menu_items(raw_menu)

    cache_root = settings.root.parent / "cache" / ".cache" / "graphrag"
    if mode == "offline":
        feedback = enrich_feedback_with_cached_aspects(prepared.feedback, cache_root)
        menu_with_cached_family = apply_cached_dish_families(prepared.menu_items, cache_root)
    else:
        feedback = prepared.feedback.copy()
        feedback["aspect_scores"] = [{} for _ in range(len(feedback))]
        feedback["sentiment"] = "neutral"
        feedback["confidence"] = 0.0
        menu_with_cached_family = prepared.menu_items.copy()
        menu_with_cached_family["dish_family"] = None
    name_map = {str(row["restaurant_id"]): row["name"] for _, row in canonical_restaurants.iterrows()}
    text_units = build_text_units_with_chunking(feedback, name_map)
    if mode == "offline":
        extracted_entities, extracted_relations = extract_entities_from_checkpoint(text_units, cache_root)
        community_reports = community_reports_to_df(cache_root)
    else:
        extracted_entities, extracted_relations = pd.DataFrame(), pd.DataFrame()
        community_reports = pd.DataFrame()

    canonical_restaurants_path = processed_dir / "canonical_restaurants.csv"
    canonical_menu_path = processed_dir / "canonical_menu_items.csv"
    aliases_path = processed_dir / "restaurant_aliases.csv"
    feedback_path = processed_dir / "feedback.csv"
    text_units_path = processed_dir / "text_units.csv"
    menu_cached_family_path = processed_dir / "menu_items_enriched.csv"
    extracted_entities_path = processed_dir / "extracted_entities.csv"
    extracted_relations_path = processed_dir / "extracted_relations.csv"
    community_reports_path = processed_dir / "community_reports.csv"
    scenario_features_path = processed_dir / "scenario_features.csv"

    write_canonical_restaurants(canonical_restaurants, canonical_restaurants_path)
    write_canonical_menu_items(canonical_menu_items, canonical_menu_path)
    aliases_path.parent.mkdir(parents=True, exist_ok=True)
    aliases.to_csv(aliases_path, index=False, encoding="utf-8-sig")
    feedback.to_csv(feedback_path, index=False, encoding="utf-8-sig")
    text_units.to_csv(text_units_path, index=False, encoding="utf-8-sig")
    menu_with_cached_family.to_csv(menu_cached_family_path, index=False, encoding="utf-8-sig")
    extracted_entities.to_csv(extracted_entities_path, index=False, encoding="utf-8-sig")
    extracted_relations.to_csv(extracted_relations_path, index=False, encoding="utf-8-sig")
    community_reports.to_csv(community_reports_path, index=False, encoding="utf-8-sig")

    if "user_scenarios_v2" in sources:
        pd.read_csv(sources["user_scenarios_v2"]).to_csv(scenario_features_path, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(scenario_features_path, index=False, encoding="utf-8-sig")

    manifest = {
        "run_id": run_id,
        "mode": mode,
        "sources": {key: str(value) for key, value in sources.items()},
        "outputs": {
            "canonical_restaurants": str(canonical_restaurants_path),
            "canonical_menu_items": str(canonical_menu_path),
            "restaurant_aliases": str(aliases_path),
            "feedback": str(feedback_path),
            "text_units": str(text_units_path),
            "menu_items_enriched": str(menu_cached_family_path),
            "extracted_entities": str(extracted_entities_path),
            "extracted_relations": str(extracted_relations_path),
            "community_reports": str(community_reports_path),
            "scenario_features": str(scenario_features_path),
        },
        "row_counts": {
            "canonical_restaurants": int(len(canonical_restaurants)),
            "canonical_menu_items": int(len(canonical_menu_items)),
            "restaurant_aliases": int(len(aliases)),
            "feedback": int(len(feedback)),
            "text_units": int(len(text_units)),
            "menu_items_enriched": int(len(menu_with_cached_family)),
            "extracted_entities": int(len(extracted_entities)),
            "extracted_relations": int(len(extracted_relations)),
            "community_reports": int(len(community_reports)),
        },
        "created_at": utc_now_iso(),
    }
    dump_json(processed_dir / "manifest.json", manifest)
    return manifest
