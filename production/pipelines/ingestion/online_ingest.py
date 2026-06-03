from __future__ import annotations

from pathlib import Path

from common import dump_json, make_run_id, utc_now_iso
from pipelines.ingestion.common_ingest import process_restaurant_sources
from pipelines.storage.minio_client import MinioStorageAdapter
from settings import Settings


def _resolve_online_sources(settings: Settings, run_id: str) -> tuple[dict[str, Path], list[dict[str, object]]]:
    """Online ingestion expects crawler/API collectors to write CSVs with the same contracts."""
    collected_dir = settings.paths.raw_root / "online_collected" / run_id
    expected = {
        "befood_restaurants": collected_dir / "befood_bachkhoa_restaurants.csv",
        "befood_menu_items": collected_dir / "befood_bachkhoa_menu_items.csv",
        "foody_places": collected_dir / "foody_hust_places_from_store_csv.csv",
        "user_scenarios_v2": settings.root.parent / "rgcn_pipeline" / "data" / "user_scenarios_phase2_top5.csv",
    }
    crawler_registry = [
        {
            "script": str(settings.root.parent / "be_store_googlemaps_crawler.py"),
            "exists": (settings.root.parent / "be_store_googlemaps_crawler.py").exists(),
            "target_contract": "befood_restaurants",
        },
        {
            "script": str(settings.root.parent / "hust_foody.py"),
            "exists": (settings.root.parent / "hust_foody.py").exists(),
            "target_contract": "foody_places",
        },
    ]
    missing = [str(path) for key, path in expected.items() if key != "user_scenarios_v2" and not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Online collectors have not produced required CSV contracts yet:\n"
            + "\n".join(missing)
            + "\nRun crawler/API collectors into production/data_lake/raw/online_collected/{run_id}/ first."
        )
    return expected, crawler_registry


def run_online_ingest(settings: Settings) -> dict[str, object]:
    run_id = make_run_id("online")
    sources, crawler_registry = _resolve_online_sources(settings, run_id)
    manifest = process_restaurant_sources(settings, run_id=run_id, mode="online", sources=sources)
    manifest["online_collectors"] = crawler_registry

    minio_results = []
    if settings.use_minio:
        adapter = MinioStorageAdapter(bucket="restaurant-prod")
        for output_path in manifest.get("outputs", {}).values():
            minio_results.append(adapter.put_file(Path(output_path), f"{run_id}/{Path(output_path).name}"))
    manifest["minio_results"] = minio_results
    manifest["updated_at"] = utc_now_iso()
    dump_json(settings.paths.processed_root / run_id / "manifest.json", manifest)
    return manifest
