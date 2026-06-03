from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import make_run_id
from pipelines.ingestion.common_ingest import process_restaurant_sources
from pipelines.storage.minio_client import MinioStorageAdapter
from settings import load_settings


def main() -> None:
    settings = load_settings(mode="online")
    run_id = make_run_id("online")
    collected_dir = settings.paths.raw_root / "online_collected" / run_id
    collected_dir.mkdir(parents=True, exist_ok=True)

    repo_root = settings.root.parent
    shutil.copy2(repo_root / "Utils" / "befood_bachkhoa_restaurants.csv", collected_dir / "befood_bachkhoa_restaurants.csv")
    shutil.copy2(repo_root / "Utils" / "befood_bachkhoa_menu_items.csv", collected_dir / "befood_bachkhoa_menu_items.csv")

    if os.getenv("RUN_FOODY_CRAWLER", "false").lower() in {"1", "true", "yes"}:
        subprocess.run([sys.executable, str(repo_root / "hust_foody.py")], cwd=str(repo_root), check=True, timeout=7200)
        crawler_output = repo_root / "foody_hust_output" / "foody_hust_places_only_v2.csv"
        shutil.copy2(crawler_output, collected_dir / "foody_hust_places_from_store_csv.csv")
    else:
        shutil.copy2(repo_root / "Utils" / "foody_hust_places_from_store_csv.csv", collected_dir / "foody_hust_places_from_store_csv.csv")

    sources = {
        "befood_restaurants": collected_dir / "befood_bachkhoa_restaurants.csv",
        "befood_menu_items": collected_dir / "befood_bachkhoa_menu_items.csv",
        "foody_places": collected_dir / "foody_hust_places_from_store_csv.csv",
        "user_scenarios_v2": repo_root / "rgcn_pipeline" / "data" / "user_scenarios_phase2_top5.csv",
    }
    manifest = process_restaurant_sources(settings, run_id=run_id, mode="online", sources=sources)

    adapter = MinioStorageAdapter()
    for source_name, source_path in sources.items():
        if source_path.exists():
            adapter.put_file(source_path, f"raw/online_collected/{run_id}/{source_name}/{source_path.name}")
    for output_path in manifest.get("outputs", {}).values():
        adapter.put_file(Path(output_path), f"processed/{run_id}/{Path(output_path).name}")
    print(manifest)


if __name__ == "__main__":
    main()
