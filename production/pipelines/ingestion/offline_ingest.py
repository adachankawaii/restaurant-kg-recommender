from __future__ import annotations

from common import make_run_id
from pipelines.ingestion.common_ingest import process_restaurant_sources
from pipelines.ingestion.source_registry import SourceRegistry
from settings import Settings


def run_offline_ingest(settings: Settings) -> dict[str, object]:
    registry = SourceRegistry(settings.root, settings.config)
    registry.validate_sources_exist()
    return process_restaurant_sources(
        settings,
        run_id=make_run_id("offline"),
        mode="offline",
        sources=registry.resolve_sources(),
    )
