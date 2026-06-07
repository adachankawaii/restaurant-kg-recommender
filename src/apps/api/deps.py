from __future__ import annotations

from functools import lru_cache

from services.graphrag_service.service import GraphRAGService
from services.graphrag_service.result_cache import GeoTestResultCache
from services.logging_service.event_logger import EventLogger
from services.query_parser.parser import QueryParser
from settings import load_settings


@lru_cache(maxsize=1)
def get_settings():
    return load_settings()


@lru_cache(maxsize=1)
def get_query_parser():
    return QueryParser()


@lru_cache(maxsize=1)
def get_graphrag_service():
    return GraphRAGService(get_settings())


@lru_cache(maxsize=1)
def get_geo_test_result_cache():
    return GeoTestResultCache.from_production_root(get_settings().root)


@lru_cache(maxsize=1)
def get_rgcn_service():
    from services.rgcn_service.service import RGCNService

    return RGCNService(get_settings())


@lru_cache(maxsize=1)
def get_event_logger():
    settings = get_settings()
    return EventLogger(settings.paths.user_events_root, settings.mode)
