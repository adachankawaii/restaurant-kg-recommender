from __future__ import annotations

from functools import lru_cache

from services.graphrag_service.service import GraphRAGService
from services.logging_service.event_logger import EventLogger
from services.query_parser.parser import QueryParser
from services.rgcn_service.service import RGCNService
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
def get_rgcn_service():
    return RGCNService(get_settings())


@lru_cache(maxsize=1)
def get_event_logger():
    settings = get_settings()
    return EventLogger(settings.paths.user_events_root, settings.mode)
