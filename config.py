from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: Optional[float] = None) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class AppConfig:
    repo_root: Path
    data_root: Path
    befood_restaurants_path: Path
    befood_menu_path: Path
    foody_path: Path

    neo4j_uri: str
    neo4j_user: str
    neo4j_password: str

    qdrant_host: str
    qdrant_port: int
    coll_text_unit: str
    coll_restaurant: str
    recreate_qdrant: bool

    embed_model: str
    embed_prefix_query: str
    embed_prefix_passage: str
    aspect_sentiment_model: str
    cross_encoder_model: str
    use_cross_encoder: bool
    cross_encoder_weight: float

    llm_provider: str
    openai_api_key: str
    openai_model: str
    openai_base_url: Optional[str]
    anthropic_api_key: str
    anthropic_model: str

    user_lat: Optional[float]
    user_lng: Optional[float]
    max_distance_km: Optional[float]
    distance_weight: float
    distance_decay_km: float

    run_community_reports: bool
    cache_dir: Path
    observability_log_path: Path


def load_config(repo_root: Optional[Path] = None) -> AppConfig:
    repo_root = repo_root or Path.cwd()
    load_dotenv(dotenv_path=repo_root / ".env")

    data_root = Path(os.getenv("DATA_ROOT", str(repo_root / "Utils")))
    cache_dir = Path(os.getenv("CACHE_DIR", str(repo_root / ".cache" / "graphrag")))

    return AppConfig(
        repo_root=repo_root,
        data_root=data_root,
        befood_restaurants_path=Path(os.getenv("BEFOOD_RESTAURANTS_PATH", str(data_root / "befood_bachkhoa_restaurants.csv"))),
        befood_menu_path=Path(os.getenv("BEFOOD_MENU_PATH", str(data_root / "befood_bachkhoa_menu_items.csv"))),
        foody_path=Path(os.getenv("FOODY_PATH", str(data_root / "foody_hust_places_from_store_csv.csv"))),
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j")),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "password123"),
        qdrant_host=os.getenv("QDRANT_HOST", "localhost"),
        qdrant_port=int(os.getenv("QDRANT_PORT", "6333")),
        coll_text_unit=os.getenv("COLL_TEXT_UNIT", "graphrag_text_units_vietnamese"),
        coll_restaurant=os.getenv("COLL_RESTAURANT", "graphrag_restaurants_vietnamese"),
        recreate_qdrant=_env_bool("RECREATE_QDRANT", True),
        embed_model=os.getenv("EMBED_MODEL", "bkai-foundation-models/vietnamese-bi-encoder"),
        embed_prefix_query=os.getenv("EMBED_PREFIX_QUERY", ""),
        embed_prefix_passage=os.getenv("EMBED_PREFIX_PASSAGE", ""),
        aspect_sentiment_model=os.getenv("ASPECT_SENTIMENT_MODEL", "wonrax/phobert-base-vietnamese-sentiment"),
        cross_encoder_model=os.getenv("CROSS_ENCODER_MODEL", "BAAI/bge-reranker-base"),
        use_cross_encoder=_env_bool("USE_CROSS_ENCODER", True),
        cross_encoder_weight=float(os.getenv("CROSS_ENCODER_WEIGHT", "0.30")),
        llm_provider=os.getenv("LLM_PROVIDER", "openai").lower(),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        openai_base_url=os.getenv("OPENAI_BASE_URL") or None,
        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY", ""),
        anthropic_model=os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514"),
        user_lat=_env_float("USER_LAT"),
        user_lng=_env_float("USER_LNG"),
        max_distance_km=_env_float("MAX_DISTANCE_KM"),
        distance_weight=float(os.getenv("DISTANCE_WEIGHT", "0.20")),
        distance_decay_km=float(os.getenv("DISTANCE_DECAY_KM", "3.0")),
        run_community_reports=_env_bool("RUN_COMMUNITY_REPORTS", True),
        cache_dir=cache_dir,
        observability_log_path=Path(os.getenv("OBSERVABILITY_LOG_PATH", str(repo_root / "logs" / "retrieval_traces.jsonl"))),
    )
