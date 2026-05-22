from __future__ import annotations

from functools import lru_cache
from typing import Optional

from config import load_config
from graph_store import Neo4jClient
from ingest import load_raw_data, prepare_data
from llm import create_intent_parser, generate_answer
from observability import JsonlTraceLogger
from retriever import GraphRAGRetriever
from vector_store import EmbeddingService, QdrantVectorStore


def create_app():
    try:
        from fastapi import FastAPI
        from pydantic import BaseModel
    except ImportError as exc:
        raise RuntimeError("Install fastapi and uvicorn to run api.py: pip install fastapi uvicorn") from exc

    app = FastAPI(title="Restaurant GraphRAG Recommender")
    config = load_config()

    @lru_cache(maxsize=1)
    def get_retriever() -> GraphRAGRetriever:
        raw_befood, raw_menu, raw_foody = load_raw_data(
            config.befood_restaurants_path,
            config.befood_menu_path,
            config.foody_path,
        )
        prepared = prepare_data(
            raw_befood,
            raw_menu,
            raw_foody,
            user_lat=config.user_lat,
            user_lng=config.user_lng,
            distance_decay_km=config.distance_decay_km,
        )
        neo4j = Neo4jClient.from_config(config)
        embeddings = EmbeddingService(config)
        vector_store = QdrantVectorStore(config, embeddings)
        trace_logger = JsonlTraceLogger(config.observability_log_path)
        intent_parser = create_intent_parser(config)
        return GraphRAGRetriever(
            config=config,
            neo4j_client=neo4j,
            vector_store=vector_store,
            summary=prepared.summary,
            intent_parser=intent_parser,
            trace_logger=trace_logger,
        )

    class RecommendRequest(BaseModel):
        query: str
        top_k: int = 5
        user_lat: Optional[float] = None
        user_lng: Optional[float] = None

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/recommend")
    def recommend(req: RecommendRequest):
        retriever = get_retriever()
        intent, ranked, trace = retriever.retrieve(
            req.query,
            top_k=req.top_k,
            user_lat=req.user_lat,
            user_lng=req.user_lng,
        )
        answer = generate_answer(config, req.query, intent, ranked)
        return {
            "query": req.query,
            "intent": intent,
            "answer": answer,
            "results": ranked,
            "trace_id": trace.request_id,
        }

    return app


app = create_app()
