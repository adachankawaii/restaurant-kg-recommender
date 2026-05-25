from __future__ import annotations

import json
from typing import Optional, Protocol

import pandas as pd
from pydantic import BaseModel, Field

from config import AppConfig
from cross_encoder import CrossEncoderReranker
from ingest import normalize_dish_family, to_float
from observability import JsonlTraceLogger, RetrievalTrace
from ranker import add_user_distance_to_record, rerank_candidates


class Neo4jLike(Protocol):
    def run(self, query: str, params: Optional[dict] = None) -> list[dict]: ...


class VectorStoreLike(Protocol):
    def search_restaurants(self, query: str, top_k: int, user_lat: Optional[float], user_lng: Optional[float]) -> list[dict]: ...
    def search_text_units(self, query: str, top_k: int, store_keys: Optional[list[str]] = None) -> list[dict]: ...


class RestaurantIntent(BaseModel):
    query_type: str = "search"
    district: Optional[str] = None
    cuisines: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    dish_name: Optional[str] = None
    min_rating: Optional[float] = Field(default=None, ge=0, le=5)
    max_distance_km: Optional[float] = Field(default=None, ge=0)
    price_band: Optional[str] = None
    geo_intent: str = "normal"
    required_attributes: list[str] = Field(default_factory=list)
    sentiment_pref: Optional[str] = None
    top_k: int = Field(default=5, ge=1, le=20)


def build_graph_candidate_query(intent: dict, has_user_location: bool, has_max_distance: bool) -> tuple[str, dict]:
    match_lines = ["MATCH (r:Restaurant)"]
    where = []
    params: dict = {}

    if intent.get("district"):
        match_lines.append("MATCH (r)-[:IN_AREA]->(area:Area)")
        where.append("toLower(area.name) CONTAINS toLower($district)")
        params["district"] = intent["district"]
    if intent.get("price_band"):
        match_lines.append("MATCH (r)-[:HAS_PRICE_BAND]->(pb:PriceBand)")
        where.append("pb.name = $price_band")
        params["price_band"] = intent["price_band"]
    if intent.get("cuisines"):
        match_lines.append("MATCH (r)-[:HAS_CUISINE]->(cui:Cuisine)")
        where.append("cui.name IN $cuisines")
        params["cuisines"] = intent["cuisines"]
    if intent.get("categories"):
        match_lines.append("MATCH (r)-[:HAS_CATEGORY]->(cat:Category)")
        where.append("cat.name IN $categories")
        params["categories"] = intent["categories"]
    if intent.get("dish_name"):
        match_lines.append("MATCH (r)-[:SERVES_FAMILY]->(dish:DishFamily)")
        where.append("toLower(dish.name) CONTAINS toLower($dish_name)")
        params["dish_name"] = normalize_dish_family(intent["dish_name"]) or intent["dish_name"]
    if intent.get("min_rating") is not None:
        where.append("coalesce(r.rating, r.gmaps_rating, r.foody_rating, 0) >= $min_rating")
        params["min_rating"] = float(intent["min_rating"])
    if has_user_location and has_max_distance:
        where.append("r.lat IS NOT NULL AND r.lng IS NOT NULL AND point.distance(point({latitude: $user_lat, longitude: $user_lng}), point({latitude: r.lat, longitude: r.lng})) / 1000.0 <= $max_distance_km")
    for i, attr in enumerate(intent.get("required_attributes", [])):
        alias = f"att{i}"
        match_lines.append(f"MATCH (r)-[:HAS_ATTRIBUTE]->({alias}:Attribute {{type: $attr_{i}}})")
        where.append(f"{alias}.score >= 0.15")
        params[f"attr_{i}"] = attr

    distance_expr = "point.distance(point({latitude: $user_lat, longitude: $user_lng}), point({latitude: r.lat, longitude: r.lng})) / 1000.0" if has_user_location else "null"
    query = "\n".join(match_lines) + "\n"
    if where:
        query += "WHERE " + " AND ".join(where) + "\n"
    query += f"""
    OPTIONAL MATCH (r)-[:HAS_ATTRIBUTE]->(att:Attribute)
    OPTIONAL MATCH (r)-[:IN_AREA]->(a:Area)
    OPTIONAL MATCH (r)-[:HAS_CATEGORY]->(cat_ret:Category)
    OPTIONAL MATCH (r)-[:HAS_CUISINE]->(cui_ret:Cuisine)
    OPTIONAL MATCH (r)-[:SERVES_FAMILY]->(df_ret:DishFamily)
    OPTIONAL MATCH (r)-[:IN_COMMUNITY]->(com:Community)-[:HAS_REPORT]->(rep:CommunityReport)
    WITH r, a, rep, collect(DISTINCT {{type: att.type, score: att.score}}) AS attributes,
         collect(DISTINCT cat_ret.name) AS categories,
         collect(DISTINCT cui_ret.name) AS cuisines,
         collect(DISTINCT df_ret.name) AS dish_families,
         CASE WHEN r.lat IS NULL OR r.lng IS NULL THEN null ELSE {distance_expr} END AS distance_km
    RETURN r.store_key AS store_key, r.name AS name, r.address AS address,
           a.name AS district, a.city AS city, coalesce(r.rating, r.gmaps_rating, r.foody_rating) AS rating,
           r.lat AS lat, r.lng AS lng, distance_km,
           r.price_band AS price_band, r.top_menu_items AS top_menu_items,
           r.menu_price_min AS menu_price_min, r.menu_price_max AS menu_price_max,
           r.menu_price_median AS menu_price_median,
           categories, cuisines, dish_families, attributes, rep.summary AS community_report
    ORDER BY CASE WHEN distance_km IS NULL THEN 1 ELSE 0 END, distance_km ASC,
             CASE WHEN coalesce(r.rating, r.gmaps_rating, r.foody_rating) IS NULL THEN 1 ELSE 0 END,
             coalesce(r.rating, r.gmaps_rating, r.foody_rating) DESC
    LIMIT $top_k
    """
    return query, params


class GraphRAGRetriever:
    def __init__(
        self,
        config: AppConfig,
        neo4j_client: Neo4jLike,
        vector_store: VectorStoreLike,
        summary: pd.DataFrame,
        intent_parser,
        trace_logger: Optional[JsonlTraceLogger] = None,
        cross_encoder_reranker: Optional[CrossEncoderReranker] = None,
    ):
        self.config = config
        self.neo4j = neo4j_client
        self.vector_store = vector_store
        self.summary_by_key = summary.set_index("store_key").to_dict("index") if not summary.empty else {}
        self.intent_parser = intent_parser
        self.trace_logger = trace_logger
        self.cross_encoder_reranker = cross_encoder_reranker

    def _location(self, user_lat: Optional[float], user_lng: Optional[float]) -> tuple[Optional[float], Optional[float]]:
        lat = to_float(user_lat) if user_lat is not None else self.config.user_lat
        lng = to_float(user_lng) if user_lng is not None else self.config.user_lng
        return lat, lng

    def graph_candidate_search(self, intent: dict, top_k: int, user_lat: Optional[float], user_lng: Optional[float]) -> list[dict]:
        lat, lng = self._location(user_lat, user_lng)
        max_distance = intent.get("max_distance_km") or self.config.max_distance_km
        has_location = lat is not None and lng is not None
        query, params = build_graph_candidate_query(intent, has_location, max_distance is not None)
        params.update({"top_k": int(top_k), "user_lat": lat, "user_lng": lng, "max_distance_km": max_distance})
        rows = self.neo4j.run(query, params)
        return [add_user_distance_to_record(r, lat, lng, self.config.distance_decay_km) for r in rows]

    def subgraph_expand_candidates(self, seed_store_keys: list[str], max_neighbors: int, user_lat: Optional[float], user_lng: Optional[float]) -> list[dict]:
        if not seed_store_keys:
            return []
        lat, lng = self._location(user_lat, user_lng)
        has_location = lat is not None and lng is not None
        distance_expr = "point.distance(point({latitude: $user_lat, longitude: $user_lng}), point({latitude: nbr.lat, longitude: nbr.lng})) / 1000.0" if has_location else "null"
        query = f"""
        UNWIND $keys AS key
        MATCH (r:Restaurant {{store_key: key}})-[s:SIMILAR_TO]-(nbr:Restaurant)
        OPTIONAL MATCH (nbr)-[:HAS_ATTRIBUTE]->(att:Attribute)
        OPTIONAL MATCH (nbr)-[:IN_AREA]->(a:Area)
        OPTIONAL MATCH (nbr)-[:HAS_CATEGORY]->(cat_ret:Category)
        OPTIONAL MATCH (nbr)-[:HAS_CUISINE]->(cui_ret:Cuisine)
        OPTIONAL MATCH (nbr)-[:SERVES_FAMILY]->(df_ret:DishFamily)
        OPTIONAL MATCH (nbr)-[:IN_COMMUNITY]->(com:Community)-[:HAS_REPORT]->(rep:CommunityReport)
        WITH nbr, max(s.similarity) AS sim, a, rep, collect(DISTINCT {{type: att.type, score: att.score}}) AS attributes,
             collect(DISTINCT cat_ret.name) AS categories,
             collect(DISTINCT cui_ret.name) AS cuisines,
             collect(DISTINCT df_ret.name) AS dish_families,
             CASE WHEN nbr.lat IS NULL OR nbr.lng IS NULL THEN null ELSE {distance_expr} END AS distance_km
        RETURN nbr.store_key AS store_key, nbr.name AS name, nbr.address AS address,
               a.name AS district, a.city AS city, coalesce(nbr.rating, nbr.gmaps_rating, nbr.foody_rating) AS rating,
               nbr.lat AS lat, nbr.lng AS lng, distance_km,
               nbr.price_band AS price_band, nbr.top_menu_items AS top_menu_items,
               nbr.menu_price_min AS menu_price_min, nbr.menu_price_max AS menu_price_max,
               nbr.menu_price_median AS menu_price_median,
               categories, cuisines, dish_families, sim, attributes, rep.summary AS community_report
        ORDER BY sim DESC, CASE WHEN distance_km IS NULL THEN 1 ELSE 0 END, distance_km ASC, rating DESC
        LIMIT $max_neighbors
        """
        rows = self.neo4j.run(query, {"keys": seed_store_keys, "max_neighbors": max_neighbors, "user_lat": lat, "user_lng": lng})
        return [add_user_distance_to_record(r, lat, lng, self.config.distance_decay_km) for r in rows]

    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        user_lat: Optional[float] = None,
        user_lng: Optional[float] = None,
        use_cross_encoder: Optional[bool] = None,
    ) -> tuple[dict, list[dict], RetrievalTrace]:
        trace = RetrievalTrace(query=query)
        intent = self.intent_parser(query)
        trace.intent = intent
        lat, lng = self._location(user_lat, user_lng)

        graph_hits = self.graph_candidate_search(intent, top_k=max(10, top_k), user_lat=lat, user_lng=lng)
        restaurant_hits = self.vector_store.search_restaurants(query, top_k=max(10, top_k), user_lat=lat, user_lng=lng)
        seed_keys = list(dict.fromkeys([r["store_key"] for r in graph_hits[:3]] + [r["store_key"] for r in restaurant_hits[:3]]))
        neighbor_hits = self.subgraph_expand_candidates(seed_keys, max_neighbors=8, user_lat=lat, user_lng=lng)
        store_scope = list({*(r["store_key"] for r in graph_hits), *(r["store_key"] for r in restaurant_hits), *(r["store_key"] for r in neighbor_hits)})
        text_hits = self.vector_store.search_text_units(query, top_k=20, store_keys=store_scope or None)

        ranked_rrf = rerank_candidates(
            query=query,
            graph_hits=graph_hits,
            neighbor_hits=neighbor_hits,
            restaurant_vector_hits=restaurant_hits,
            text_unit_hits=text_hits,
            intent=intent,
            summary_by_key=self.summary_by_key,
            distance_weight=self.config.distance_weight,
            max_distance_km=self.config.max_distance_km,
        )
        should_use_ce = self.config.use_cross_encoder if use_cross_encoder is None else use_cross_encoder
        if should_use_ce and self.cross_encoder_reranker is not None:
            ranked = self.cross_encoder_reranker.rerank(
                query=query,
                candidates=ranked_rrf,
                top_k=top_k,
                intent=intent,
                max_distance_km=self.config.max_distance_km,
            )
        else:
            ranked = ranked_rrf[:top_k]
        trace.add_candidates(ranked)
        if self.trace_logger:
            self.trace_logger.write(trace)
        return intent, ranked, trace

    @staticmethod
    def format_prompt_context(rows: list[dict]) -> str:
        lines = []
        for i, r in enumerate(rows, start=1):
            evidence = " | ".join(r.get("evidence", [])[:2]) if r.get("evidence") else ""
            dist = f" | distance_km={r.get('distance_km'):.2f}" if r.get("distance_km") is not None else ""
            lines.append(
                f"{i}. {r.get('name')} | rating={r.get('rating')} | district={r.get('district')}{dist} | "
                f"score={r.get('final_score'):.4f} | sources={','.join(r.get('source_flags', []))}\n"
                f"   address={r.get('address')}\n"
                f"   evidence={evidence}"
            )
        return "\n\n".join(lines)

    def dump_trace_context(self, trace: RetrievalTrace, rows: list[dict]) -> RetrievalTrace:
        trace.prompt_context = self.format_prompt_context(rows)
        return trace
