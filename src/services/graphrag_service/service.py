from __future__ import annotations

import ast
import json
import os
import re
from pathlib import Path

import pandas as pd

from common import latest_complete_dir
from services.graphrag_service.context_builder import build_context
from services.graphrag_service.retriever import LocalVectorRetriever
from services.distance import as_float, distance_km, distance_meters
from settings import Settings


def _parse_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
        except Exception:
            return [text]
    return [item.strip() for item in text.split("|") if item.strip()]


def _normalize_token_text(value: str) -> str:
    value = str(value or "").lower()
    value = value.replace("đ", "d")
    import unicodedata

    value = "".join(char for char in unicodedata.normalize("NFKD", value) if not unicodedata.combining(char))
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return " ".join(value.split())


def _query_terms(query: str, rules: dict) -> set[str]:
    values = [
        query,
        rules.get("food"),
        rules.get("dish_name"),
        rules.get("location"),
        rules.get("district"),
        " ".join(_parse_list(rules.get("categories"))),
        " ".join(_parse_list(rules.get("cuisines"))),
        " ".join(_parse_list(rules.get("entity_terms"))),
        " ".join(_parse_list(rules.get("required_attributes"))),
    ]
    stop = {"a", "an", "co", "cua", "duoc", "gan", "minh", "muon", "o", "quan", "tim", "toi", "va"}
    return {term for term in _normalize_token_text(" ".join(str(item or "") for item in values)).split() if len(term) > 1 and term not in stop}


def _openai_api_key() -> str:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if api_key:
        return api_key
    if os.getenv("LLM_PROVIDER", "").strip().lower() == "openai":
        return os.getenv("LLM_API_KEY", "").strip()
    return ""


def _openai_model_name() -> str:
    model = os.getenv("OPENAI_MODEL", "").strip()
    if model:
        return model
    llm_model = os.getenv("LLM_MODEL", "").strip()
    if llm_model and llm_model.lower() not in {"local-or-api-model-name", "local", "none"}:
        return llm_model
    return "gpt-4.1-mini"


class OpenAIGraphRAGReasoner:
    def __init__(self, settings: Settings):
        api_key = _openai_api_key()
        if not api_key:
            raise RuntimeError("OpenAI API key is not set")
        from neo4j import GraphDatabase
        from openai import OpenAI

        self.settings = settings
        self.driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(
                os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j")),
                os.getenv("NEO4J_PASSWORD", "password"),
            ),
        )
        kwargs = {"api_key": api_key}
        if os.getenv("OPENAI_BASE_URL"):
            kwargs["base_url"] = os.getenv("OPENAI_BASE_URL")
        self.client = OpenAI(**kwargs)
        self.model = _openai_model_name()

    def close(self) -> None:
        self.driver.close()

    def _fetch_graph_context(self) -> list[dict]:
        cypher = """
        MATCH (r:Restaurant)
        OPTIONAL MATCH (r)-[:HAS_MENU_ITEM]->(mi:MenuItem)
        WITH r, collect(DISTINCT mi.name)[0..6] AS menu_items
        OPTIONAL MATCH (r)-[:HAS_CATEGORY]->(cat:Category)
        OPTIONAL MATCH (r)-[:HAS_CUISINE]->(cui:Cuisine)
        OPTIONAL MATCH (r)-[:SERVES_FAMILY|SERVES]->(df:DishFamily)
        OPTIONAL MATCH (r)-[:HAS_EXTRACTED_ENTITY]->(ee:ExtractedEntity)
        OPTIONAL MATCH (r)<-[:ABOUT]-(tu:TextUnit)
        OPTIONAL MATCH (r)-[:IN_COMMUNITY]->(com:Community)-[:HAS_REPORT]->(rep:CommunityReport)
        OPTIONAL MATCH (r)-[:SIMILAR_TO]-(sim:Restaurant)
        RETURN r.store_key AS restaurant_id,
               r.name AS name,
               r.address AS address,
               r.rating AS rating,
               r.review_count AS review_count,
               r.lat AS latitude,
               r.lng AS longitude,
               menu_items,
               collect(DISTINCT cat.name)[0..8] AS categories,
               collect(DISTINCT cui.name)[0..5] AS cuisines,
               collect(DISTINCT df.name)[0..8] AS dish_families,
               collect(DISTINCT {name: ee.name, type: ee.type})[0..8] AS extracted_entities,
               collect(DISTINCT tu.text)[0..3] AS review_evidence,
               collect(DISTINCT rep.title)[0..2] AS community_titles,
               collect(DISTINCT rep.summary)[0..2] AS community_reports,
               collect(DISTINCT sim.name)[0..5] AS similar_restaurants
        """
        with self.driver.session(database=os.getenv("NEO4J_DATABASE", "neo4j")) as session:
            return [record.data() for record in session.run(cypher)]

    def _candidate_subset(self, query: str, rules: dict, top_k: int) -> list[dict]:
        rows = self._fetch_graph_context()
        terms = set(_normalize_token_text(query).split())
        food_values = [
            rules.get("food"),
            rules.get("dish_name"),
            " ".join(_parse_list(rules.get("categories"))),
            " ".join(_parse_list(rules.get("cuisines"))),
        ]
        entity_values = [
            " ".join(_parse_list(rules.get("entity_terms"))),
            " ".join(_parse_list(rules.get("required_attributes"))),
        ]
        location_terms = set(_normalize_token_text(rules.get("location") or rules.get("district") or "").split())
        food_terms = set(_normalize_token_text(" ".join(str(item or "") for item in food_values)).split())
        entity_terms = set(_normalize_token_text(" ".join(str(item or "") for item in entity_values)).split())
        all_terms = terms | food_terms | location_terms | entity_terms
        query_lat = rules.get("query_lat")
        query_lng = rules.get("query_lng")
        max_distance_km = rules.get("max_distance_km")
        max_price = rules.get("max_price")
        price_band = str(rules.get("price_band") or "").lower().strip()

        scored = []
        for row in rows:
            text_parts = [
                row.get("name", ""),
                row.get("address", ""),
                " ".join(row.get("menu_items") or []),
                " ".join(row.get("categories") or []),
                " ".join(row.get("dish_families") or []),
                " ".join(item.get("name", "") for item in (row.get("extracted_entities") or []) if item),
                " ".join(row.get("community_titles") or []),
                " ".join(row.get("community_reports") or []),
            ]
            text = _normalize_token_text(" ".join(str(part) for part in text_parts if part))
            overlap = len(all_terms & set(text.split()))
            distance_m = distance_meters(query_lat, query_lng, row.get("latitude"), row.get("longitude"))
            if max_distance_km is not None and distance_m is not None:
                try:
                    if distance_m / 1000.0 > float(max_distance_km):
                        continue
                except (TypeError, ValueError):
                    pass
            distance_bonus = 0.0
            geo_intent = str(rules.get("geo_intent") or "").lower()
            if distance_m is not None and ("distance" in (rules.get("priority") or []) or geo_intent in {"nearest", "nearby"}):
                tolerance = float(rules.get("distance_tolerance_m") or 1500.0)
                distance_bonus = max(0.0, 1.0 - min(distance_m / max(tolerance, 1.0), 1.0))
            popularity = min(float(row.get("review_count") or 0) / 1000.0, 1.0)
            price_bonus = 0.0
            if max_price:
                matched_prices = []
                for item in row.get("menu_items") or []:
                    if isinstance(item, dict) and item.get("price") is not None:
                        try:
                            matched_prices.append(float(item["price"]))
                        except (TypeError, ValueError):
                            pass
                if matched_prices and min(matched_prices) <= float(max_price):
                    price_bonus = 0.5
            if price_band:
                # Full price-band filtering is handled in Neo4j/notebook KG; this is a soft signal for the LLM subset.
                price_bonus += 0.1
            row["distance_m"] = round(distance_m, 1) if distance_m is not None else None
            row["distance_km"] = distance_km(distance_m)
            row["_prefilter_score"] = overlap + distance_bonus + popularity + price_bonus
            scored.append(row)
        scored.sort(key=lambda item: item["_prefilter_score"], reverse=True)
        return scored[: max(top_k * 8, 24)]

    @staticmethod
    def _context_lines(candidates: list[dict]) -> str:
        lines = []
        for row in candidates:
            evidence = []
            evidence.extend(str(item) for item in (row.get("menu_items") or [])[:4] if item)
            evidence.extend(str(item)[:180] for item in (row.get("review_evidence") or [])[:2] if item)
            entities = ", ".join(
                f"{item.get('name')} ({item.get('type')})"
                for item in (row.get("extracted_entities") or [])[:5]
                if item and item.get("name")
            )
            community = " ".join(str(item) for item in (row.get("community_reports") or row.get("community_titles") or []) if item)
            lines.append(
                json.dumps(
                    {
                        "restaurant_id": str(row.get("restaurant_id", "")),
                        "name": row.get("name", ""),
                        "rating": row.get("rating"),
                        "review_count": row.get("review_count"),
                        "distance_km": row.get("distance_km"),
                        "categories": row.get("categories") or [],
                        "cuisines": row.get("cuisines") or [],
                        "dish_families": row.get("dish_families") or [],
                        "extracted_entities": entities,
                        "community_report": community[:500],
                        "evidence": evidence[:6],
                        "similar_restaurants": row.get("similar_restaurants") or [],
                    },
                    ensure_ascii=False,
                )
            )
        return "\n".join(lines)

    def recommend(self, query: str, rules: dict, top_k: int) -> list[dict]:
        candidates = self._candidate_subset(query, rules, top_k)
        if not candidates:
            return []
        by_id = {str(row.get("restaurant_id")): row for row in candidates}
        prompt = (
            "You are the production restaurant GraphRAG reasoner. Use only the supplied KG context. "
            "Reason over menu items, review text units, extracted entities, community reports, similar restaurants, "
            "rating, distance, and query constraints. Return recommendations with Vietnamese user-facing reasons. "
            "Each item must include restaurant_id, score from 0 to 1, and a short reason.\n\n"
            f"Query: {query}\nRules: {json.dumps(rules, ensure_ascii=False)}\nTop_k: {top_k}\n\n"
            "KG context JSONL:\n"
            f"{self._context_lines(candidates)}"
        )
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": "Return a JSON object shaped exactly as {\"recommendations\": [...]}."},
                {"role": "user", "content": prompt},
            ],
        )
        content = response.choices[0].message.content or "{}"
        payload = json.loads(content)
        recommendations = payload.get("recommendations", payload if isinstance(payload, list) else [])
        ranked = []
        for item in recommendations:
            restaurant_id = str(item.get("restaurant_id", "")).strip()
            if restaurant_id not in by_id:
                continue
            row = by_id[restaurant_id]
            score = float(item.get("score") or row.get("_prefilter_score") or 0.0)
            ranked.append(
                {
                    "restaurant_id": restaurant_id,
                    "name": row.get("name", ""),
                    "matched_items": [{"name": name, "price": None} for name in (row.get("menu_items") or [])[:5]],
                    "categories": row.get("categories") or [],
                    "dish_families": row.get("dish_families") or [],
                    "review_count": float(row.get("review_count") or 0),
                    "rating": float(row.get("rating") or 0),
                    "latitude": as_float(row.get("latitude")),
                    "longitude": as_float(row.get("longitude")),
                    "distance_m": row.get("distance_m"),
                    "distance_km": row.get("distance_km"),
                    "evidence": [
                        {"source": "graphrag_kg", "field": "reason", "value": str(item.get("reason", ""))},
                        {"source": "community_report", "field": "summary", "value": " ".join(row.get("community_reports") or row.get("community_titles") or [])[:220]},
                    ],
                    "extracted_entities": row.get("extracted_entities") or [],
                    "graphrag_score": score,
                    "rule_score": 0.0,
                    "popularity_score": min(float(row.get("review_count") or 0) / 1000.0, 1.0),
                    "graphrag_reason": str(item.get("reason", "")),
                    "graphrag_mode": "openai_kg",
                }
            )
        ranked.sort(key=lambda row: row.get("graphrag_score", 0.0), reverse=True)
        return ranked[:top_k]


class GraphRAGService:
    @staticmethod
    def _read_optional_csv(path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path).fillna("")
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def __init__(self, settings: Settings):
        active_index = json.loads((settings.paths.data_lake_root / "vector_index" / "ACTIVE_INDEX.json").read_text(encoding="utf-8"))
        self.retriever = LocalVectorRetriever(Path(active_index["index_path"]))
        self.openai_reasoner = None
        if _openai_api_key():
            try:
                self.openai_reasoner = OpenAIGraphRAGReasoner(settings)
            except Exception:
                self.openai_reasoner = None
        processed_dir = latest_complete_dir(
            settings.paths.processed_root,
            ["canonical_restaurants.csv", "canonical_menu_items.csv"],
            "processed ingestion output",
        )
        self.restaurants = pd.read_csv(processed_dir / "canonical_restaurants.csv").fillna("")
        self.menu_items = self._read_optional_csv(processed_dir / "menu_items_enriched.csv")
        self.text_units = self._read_optional_csv(processed_dir / "text_units.csv")
        self.entities = self._read_optional_csv(processed_dir / "extracted_entities.csv")
        self.community_reports = self._read_optional_csv(processed_dir / "community_reports.csv")

    def _restaurant_payload(self, restaurant_id: str) -> dict:
        matched = self.restaurants[self.restaurants["restaurant_id"].astype(str) == restaurant_id]
        if matched.empty:
            return {}
        row = matched.iloc[0]
        menu_rows = self.menu_items[self.menu_items["store_key"].astype(str) == restaurant_id] if "store_key" in self.menu_items.columns else pd.DataFrame()
        text_rows = self.text_units[self.text_units["store_key"].astype(str) == restaurant_id] if "store_key" in self.text_units.columns else pd.DataFrame()
        entity_rows = self.entities[self.entities["store_key"].astype(str) == restaurant_id] if "store_key" in self.entities.columns else pd.DataFrame()
        evidence = []
        for _, menu_row in menu_rows.head(3).iterrows():
            evidence.append(
                {
                    "source": "menu",
                    "source_record_id": str(menu_row.get("menu_item_id", menu_row.get("restaurant_item_id", ""))),
                    "field": "menu_items",
                    "value": str(menu_row.get("item_name", "")),
                }
            )
        for _, text_row in text_rows.head(3).iterrows():
            evidence.append(
                {
                    "source": "review",
                    "source_record_id": str(text_row.get("text_unit_id", "")),
                    "field": "text_units",
                    "value": str(text_row.get("chunk_text", ""))[:220],
                }
            )
        return {
            "restaurant_id": restaurant_id,
            "name": row["name"],
            "matched_items": [
                {"name": str(menu_row.get("item_name", "")), "price": menu_row.get("price")}
                for _, menu_row in menu_rows.head(5).iterrows()
            ],
            "categories": _parse_list(row.get("categories", "")),
            "dish_families": _parse_list(menu_rows["dish_family"].tolist() if "dish_family" in menu_rows.columns else []),
            "review_count": float(row.get("review_count", 0) or 0),
            "rating": float(row.get("rating", 0) or 0),
            "latitude": as_float(row.get("latitude")),
            "longitude": as_float(row.get("longitude")),
            "distance_m": None,
            "distance_km": as_float(row.get("distance_km")),
            "evidence": evidence,
            "extracted_entities": [
                {"name": str(ent.get("name", "")), "type": str(ent.get("entity_type", ""))}
                for _, ent in entity_rows.head(5).iterrows()
            ],
        }

    def _local_graphrag_recommend(self, query: str, rules: dict, top_k: int) -> list[dict]:
        terms = _query_terms(query, rules)
        query_lat = rules.get("query_lat")
        query_lng = rules.get("query_lng")
        geo_intent = str(rules.get("geo_intent") or "").lower()
        wants_distance = "distance" in (rules.get("priority") or []) or geo_intent in {"nearest", "nearby"}
        tolerance_m = float(rules.get("distance_tolerance_m") or 1500.0)
        max_distance_km = rules.get("max_distance_km")
        max_price = rules.get("max_price")

        candidates = self.retriever.search(query, top_k=max(top_k * 20, 80))
        seed_ids = [str(candidate.get("restaurant_id", "")).strip() for candidate in candidates if candidate.get("restaurant_id")]
        if len(seed_ids) < top_k * 4:
            seed_ids.extend(self.restaurants["restaurant_id"].astype(str).head(200).tolist())

        grouped = {}
        for restaurant_id in dict.fromkeys(seed_ids):
            payload = self._restaurant_payload(restaurant_id)
            if not payload:
                continue
            menu_text = " ".join(str(item.get("name", "")) for item in payload.get("matched_items", []))
            entity_text = " ".join(str(item.get("name", "")) for item in payload.get("extracted_entities", []))
            evidence_text = " ".join(str(item.get("value", "")) for item in payload.get("evidence", []))
            haystack = _normalize_token_text(
                " ".join(
                    [
                        str(payload.get("name", "")),
                        " ".join(payload.get("categories", [])),
                        " ".join(payload.get("dish_families", [])),
                        menu_text,
                        entity_text,
                        evidence_text,
                    ]
                )
            )
            hay_terms = set(haystack.split())
            term_score = len(terms & hay_terms) / max(len(terms), 1)

            distance_m = distance_meters(query_lat, query_lng, payload.get("latitude"), payload.get("longitude"))
            if distance_m is not None:
                payload["distance_m"] = round(distance_m, 1)
                payload["distance_km"] = distance_km(distance_m)
                if max_distance_km is not None and distance_m / 1000.0 > float(max_distance_km):
                    continue
            distance_score = 0.0
            if wants_distance and distance_m is not None:
                distance_score = max(0.0, 1.0 - min(distance_m / max(tolerance_m, 1.0), 1.0))

            price_score = 0.0
            if max_price:
                prices = [float(item.get("price") or 0) for item in payload.get("matched_items", []) if item.get("price")]
                if prices and min(prices) <= float(max_price):
                    price_score = 0.5

            rating_score = min(float(payload.get("rating") or 0) / 5.0, 1.0)
            popularity_score = min(float(payload.get("review_count") or 0) / 1000.0, 1.0)
            vector_score = 0.0
            for candidate in candidates:
                if str(candidate.get("restaurant_id", "")).strip() == restaurant_id:
                    vector_score += float(candidate.get("graphrag_score") or 0.0)
            vector_score = min(vector_score / 8.0, 1.0)

            payload["graphrag_score"] = round(0.38 * term_score + 0.20 * vector_score + 0.22 * rating_score + 0.20 * popularity_score, 6)
            payload["rule_score"] = round(distance_score + price_score, 6)
            payload["popularity_score"] = round(popularity_score, 6)
            payload["graphrag_mode"] = "local_kg_graphrag"
            payload["graphrag_reason"] = "Chấm nhanh trên KG/menu/review/entity đã build sẵn, không gọi LLM."
            grouped[restaurant_id] = payload

        ranked = list(grouped.values())
        ranked.sort(key=lambda item: float(item.get("graphrag_score", 0.0)) + float(item.get("rule_score", 0.0)), reverse=True)
        return ranked[:top_k]

    def recommend(self, query: str, rules: dict, top_k: int = 5) -> list[dict]:
        local_ranked = self._local_graphrag_recommend(query, rules, top_k)
        if local_ranked:
            build_context(query, local_ranked)
            return local_ranked
        if self.openai_reasoner is not None:
            try:
                return self.openai_reasoner.recommend(query, rules, top_k)
            except Exception:
                pass
        candidates = self.retriever.search(query, top_k=max(top_k * 10, 40))
        grouped: dict[str, dict] = {}
        query_lat = rules.get("query_lat")
        query_lng = rules.get("query_lng")
        tolerance_m = float(rules.get("distance_tolerance_m") or 1500.0)
        wants_distance = "distance" in (rules.get("priority") or [])
        for candidate in candidates:
            restaurant_id = str(candidate.get("restaurant_id", "")).strip()
            if not restaurant_id:
                continue
            payload = grouped.get(restaurant_id) or self._restaurant_payload(restaurant_id)
            if not payload:
                continue
            payload.setdefault("graphrag_score", 0.0)
            payload["graphrag_score"] += float(candidate.get("graphrag_score", 0.0))
            distance_m = distance_meters(query_lat, query_lng, payload.get("latitude"), payload.get("longitude"))
            if distance_m is not None:
                payload["distance_m"] = round(distance_m, 1)
                payload["distance_km"] = distance_km(distance_m)
                if wants_distance:
                    distance_score = max(0.0, 1.0 - min(distance_m / max(tolerance_m, 1.0), 1.0))
                    payload["rule_score"] = payload.get("rule_score", 0.0) + distance_score
            food_rule = rules.get("food") or rules.get("dish_name")
            if food_rule:
                food = str(food_rule).lower()
                payload["rule_score"] = payload.get("rule_score", 0.0) + (
                    1.0
                    if any(food in str(item.get("name", "")).lower() for item in payload["matched_items"])
                    or any(food in str(item).lower() for item in payload.get("categories", []))
                    or any(food in str(item).lower() for item in payload.get("dish_families", []))
                    else 0.0
                )
            else:
                payload["rule_score"] = payload.get("rule_score", 0.0)
            payload["popularity_score"] = min(payload.get("review_count", 0.0) / 1000.0, 1.0)
            if rules.get("max_price"):
                matched_prices = [float(item.get("price") or 0) for item in payload["matched_items"] if item.get("price")]
                if matched_prices and min(matched_prices) <= float(rules["max_price"]):
                    payload["rule_score"] += 0.5
            payload["graphrag_mode"] = "rule_based"
            grouped[restaurant_id] = payload
        ranked = list(grouped.values())
        ranked.sort(key=lambda item: (float(item.get("graphrag_score", 0.0)) + float(item.get("rule_score", 0.0))), reverse=True)
        build_context(query, ranked)
        return ranked[:top_k]
