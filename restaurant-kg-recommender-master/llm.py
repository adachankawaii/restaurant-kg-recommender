from __future__ import annotations

import json
from typing import Callable

from config import AppConfig
from ingest import slugify_vn
from retriever import RestaurantIntent


def get_llm(config: AppConfig):
    if config.llm_provider == "anthropic":
        if not config.anthropic_api_key:
            raise RuntimeError("LLM_PROVIDER=anthropic but ANTHROPIC_API_KEY is missing.")
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=config.anthropic_model, api_key=config.anthropic_api_key, temperature=0)
    if config.llm_provider == "openai":
        if not config.openai_api_key:
            raise RuntimeError("LLM_PROVIDER=openai but OPENAI_API_KEY is missing.")
        from langchain_openai import ChatOpenAI

        kwargs = {"model": config.openai_model, "api_key": config.openai_api_key, "temperature": 0}
        if config.openai_base_url:
            kwargs["base_url"] = config.openai_base_url
        return ChatOpenAI(**kwargs)
    raise ValueError(f"Unsupported LLM_PROVIDER={config.llm_provider}. Use 'openai' or 'anthropic'.")


def fallback_intent_parser(query: str) -> dict:
    q = slugify_vn(query)
    intent = RestaurantIntent().model_dump()
    if "com" in q:
        intent["dish_name"] = "cơm"
    elif "pho" in q:
        intent["dish_name"] = "phở"
    elif "bun" in q:
        intent["dish_name"] = "bún"
    entity_terms = []
    for term in ["cho-de-xe", "wifi", "dieu-hoa", "ho-tay", "hen-ho", "tu-tap", "sinh-vien", "takeaway"]:
        if term in q:
            entity_terms.append(term.replace("-", " "))
    if entity_terms:
        intent["entity_terms"] = entity_terms
    if any(x in q for x in ["gan-nhat", "closest", "nearest"]):
        intent["geo_intent"] = "nearest"
        intent["max_distance_km"] = 3.0
    elif any(x in q for x in ["gan", "quanh", "near", "around", "xung-quanh"]):
        intent["geo_intent"] = "nearby"
        intent["max_distance_km"] = 3.0
    if "dong-da" in q:
        intent["district"] = "Dong Da"
    if "hai-ba-trung" in q:
        intent["district"] = "Hai Ba Trung"
    return intent


def create_intent_parser(config: AppConfig, use_llm: bool = True) -> Callable[[str], dict]:
    if not use_llm:
        return fallback_intent_parser

    try:
        from langchain.prompts import ChatPromptTemplate

        llm = get_llm(config)
        intent_llm = llm.with_structured_output(RestaurantIntent)
        prompt = ChatPromptTemplate.from_messages([
            ("system", """Bạn là bộ phân tích intent cho hệ gợi ý quán ăn GraphRAG.
Trích xuất truy vấn thành schema RestaurantIntent.
Chỉ chọn district/cuisine/category nếu người dùng thật sự nêu hoặc suy ra rõ.
Nếu query nhắc tiện ích/ngữ cảnh/mốc địa lý/chất lượng món cụ thể ngoài schema cứng, đưa cụm đó vào entity_terms để match ExtractedEntity từ review.
Nếu người dùng nói "gần nhất", set geo_intent=\"nearest\". Nếu nói "gần đây", "quanh tôi", "trong bán kính X km", set geo_intent=\"nearby\" và set max_distance_km nếu có số km rõ ràng; nếu không có số rõ ràng thì để null."""),
            ("human", "Query: {query}"),
        ])

        def parse(query: str) -> dict:
            parsed = (prompt | intent_llm).invoke({"query": query})
            if hasattr(parsed, "model_dump"):
                return parsed.model_dump()
            if isinstance(parsed, dict):
                return parsed
            return fallback_intent_parser(query)

        return parse
    except Exception:
        return fallback_intent_parser


def format_recommendation_context(rows: list[dict]) -> str:
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


def generate_answer(config: AppConfig, query: str, intent: dict, rows: list[dict]) -> str:
    try:
        from langchain.prompts import ChatPromptTemplate

        llm = get_llm(config)
        prompt = ChatPromptTemplate.from_messages([
            ("system", "Bạn là trợ lý gợi ý quán ăn. Chỉ dùng Context được cung cấp, không bịa thông tin."),
            ("human", "Query: {query}\nIntent: {intent}\n\nContext:\n{context}"),
        ])
        resp = (prompt | llm).invoke({
            "query": query,
            "intent": json.dumps(intent, ensure_ascii=False),
            "context": format_recommendation_context(rows),
        })
        return resp.content if hasattr(resp, "content") else str(resp)
    except Exception:
        return format_recommendation_context(rows)
