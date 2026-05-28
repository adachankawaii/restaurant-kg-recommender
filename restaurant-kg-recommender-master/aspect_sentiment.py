from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd

from cache import JsonDiskCache
from config import AppConfig
from ingest import normalize_text


ASPECTS: dict[str, str] = {
    "food_quality": "chat luong mon an, do ngon, huong vi, do tuoi",
    "service": "thai do va chat luong phuc vu cua nhan vien",
    "cleanliness": "ve sinh, sach se, mui, an toan thuc pham",
    "packaging": "dong goi, giao hang khong do vo, day du mon",
    "price": "gia ca, do dang tien, phu hop sinh vien",
    "space": "khong gian quan, do rong, yen tinh, thoai mai",
    "speed": "toc do phuc vu, thoi gian cho mon hoac giao hang",
}

LABEL_TO_SCORE = {
    "negative": -1.0, "neg": -1.0, "0": -1.0,
    "neutral": 0.0, "neu": 0.0, "1": 0.0,
    "positive": 1.0, "pos": 1.0, "2": 1.0,
}

ASPECT_SCORE_KEYS = list(ASPECTS.keys())


def sanitize_aspect_scores(raw: Any) -> dict[str, float]:
    if hasattr(raw, "model_dump"):
        raw = raw.model_dump()
    if not isinstance(raw, dict):
        return {key: 0.0 for key in ASPECT_SCORE_KEYS}
    if "scores" in raw and isinstance(raw["scores"], dict):
        raw = raw["scores"]
    scores: dict[str, float] = {}
    for key in ASPECT_SCORE_KEYS:
        try:
            value = float(raw.get(key, 0.0))
        except Exception:
            value = 0.0
        scores[key] = round(max(-1.0, min(1.0, value)), 4)
    return scores


class AspectSentimentService:
    def __init__(self, config: AppConfig, device: Optional[str] = None, batch_size: int = 64):
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self.config = config
        self.torch = torch
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.batch_size = batch_size
        self.tokenizer = AutoTokenizer.from_pretrained(config.aspect_sentiment_model, use_fast=False)
        self.model = AutoModelForSequenceClassification.from_pretrained(config.aspect_sentiment_model).to(self.device)
        self.model.eval()
        self.id2label = {int(k): str(v).lower() for k, v in self.model.config.id2label.items()}
        self.aspect_names = list(ASPECTS.keys())
        self.aspect_descs = list(ASPECTS.values())
        self.cache = JsonDiskCache(config.cache_dir, "aspect_sentiment")

    def _batch_infer(self, texts: list[str]) -> list[float]:
        scores = []
        with self.torch.inference_mode():
            for i in range(0, len(texts), self.batch_size):
                batch = texts[i:i + self.batch_size]
                enc = self.tokenizer(batch, truncation=True, max_length=256, padding=True, return_tensors="pt").to(self.device)
                logits = self.model(**enc).logits
                probs = self.torch.softmax(logits, dim=-1).detach().cpu().numpy()
                for row in probs:
                    expected, mass = 0.0, 0.0
                    for j, p in enumerate(row):
                        label = self.id2label.get(j, str(j)).lower()
                        score = LABEL_TO_SCORE.get(label)
                        if score is None:
                            raise RuntimeError(f"Unsupported label {label} from {self.config.aspect_sentiment_model}")
                        expected += float(p) * score
                        mass += float(p)
                    scores.append(round(expected / max(mass, 1e-9), 4))
        return scores

    def score_reviews_batch(self, reviews: list[str]) -> list[dict[str, float]]:
        normalized = [normalize_text(x) for x in reviews]
        results: list[Optional[dict[str, float]]] = [None] * len(normalized)
        missing_indexes = []
        missing_reviews = []
        for idx, review in enumerate(normalized):
            cached = self.cache.get_or_compute(
                {"model": self.config.aspect_sentiment_model, "review": review},
                lambda: None,
            )
            if cached is None:
                missing_indexes.append(idx)
                missing_reviews.append(review)
            else:
                results[idx] = cached

        if missing_reviews:
            aspect_texts = []
            for desc in self.aspect_descs:
                for review in missing_reviews:
                    aspect_texts.append(f"Khía cạnh: {desc}. Nhận xét: {review}")
            all_scores = self._batch_infer(aspect_texts)
            n = len(missing_reviews)
            for local_idx, original_idx in enumerate(missing_indexes):
                result = {}
                for asp_idx, asp_name in enumerate(self.aspect_names):
                    result[asp_name] = all_scores[asp_idx * n + local_idx]
                results[original_idx] = result
                self.cache.set(
                    self.cache_key(normalized[original_idx]),
                    result,
                )

        self.cache.save()
        return [r or {} for r in results]

    def cache_key(self, review: str) -> str:
        from cache import stable_hash

        return stable_hash({"model": self.config.aspect_sentiment_model, "review": review})


class LLMAspectSentimentService:
    """Food-domain aspect sentiment using an LLM with cache.

    Scores are in [-1, 1]. Negative food-specific descriptors like "khô",
    "cứng", "ngấy", "bở", "tanh" are handled in-context by the LLM instead
    of relying on a generic sentiment classifier.
    """

    prompt_version = "llm_food_aspect_v1"

    def __init__(self, config: AppConfig, llm=None):
        from langchain.prompts import ChatPromptTemplate
        from pydantic import BaseModel, Field
        from llm import get_llm

        class AspectResult(BaseModel):
            scores: dict[str, float] = Field(default_factory=dict)

        self.config = config
        self.cache = JsonDiskCache(config.cache_dir, "llm_aspect_sentiment")
        llm = llm or get_llm(config)
        self.chain = ChatPromptTemplate.from_messages([
            ("system", """Bạn chấm aspect sentiment cho review quán ăn Việt Nam.
Trả điểm từng aspect trong [-1, 1], chỉ dựa trên text.
Aspect hợp lệ:
- food_quality: hương vị, độ tươi, texture; các từ như khô/cứng/ngấy/tanh/bở là tiêu cực nếu nói về món ăn.
- service: thái độ phục vụ.
- cleanliness: vệ sinh, mùi, an toàn.
- packaging: đóng gói/giao hàng.
- price: giá, đáng tiền.
- space: không gian, chỗ ngồi.
- speed: tốc độ phục vụ/giao.
Nếu aspect không được nhắc, trả 0. Không bịa thông tin."""),
            ("human", "Review: {review}"),
        ]) | llm.with_structured_output(AspectResult)

    def score_review(self, review: str) -> dict[str, float]:
        normalized = normalize_text(review)
        payload = {
            "model": self.config.llm_aspect_model_id,
            "prompt_version": self.prompt_version,
            "review": normalized,
        }
        cached = self.cache.get_or_compute(payload, lambda: None)
        if cached is not None:
            return sanitize_aspect_scores(cached)
        result = sanitize_aspect_scores(self.chain.invoke({"review": normalized}))
        from cache import stable_hash

        self.cache.set(stable_hash(payload), result)
        return result

    def score_reviews_batch(self, reviews: list[str]) -> list[dict[str, float]]:
        results = [self.score_review(review) for review in reviews]
        self.cache.save()
        return results


def create_aspect_sentiment_service(config: AppConfig):
    if config.aspect_sentiment_backend == "llm":
        return LLMAspectSentimentService(config)
    if config.aspect_sentiment_backend != "phobert":
        raise ValueError("ASPECT_SENTIMENT_BACKEND must be 'phobert' or 'llm'")
    return AspectSentimentService(config)


def classify_sentiment_from_aspects(aspect_scores: dict[str, float]) -> str:
    avg = float(np.mean(list(aspect_scores.values()))) if aspect_scores else 0.0
    if avg >= 0.20:
        return "positive"
    if avg <= -0.20:
        return "negative"
    return "neutral"


def score_feedback_dataframe(feedback: pd.DataFrame, service: AspectSentimentService) -> pd.DataFrame:
    out = feedback.copy()
    out["feedback_norm"] = out["feedback"].apply(normalize_text)
    out["aspect_scores"] = service.score_reviews_batch(out["feedback_norm"].tolist())
    out["sentiment"] = out["aspect_scores"].apply(classify_sentiment_from_aspects)
    return out
