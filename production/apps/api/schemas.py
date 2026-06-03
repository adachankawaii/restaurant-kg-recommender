from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class RecommendRequest(BaseModel):
    query: str
    manual_rules: dict[str, Any] | None = None
    algorithm: Literal["graphrag", "rgcn", "hybrid"] = "hybrid"
    top_k: int = Field(default=5, ge=1, le=20)
    session_id: str | None = None
    user_lat: float | None = None
    user_lng: float | None = None
    distance_tolerance_m: float | None = Field(default=None, ge=1)


class FeedbackRequest(BaseModel):
    session_id: str
    restaurant_id: str
    feedback: Literal["like", "dislike", "click", "save"]
    rank_position: int | None = None
