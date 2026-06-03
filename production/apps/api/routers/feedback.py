from __future__ import annotations

from fastapi import APIRouter

from apps.api.deps import get_event_logger
from apps.api.schemas import FeedbackRequest

router = APIRouter()


@router.post("/feedback")
def submit_feedback(req: FeedbackRequest):
    logger = get_event_logger()
    event = logger.log_event(
        {
            "session_id": req.session_id,
            "event_type": "feedback_created",
            "clicked_restaurant_id": req.restaurant_id,
            "feedback_value": req.feedback,
            "rank_position": req.rank_position,
        }
    )
    if req.feedback == "click":
        logger.label_clicked_scenario(
            session_id=req.session_id,
            restaurant_id=req.restaurant_id,
            rank_position=req.rank_position,
        )
    return {"status": "ok", "event": event}
