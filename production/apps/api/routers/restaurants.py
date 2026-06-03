from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException

from apps.api.deps import get_event_logger, get_settings

router = APIRouter()


@router.get("/restaurants/{restaurant_id}")
def get_restaurant(restaurant_id: str, session_id: str | None = None, rank_position: int | None = None):
    settings = get_settings()
    processed_dirs = [path for path in settings.paths.processed_root.iterdir() if path.is_dir()]
    if not processed_dirs:
        raise HTTPException(status_code=404, detail="No processed data found")
    processed_dir = sorted(processed_dirs)[-1]
    frame = pd.read_csv(Path(processed_dir) / "canonical_restaurants.csv").fillna("")
    matched = frame[frame["restaurant_id"].astype(str) == restaurant_id]
    if matched.empty:
        raise HTTPException(status_code=404, detail="Restaurant not found")
    if session_id:
        logger = get_event_logger()
        logger.log_event(
            {
                "session_id": session_id,
                "event_type": "restaurant_clicked",
                "clicked_restaurant_id": restaurant_id,
                "rank_position": rank_position,
            }
        )
        logger.label_clicked_scenario(
            session_id=session_id,
            restaurant_id=restaurant_id,
            rank_position=rank_position,
        )
    return matched.iloc[0].to_dict()
