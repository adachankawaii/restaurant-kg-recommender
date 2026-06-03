from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from common import append_jsonl, utc_now_iso
from services.logging_service.scenario_logger import ScenarioLogger


class EventLogger:
    def __init__(self, user_events_root, mode: str):
        self.user_events_root = user_events_root
        self.mode = mode
        self.scenario_logger = ScenarioLogger(user_events_root.parent)

    def log_event(self, payload: dict) -> dict:
        event = {
            "event_id": str(uuid4()),
            "timestamp": utc_now_iso(),
            **payload,
        }
        date_str = datetime.now(UTC).strftime("%Y-%m-%d")
        path = self.user_events_root / date_str / "events.jsonl"
        append_jsonl(path, event)
        return event

    def log_shown_scenarios(self, *, session_id: str, raw_query: str, rules: dict, results: list[dict], normalized_query: str = "") -> None:
        self.scenario_logger.log_shown_candidates(
            session_id=session_id,
            raw_query=raw_query,
            normalized_query=normalized_query,
            rules=rules,
            results=results,
        )

    def label_clicked_scenario(self, *, session_id: str, restaurant_id: str, rank_position: int | None = None):
        return self.scenario_logger.label_click(
            session_id=session_id,
            restaurant_id=restaurant_id,
            rank_position=rank_position,
        )
