from __future__ import annotations

from fastapi import APIRouter

from apps.api.deps import get_settings

router = APIRouter()


@router.get("/health")
def health():
    settings = get_settings()
    return {"status": "ok", "mode": settings.mode}
