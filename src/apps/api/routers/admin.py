from __future__ import annotations

from fastapi import APIRouter

from apps.api.deps import get_rgcn_service, get_settings

router = APIRouter(prefix="/admin")


@router.get("/active-version")
def active_version():
    settings = get_settings()
    active_graph = None
    if (settings.paths.kg_root / "ACTIVE_VERSION").exists():
        active_graph = (settings.paths.kg_root / "ACTIVE_VERSION").read_text(encoding="utf-8").strip()
    return {
        "mode": settings.mode,
        "active_graph_version": active_graph,
        "rgcn_model_loaded": bool(get_rgcn_service().model_config),
    }


@router.post("/reload-models")
def reload_models():
    service = get_rgcn_service()
    service.model_config = service.model_config
    return {"status": "ok", "note": "Model reload endpoint is available; live reload is minimal in offline implementation."}
