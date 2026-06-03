from __future__ import annotations

from training.rgcn.model_registry import load_active_model_config


def load_active_model(models_root) -> dict:
    return load_active_model_config(models_root)
