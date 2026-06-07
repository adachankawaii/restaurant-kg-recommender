from __future__ import annotations

import argparse
import csv
import math
import random
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import torch
import torch.nn as nn
import torch.nn.functional as F

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent))
    from src.data_loader import load_graph_data  # type: ignore
    from src.model import RGCN, score_edges  # type: ignore
    from src.utils import ensure_dir, set_seed, write_log  # type: ignore
else:
    from .src.data_loader import load_graph_data
    from .src.model import RGCN, score_edges
    from .src.utils import ensure_dir, set_seed, write_log


@dataclass
class Phase2Config:
    epochs: int = 50
    lr: float = 0.005
    weight_decay: float = 1e-4
    num_neg: int = 10
    eval_every: int = 1
    reg_lambda: float = 0.0
    seed: int = 42
    add_reverse_edges: bool = True
    topk: int = 5
    emb_dim: int = 64
    dropout: float = 0.1
    num_bases: int | None = None
    scorer_hidden_dim: int = 128
    pos_order_alpha: float = 0.0
    kg_aux_beta: float = 0.05
    hard_negative_ratio: float = 0.5
    model_hard_negative_ratio: float = 0.25
    kg_aux_batch_size: int = 2048


@dataclass
class QueryExample:
    query_node_id: str
    query_idx: int
    area_id: str
    time_slot_id: str
    term_tokens: list[str]
    aspect_tokens: list[str]
    price_range_id: str
    query_lat: float | None
    query_lng: float | None
    distance_tolerance_m: float
    pos_store_indices: list[int]
    pos_weights: list[float]
    pos_row_by_store: dict[int, dict[str, str]]


PAIR_FEATURE_DIM = 8


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def pick(row: dict[str, Any], candidates: Iterable[str], default: str = "") -> str:
    for key in candidates:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return default


def as_float(value: object) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(str(value))
    except ValueError:
        return None


def as_int(value: object) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except ValueError:
        return None


def as_bool(value: object) -> bool:
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def strip_accents(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def slugify(text: str) -> str:
    text = strip_accents(text).lower()
    cleaned = []
    for ch in text:
        if ch.isalnum() or ch in {"_", " "}:
            cleaned.append(ch)
        else:
            cleaned.append(" ")
    text = "".join(cleaned)
    text = "_".join(text.split())
    return text


def split_tokens(value: str) -> list[str]:
    return [slugify(part) for part in value.split("|") if part.strip()]


ASPECT_ALIASES = {
    "taste": "food_quality",
    "food": "food_quality",
    "food_quality": "food_quality",
    "value": "price",
    "value_for_money": "price",
    "price": "price",
    "staff": "service",
    "staff_service": "service",
    "service": "service",
    "clean": "cleanliness",
    "cleanliness": "cleanliness",
    "fast_delivery": "speed",
    "delivery": "speed",
    "speed": "speed",
    "portion": "food_quality",
    "high_rating": "food_quality",
    "nearby": "location",
}


def split_aspect_tokens(value: str) -> list[str]:
    tokens = []
    for token in split_tokens(value):
        mapped = ASPECT_ALIASES.get(token, token)
        if mapped and mapped not in tokens:
            tokens.append(mapped)
    return tokens


def price_range_to_budget(price_range_id: str) -> int:
    value = slugify(price_range_id)
    if value in {"under_50k", "u50k", "below_50k"}:
        return 50000
    if value in {"50k_100k", "50k_to_100k", "between_50k_100k"}:
        return 100000
    if value in {"over_100k", "above_100k"}:
        return 200000
    return 60000


def rating_to_score(rating: object) -> float:
    value = as_float(rating)
    if value is None:
        return 0.0
    value = max(1.0, min(5.0, value))
    return (value - 3.0) / 2.0


def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    r = 6371000
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def infer_checkpoint_num_relations(state: dict[str, torch.Tensor]) -> int | None:
    if "conv1.comp" in state:
        return int(state["conv1.comp"].shape[0])
    if "conv1.weight" in state:
        weight = state["conv1.weight"]
        if weight.dim() == 3:
            return int(weight.shape[0])
    return None


def expand_embedding(embedding: nn.Embedding, new_num_nodes: int) -> nn.Embedding:
    old_weight = embedding.weight.data
    if new_num_nodes <= old_weight.size(0):
        return embedding
    new_emb = nn.Embedding(new_num_nodes, old_weight.size(1))
    nn.init.xavier_uniform_(new_emb.weight)
    new_emb.weight.data[: old_weight.size(0)] = old_weight
    return new_emb


def expand_rgcn_conv(conv: nn.Module, new_num_relations: int) -> None:
    old_num_relations = getattr(conv, "num_relations", None)
    if old_num_relations is None:
        if hasattr(conv, "weight") and isinstance(conv.weight, torch.Tensor):
            if conv.weight.dim() == 3:
                old_num_relations = int(conv.weight.shape[0])
        if old_num_relations is None and hasattr(conv, "comp") and isinstance(conv.comp, torch.Tensor):
            old_num_relations = int(conv.comp.shape[0])
    if old_num_relations is None or new_num_relations <= old_num_relations:
        return

    if hasattr(conv, "comp") and isinstance(conv.comp, torch.Tensor):
        old_comp = conv.comp.data
        new_comp = torch.empty((new_num_relations, old_comp.size(1)), device=old_comp.device)
        nn.init.xavier_uniform_(new_comp)
        new_comp[:old_num_relations] = old_comp
        conv.comp = nn.Parameter(new_comp)
        conv.num_relations = new_num_relations
        return

    if hasattr(conv, "weight") and isinstance(conv.weight, torch.Tensor) and conv.weight.dim() == 3:
        old_weight = conv.weight.data
        new_weight = torch.empty(
            (new_num_relations, old_weight.size(1), old_weight.size(2)),
            device=old_weight.device,
        )
        nn.init.xavier_uniform_(new_weight)
        new_weight[:old_num_relations] = old_weight
        conv.weight = nn.Parameter(new_weight)
        conv.num_relations = new_num_relations
        return

    raise RuntimeError("Unsupported RGCNConv layout for relation expansion.")


def build_edge_index(
    triples: torch.Tensor,
    base_rel_count: int,
    full_rel_count: int,
    add_reverse_edges: bool,
) -> tuple[torch.Tensor, torch.Tensor]:
    if triples.numel() == 0:
        return torch.empty((2, 0), dtype=torch.long), torch.empty((0,), dtype=torch.long)
    edge_index = triples[:, [0, 2]].t().contiguous()
    rel = triples[:, 1].contiguous()
    if not add_reverse_edges:
        return edge_index, rel

    query_rel_count = max(full_rel_count - base_rel_count, 0)
    forward_rel = rel.clone()
    if query_rel_count > 0:
        mask = rel >= base_rel_count
        if mask.any():
            forward_rel[mask] = base_rel_count * 2 + (rel[mask] - base_rel_count)

    reverse_index = triples[:, [2, 0]].t().contiguous()
    reverse_rel = rel.clone()
    if query_rel_count > 0:
        mask = rel >= base_rel_count
        if mask.any():
            reverse_rel[mask] = base_rel_count * 2 + query_rel_count + (rel[mask] - base_rel_count)
        reverse_rel[~mask] = base_rel_count + rel[~mask]
    else:
        reverse_rel = base_rel_count + rel

    edge_index = torch.cat([edge_index, reverse_index], dim=1)
    edge_type = torch.cat([forward_rel, reverse_rel], dim=0)
    return edge_index, edge_type


class InteractionScoringHead(nn.Module):
    """MLP interaction head for scenario-store ranking."""

    def __init__(self, emb_dim: int, pair_feature_dim: int = PAIR_FEATURE_DIM, hidden_dim: int = 128):
        super().__init__()
        input_dim = emb_dim * 4 + pair_feature_dim
        self.net = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Linear(hidden_dim // 2, 1),
        )

    def forward(self, z_q: torch.Tensor, z_s: torch.Tensor, pair_features: torch.Tensor) -> torch.Tensor:
        if z_q.dim() == 1:
            z_q = z_q.unsqueeze(0).expand(z_s.size(0), -1)
        interaction = torch.cat([z_q, z_s, z_q * z_s, torch.abs(z_q - z_s), pair_features], dim=1)
        return self.net(interaction).squeeze(-1)


def score_pairs(
    z_q: torch.Tensor,
    z_s: torch.Tensor,
    pair_features: torch.Tensor,
    scorer: nn.Module | torch.Tensor,
    beta: torch.Tensor | None = None,
) -> torch.Tensor:
    if isinstance(scorer, nn.Module):
        return scorer(z_q, z_s, pair_features)

    if beta is None:
        raise ValueError("beta is required for legacy bilinear scoring.")
    weight_matrix = scorer
    proj = z_q @ weight_matrix
    bilinear = (z_s * proj.unsqueeze(0)).sum(dim=1)
    feature_score = pair_features @ beta
    return bilinear + feature_score


def build_store_category_map(
    triples: torch.Tensor,
    idx_to_node_id: list[str],
    rel_to_idx: dict[str, int],
) -> dict[int, set[str]]:
    term_rels = {
        rel_to_idx.get("HAS_CATEGORY"),
        rel_to_idx.get("HAS_PRIMARY_CATEGORY"),
        rel_to_idx.get("SERVES"),
    }
    term_rels.discard(None)
    if not term_rels:
        return {}
    store_terms: dict[int, set[str]] = {}
    for src_idx, rel_idx, dst_idx in triples.tolist():
        if rel_idx not in term_rels:
            continue
        src_id = idx_to_node_id[src_idx]
        dst_id = idx_to_node_id[dst_idx]
        if not src_id.startswith("store:") or not (dst_id.startswith("category:") or dst_id.startswith("dish:")):
            continue
        token = slugify(dst_id.split(":", 1)[1])
        store_terms.setdefault(src_idx, set()).add(token)
    return store_terms


def build_store_metadata(
    rows: list[dict[str, str]],
    node_id_to_idx: dict[str, int],
) -> dict[int, dict[str, float]]:
    store_meta: dict[int, dict[str, float]] = {}
    for row in rows:
        store_node_id = pick(row, ("store_node_id", "store_id", "node_id"))
        if store_node_id and not store_node_id.startswith("store:"):
            store_node_id = f"store:{store_node_id}"
        store_idx = node_id_to_idx.get(store_node_id)
        if store_idx is None:
            continue

        lat = as_float(pick(row, ("latitude", "lat"), ""))
        lng = as_float(pick(row, ("longitude", "lng"), ""))
        price = as_float(pick(row, ("median_price", "menu_price_median", "price_median"), ""))
        if price is None:
            lo = as_float(pick(row, ("price_min", "menu_price_min"), ""))
            hi = as_float(pick(row, ("price_max", "menu_price_max"), ""))
            vals = [v for v in (lo, hi) if v is not None]
            price = sum(vals) / len(vals) if vals else None

        meta = store_meta.get(store_idx, {})
        if lat is not None and lng is not None:
            meta["lat"] = lat
            meta["lng"] = lng
        if price is not None:
            meta["median_price"] = price
        rating = as_float(pick(row, ("rating",), ""))
        review_count = as_float(pick(row, ("review_count",), ""))
        if rating is not None:
            meta["rating"] = rating
        if review_count is not None:
            meta["review_count"] = review_count
        if meta:
            store_meta[store_idx] = meta
    return store_meta


def build_pair_features(
    query: QueryExample,
    store_indices: list[int],
    store_meta: dict[int, dict[str, float]],
    store_categories: dict[int, set[str]],
) -> torch.Tensor:
    target_price = price_range_to_budget(query.price_range_id)
    features: list[list[float]] = []
    term_tokens = set(query.term_tokens)
    tolerance = max(query.distance_tolerance_m, 1.0)

    for store_idx in store_indices:
        row = query.pos_row_by_store.get(store_idx)
        distance_m = None
        price = None
        is_closed = None
        rating = None
        review_count = None

        if row is not None:
            distance_m = as_float(row.get("distance_m"))
            price = as_float(row.get("median_price"))
            is_closed = as_bool(row.get("is_closed")) if row.get("is_closed") is not None else None
            rating = as_float(row.get("rating"))
            review_count = as_float(row.get("review_count"))

        if distance_m is None:
            meta = store_meta.get(store_idx)
            if (
                meta
                and "lat" in meta
                and "lng" in meta
                and query.query_lat is not None
                and query.query_lng is not None
            ):
                distance_m = haversine_m(query.query_lat, query.query_lng, meta["lat"], meta["lng"])
            else:
                distance_m = tolerance

        if price is None:
            meta = store_meta.get(store_idx)
            price = meta.get("median_price") if meta and "median_price" in meta else None
        if price is None:
            price = float(target_price)
        if rating is None:
            meta = store_meta.get(store_idx)
            rating = meta.get("rating") if meta and "rating" in meta else None
        if review_count is None:
            meta = store_meta.get(store_idx)
            review_count = meta.get("review_count") if meta and "review_count" in meta else None

        open_flag = 1.0
        if is_closed is not None:
            open_flag = 0.0 if is_closed else 1.0

        category_tokens = store_categories.get(store_idx, set())
        category_match = 1.0 if term_tokens and term_tokens.intersection(category_tokens) else 0.0
        aspect_sentiment = rating_to_score(rating) if query.aspect_tokens else 0.0
        review_confidence = min(math.log1p(max(float(review_count or 0.0), 0.0)) / 8.0, 1.0)

        distance_norm = float(distance_m) / tolerance
        price_diff = abs(float(price) - target_price) / max(target_price, 1)
        radius_ok = 1.0 if distance_norm <= 1.0 else 0.0
        service_match = 0.0
        features.append(
            [
                distance_norm,
                price_diff,
                open_flag,
                category_match,
                aspect_sentiment,
                review_confidence,
                radius_ok,
                service_match,
            ]
        )

    return torch.tensor(features, dtype=torch.float32)


def sample_negatives(
    store_pool: list[int],
    positives: set[int],
    num_samples: int,
    rng: random.Random,
) -> list[int]:
    negatives: list[int] = []
    if not store_pool or num_samples <= 0:
        return negatives
    max_attempts = max(num_samples * 30, 200)
    attempts = 0
    while len(negatives) < num_samples and attempts < max_attempts:
        attempts += 1
        candidate = rng.choice(store_pool)
        if candidate in positives:
            continue
        negatives.append(candidate)
    while len(negatives) < num_samples:
        candidate = rng.choice(store_pool)
        if candidate in positives:
            continue
        negatives.append(candidate)
    return negatives


def build_candidate_store_indices(
    query: QueryExample,
    store_pool: list[int],
    store_meta: dict[int, dict[str, float]],
    store_categories: dict[int, set[str]],
) -> list[int]:
    if not store_pool:
        return []

    features = build_pair_features(query, store_pool, store_meta, store_categories)
    has_location = query.query_lat is not None and query.query_lng is not None
    has_terms = bool(query.term_tokens)
    candidates: list[int] = []
    for idx, store_idx in enumerate(store_pool):
        row = features[idx]
        radius_ok = row[6].item() >= 0.5 if has_location else True
        category_ok = row[3].item() >= 0.5 if has_terms else True
        open_ok = row[2].item() >= 0.5
        if radius_ok and category_ok and open_ok:
            candidates.append(store_idx)

    positives = set(query.pos_store_indices)
    for store_idx in positives:
        if store_idx in store_pool and store_idx not in candidates:
            candidates.append(store_idx)
    return candidates if len(candidates) > len(positives) else list(store_pool)


def sample_scenario_negatives(
    query: QueryExample,
    candidate_pool: list[int],
    positives: set[int],
    num_samples: int,
    rng: random.Random,
    store_meta: dict[int, dict[str, float]],
    store_categories: dict[int, set[str]],
    *,
    model_scores: dict[int, float] | None = None,
    hard_ratio: float = 0.5,
    model_hard_ratio: float = 0.25,
) -> list[int]:
    negatives, _ = sample_scenario_negatives_with_stats(
        query,
        candidate_pool,
        positives,
        num_samples,
        rng,
        store_meta,
        store_categories,
        model_scores=model_scores,
        hard_ratio=hard_ratio,
        model_hard_ratio=model_hard_ratio,
    )
    return negatives


def sample_scenario_negatives_with_stats(
    query: QueryExample,
    candidate_pool: list[int],
    positives: set[int],
    num_samples: int,
    rng: random.Random,
    store_meta: dict[int, dict[str, float]],
    store_categories: dict[int, set[str]],
    *,
    model_scores: dict[int, float] | None = None,
    hard_ratio: float = 0.5,
    model_hard_ratio: float = 0.25,
) -> tuple[list[int], dict[str, int]]:
    stats = {
        "requested": max(num_samples, 0),
        "easy": 0,
        "metadata_hard": 0,
        "model_hard": 0,
        "available_hard_pool": 0,
        "available_model_hard_pool": 0,
    }
    if num_samples <= 0:
        return [], stats

    negative_pool = [idx for idx in candidate_pool if idx not in positives]
    if not negative_pool:
        return [], stats

    features = build_pair_features(query, negative_pool, store_meta, store_categories)
    hard_pool = [
        store_idx
        for row_idx, store_idx in enumerate(negative_pool)
        if features[row_idx, 3].item() >= 0.5
        or features[row_idx, 6].item() >= 0.5
        or features[row_idx, 1].item() <= 0.5
        or features[row_idx, 2].item() >= 0.5
    ]
    if not hard_pool:
        hard_pool = negative_pool
    stats["available_hard_pool"] = len(hard_pool)

    model_hard_pool: list[int] = []
    if model_scores and hard_ratio > 0 and model_hard_ratio > 0:
        model_hard_pool = [
            idx
            for idx, _ in sorted(
                ((idx, model_scores.get(idx, -float("inf"))) for idx in negative_pool),
                key=lambda item: item[1],
                reverse=True,
            )
        ][: max(num_samples, 1)]
    stats["available_model_hard_pool"] = len(model_hard_pool)

    effective_model_ratio = model_hard_ratio if hard_ratio > 0 else 0.0
    model_count = min(int(round(num_samples * effective_model_ratio)), num_samples)
    hard_count = min(int(round(num_samples * hard_ratio)), num_samples - model_count)
    easy_count = num_samples - model_count - hard_count

    negatives: list[int] = []

    def extend_from(pool: list[int], count: int) -> int:
        if not pool or count <= 0:
            return 0
        for _ in range(count):
            negatives.append(rng.choice(pool))
        return count

    stats["model_hard"] = extend_from(model_hard_pool, model_count)
    stats["metadata_hard"] = extend_from(hard_pool, hard_count)
    stats["easy"] = extend_from(negative_pool, easy_count)
    while len(negatives) < num_samples:
        negatives.append(rng.choice(negative_pool))
        stats["easy"] += 1
    return negatives[:num_samples], stats


def positive_order_loss(pos_scores: torch.Tensor, pos_weights: torch.Tensor | None = None) -> torch.Tensor:
    if pos_scores.numel() < 2:
        return pos_scores.new_tensor(0.0)
    losses: list[torch.Tensor] = []
    weights: list[float] = []
    for i in range(pos_scores.numel()):
        for j in range(i + 1, pos_scores.numel()):
            discount_i = 1.0 / math.log2(i + 2)
            discount_j = 1.0 / math.log2(j + 2)
            if pos_weights is not None and pos_weights.numel() == pos_scores.numel():
                gain_i = float(pos_weights[i].detach().clamp_min(0.0).item())
                gain_j = float(pos_weights[j].detach().clamp_min(0.0).item())
            else:
                gain_i = 1.0
                gain_j = 1.0
            omega = abs(gain_i * discount_i - gain_j * discount_j)
            losses.append(-F.logsigmoid(pos_scores[i] - pos_scores[j]))
            weights.append(max(omega, 1e-3))
    weight_tensor = pos_scores.new_tensor(weights)
    return (torch.stack(losses) * weight_tensor).sum() / weight_tensor.sum().clamp_min(1e-6)


def kg_auxiliary_loss(
    z: torch.Tensor,
    rel_emb: nn.Embedding,
    kg_edges: torch.Tensor,
    *,
    num_nodes: int,
    batch_size: int,
    rng: random.Random,
) -> torch.Tensor:
    if kg_edges.numel() == 0 or batch_size <= 0:
        return z.new_tensor(0.0)
    sample_size = min(batch_size, kg_edges.size(0))
    edge_idx = torch.tensor(rng.sample(range(kg_edges.size(0)), sample_size), device=kg_edges.device)
    pos_edges = kg_edges[edge_idx]
    neg_edges = pos_edges.clone()
    neg_edges[:, 2] = torch.randint(0, num_nodes, (sample_size,), device=kg_edges.device)
    pos_score = score_edges(z, pos_edges, rel_emb)
    neg_score = score_edges(z, neg_edges, rel_emb)
    return -F.logsigmoid(pos_score - neg_score).mean()


def build_queries_and_graph(
    query_rows: list[dict[str, str]],
    node_id_to_idx: dict[str, int],
    idx_to_node_id: list[str],
    node_types: list[str],
    rel_to_idx: dict[str, int],
    idx_to_rel: list[str],
    store_metadata_rows: list[dict[str, str]] | None = None,
) -> tuple[list[QueryExample], list[tuple[int, int, int]], dict[int, dict[str, float]]]:
    store_meta: dict[int, dict[str, float]] = build_store_metadata(store_metadata_rows or [], node_id_to_idx)
    queries: dict[str, QueryExample] = {}
    query_edges: set[tuple[int, int, int]] = set()

    def ensure_node(node_id: str, node_type: str) -> int:
        if node_id in node_id_to_idx:
            return node_id_to_idx[node_id]
        idx = len(idx_to_node_id)
        node_id_to_idx[node_id] = idx
        idx_to_node_id.append(node_id)
        node_types.append(node_type)
        return idx

    def ensure_relation(name: str) -> int:
        if name in rel_to_idx:
            return rel_to_idx[name]
        rel_to_idx[name] = len(idx_to_rel)
        idx_to_rel.append(name)
        return rel_to_idx[name]

    time_slot_map = {
        "lunch": "context:dining_option:bua_trua",
        "afternoon": "context:dining_option:bua_nua_buoi",
        "dinner": "context:dining_option:bua_toi",
    }

    for row in query_rows:
        query_node_id = pick(row, ("query_node_id", "query_id"))
        if not query_node_id:
            continue
        store_node_id = pick(row, ("store_node_id", "store_id"))
        if store_node_id and not store_node_id.startswith("store:"):
            store_node_id = f"store:{store_node_id}"
        if store_node_id and store_node_id not in node_id_to_idx:
            continue

        query_idx = ensure_node(query_node_id, "Query")
        store_idx = node_id_to_idx.get(store_node_id) if store_node_id else None

        area_id = pick(row, ("area_id",), "")
        time_slot_id = pick(row, ("time_slot_id",), "")
        term_tokens = split_tokens(pick(row, ("term",), ""))
        aspect_tokens = split_aspect_tokens(pick(row, ("preferred_aspects",), ""))
        price_range_id = pick(row, ("desired_price_range_id",), "")
        distance_tolerance_m = as_float(pick(row, ("distance_tolerance_m",), "")) or 1000.0
        query_lat = as_float(pick(row, ("query_lat",), ""))
        query_lng = as_float(pick(row, ("query_lng",), ""))

        if query_node_id not in queries:
            queries[query_node_id] = QueryExample(
                query_node_id=query_node_id,
                query_idx=query_idx,
                area_id=area_id,
                time_slot_id=time_slot_id,
                term_tokens=term_tokens,
                aspect_tokens=aspect_tokens,
                price_range_id=price_range_id,
                query_lat=query_lat,
                query_lng=query_lng,
                distance_tolerance_m=distance_tolerance_m,
                pos_store_indices=[],
                pos_weights=[],
                pos_row_by_store={},
            )

        query = queries[query_node_id]
        if store_idx is not None and store_idx not in query.pos_store_indices:
            query.pos_store_indices.append(store_idx)
            weight = as_float(row.get("relevance_weight")) or 1.0
            query.pos_weights.append(float(weight))
            query.pos_row_by_store[store_idx] = row

            lat = as_float(row.get("latitude"))
            lng = as_float(row.get("longitude"))
            price = as_float(row.get("median_price"))
            rating = as_float(row.get("rating"))
            review_count = as_float(row.get("review_count"))
            if lat is not None and lng is not None:
                meta = store_meta.get(store_idx, {})
                meta["lat"] = lat
                meta["lng"] = lng
                if price is not None:
                    meta["median_price"] = price
                if rating is not None:
                    meta["rating"] = rating
                if review_count is not None:
                    meta["review_count"] = review_count
                store_meta[store_idx] = meta
            elif price is not None or rating is not None or review_count is not None:
                meta = store_meta.get(store_idx, {})
                if price is not None and "median_price" not in meta:
                    meta["median_price"] = price
                if rating is not None and "rating" not in meta:
                    meta["rating"] = rating
                if review_count is not None and "review_count" not in meta:
                    meta["review_count"] = review_count
                store_meta[store_idx] = meta

        area_node = ""
        if area_id:
            area_candidate = f"area:{slugify(area_id)}"
            if area_candidate in node_id_to_idx:
                area_node = area_candidate
            else:
                area_node = f"query_area:{slugify(area_id)}"
            area_idx = ensure_node(area_node, "QueryFeature")
            rel_name = "LOCATED_IN" if "LOCATED_IN" in rel_to_idx else "QUERY_HAS_AREA"
            rel_idx = ensure_relation(rel_name)
            query_edges.add((query_idx, rel_idx, area_idx))

        if time_slot_id:
            mapped = time_slot_map.get(slugify(time_slot_id))
            time_node = ""
            if mapped and mapped in node_id_to_idx:
                time_node = mapped
            else:
                time_node = f"query_time:{slugify(time_slot_id)}"
            time_idx = ensure_node(time_node, "QueryFeature")
            rel_name = "HAS_CONTEXT_TAG" if "HAS_CONTEXT_TAG" in rel_to_idx else "QUERY_TIME_SLOT"
            rel_idx = ensure_relation(rel_name)
            query_edges.add((query_idx, rel_idx, time_idx))

        if price_range_id:
            price_node = f"query_price:{slugify(price_range_id)}"
            price_idx = ensure_node(price_node, "QueryFeature")
            rel_idx = ensure_relation("QUERY_PRICE_RANGE")
            query_edges.add((query_idx, rel_idx, price_idx))

        for token in term_tokens:
            if not token:
                continue
            linked = False
            category_node = f"category:{token}"
            if category_node in node_id_to_idx:
                rel_name = "HAS_CATEGORY" if "HAS_CATEGORY" in rel_to_idx else "QUERY_HAS_TERM"
                rel_idx = ensure_relation(rel_name)
                query_edges.add((query_idx, rel_idx, node_id_to_idx[category_node]))
                linked = True

            dish_node = f"dish:{token}"
            if dish_node in node_id_to_idx:
                rel_name = "SERVES" if "SERVES" in rel_to_idx else "QUERY_HAS_TERM"
                rel_idx = ensure_relation(rel_name)
                query_edges.add((query_idx, rel_idx, node_id_to_idx[dish_node]))
                linked = True

            if not linked:
                term_idx = ensure_node(f"query_term:{token}", "QueryFeature")
                rel_idx = ensure_relation("QUERY_HAS_TERM")
                query_edges.add((query_idx, rel_idx, term_idx))

        for token in aspect_tokens:
            if not token:
                continue
            aspect_node = f"aspect:{token}"
            context_node = f"context:{token}"
            if aspect_node in node_id_to_idx:
                aspect_idx = node_id_to_idx[aspect_node]
            elif context_node in node_id_to_idx:
                aspect_idx = node_id_to_idx[context_node]
            else:
                aspect_idx = ensure_node(f"query_aspect:{token}", "QueryFeature")
            rel_name = "HAS_CONTEXT_TAG" if "HAS_CONTEXT_TAG" in rel_to_idx else "QUERY_HAS_ASPECT"
            rel_idx = ensure_relation(rel_name)
            query_edges.add((query_idx, rel_idx, aspect_idx))

    return list(queries.values()), list(query_edges), store_meta


def evaluate(
    model: nn.Module,
    edge_index: torch.Tensor,
    edge_type: torch.Tensor,
    queries: list[QueryExample],
    store_indices: list[int],
    store_meta: dict[int, dict[str, float]],
    store_categories: dict[int, set[str]],
    scorer: nn.Module,
    topk: int,
) -> dict[str, float]:
    model.eval()
    scorer.eval()
    with torch.no_grad():
        z = model(edge_index, edge_type)
    device = z.device
    hits: list[float] = []
    recalls: list[float] = []
    store_idx_tensor = torch.tensor(store_indices, dtype=torch.long, device=device)

    for query in queries:
        z_q = z[query.query_idx]
        candidate_indices = build_candidate_store_indices(query, store_indices, store_meta, store_categories)
        candidate_tensor = torch.tensor(candidate_indices, dtype=torch.long, device=device)
        features = build_pair_features(query, candidate_indices, store_meta, store_categories).to(device)
        scores = score_pairs(z_q, z[candidate_tensor], features, scorer)
        _, top_idx = torch.topk(scores, k=min(topk, scores.numel()))
        top_store_indices = {candidate_indices[i] for i in top_idx.tolist()}
        positives = set(query.pos_store_indices)
        if not positives:
            continue
        hit = 1.0 if positives.intersection(top_store_indices) else 0.0
        recall = len(positives.intersection(top_store_indices)) / len(positives)
        hits.append(hit)
        recalls.append(recall)

    if not hits:
        return {"hit@k": 0.0, "recall@k": 0.0}
    return {
        "hit@k": float(sum(hits) / len(hits)),
        "recall@k": float(sum(recalls) / len(recalls)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Phase 2 fine-tune for top-k store recommendation.")
    parser.add_argument("--nodes", type=Path, default=Path(__file__).resolve().parent / "data" / "graphrag_nodes.csv")
    parser.add_argument("--edges", type=Path, default=Path(__file__).resolve().parent / "data" / "graphrag_edges.csv")
    parser.add_argument(
        "--queries",
        type=Path,
        default=Path(__file__).resolve().parent / "query_store_top5_merged.csv",
    )
    parser.add_argument(
        "--store-metadata",
        type=Path,
        default=Path(__file__).resolve().parent / "data" / "store_metadata.csv",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=Path(__file__).resolve().parent / "outputs_graphrag" / "best_model.pt",
    )
    parser.add_argument("--pretrained-embeddings", type=Path, default=None)
    parser.add_argument("--outputs", type=Path, default=Path(__file__).resolve().parent / "outputs_phase2_graphrag")

    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--lr", type=float, default=0.005)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--num-neg", type=int, default=10)
    parser.add_argument("--pos-order-alpha", type=float, default=0.0)
    parser.add_argument("--kg-aux-beta", type=float, default=0.05)
    parser.add_argument("--hard-negative-ratio", type=float, default=0.5)
    parser.add_argument("--model-hard-negative-ratio", type=float, default=0.25)
    parser.add_argument("--eval-every", type=int, default=1)
    parser.add_argument("--reg-lambda", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument("--no-reverse-edges", action="store_true")

    parser.add_argument("--emb-dim", type=int, default=64)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--num-bases", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = Phase2Config(
        epochs=args.epochs,
        lr=args.lr,
        weight_decay=args.weight_decay,
        num_neg=args.num_neg,
        pos_order_alpha=args.pos_order_alpha,
        kg_aux_beta=args.kg_aux_beta,
        hard_negative_ratio=args.hard_negative_ratio,
        model_hard_negative_ratio=args.model_hard_negative_ratio,
        eval_every=args.eval_every,
        reg_lambda=args.reg_lambda,
        seed=args.seed,
        add_reverse_edges=not args.no_reverse_edges,
        topk=args.topk,
        emb_dim=args.emb_dim,
        dropout=args.dropout,
        num_bases=args.num_bases,
    )

    set_seed(config.seed)
    output_dir = ensure_dir(args.outputs)
    log_path = output_dir / "logs.txt"
    log_path.write_text("", encoding="utf-8")

    if not args.nodes.exists() or not args.edges.exists():
        raise FileNotFoundError("nodes.csv or edges.csv not found.")
    if not args.queries.exists():
        raise FileNotFoundError("Query CSV not found.")

    data = load_graph_data(args.nodes, args.edges, add_reverse_edges=False, seed=config.seed)
    base_num_nodes = data["num_nodes"]
    base_num_rels = data["num_rels"]

    node_id_to_idx = dict(data["node_id_to_idx"])
    idx_to_node_id = list(data["idx_to_node_id"])
    node_types = list(data["node_types"])
    rel_to_idx = dict(data["rel_to_idx"])
    idx_to_rel = list(data["idx_to_rel"])

    query_rows = read_csv_rows(args.queries)
    store_metadata_rows = read_csv_rows(args.store_metadata) if args.store_metadata.exists() else []
    queries, query_edges, store_meta = build_queries_and_graph(
        query_rows,
        node_id_to_idx,
        idx_to_node_id,
        node_types,
        rel_to_idx,
        idx_to_rel,
        store_metadata_rows=store_metadata_rows,
    )

    if not queries:
        raise RuntimeError("No query rows loaded. Check the query CSV format.")

    checkpoint_state: dict[str, Any] | None = None
    ckpt_message_rels = base_num_rels * (2 if config.add_reverse_edges else 1)
    if args.checkpoint.exists():
        checkpoint_state = torch.load(args.checkpoint, map_location="cpu", weights_only=False)
        state_dict = checkpoint_state.get("model_state", {})
        inferred_rels = infer_checkpoint_num_relations(state_dict)
        if inferred_rels is not None:
            ckpt_message_rels = inferred_rels
        if "emb.weight" in state_dict:
            ckpt_emb_dim = int(state_dict["emb.weight"].shape[1])
            if ckpt_emb_dim != config.emb_dim:
                write_log(log_path, f"Override emb_dim from checkpoint: {ckpt_emb_dim}")
                config.emb_dim = ckpt_emb_dim
        ckpt_config = checkpoint_state.get("config")
        if isinstance(ckpt_config, dict) and config.num_bases is None:
            config.num_bases = ckpt_config.get("num_bases")
        write_log(log_path, f"Loaded checkpoint: {args.checkpoint}")

    if ckpt_message_rels > base_num_rels and not config.add_reverse_edges:
        write_log(log_path, "Checkpoint uses reverse edges; enabling add_reverse_edges.")
        config.add_reverse_edges = True

    full_num_nodes = len(idx_to_node_id)
    full_num_rels = len(idx_to_rel)
    query_rel_count = max(full_num_rels - base_num_rels, 0)
    if config.add_reverse_edges:
        full_message_rels = base_num_rels * 2 + query_rel_count * 2
    else:
        full_message_rels = full_num_rels

    base_triples = data["all_positive_edges"]
    base_edge_index, base_edge_type = build_edge_index(
        base_triples,
        base_num_rels,
        full_num_rels,
        config.add_reverse_edges,
    )

    if query_edges:
        query_triples = torch.tensor(query_edges, dtype=torch.long)
        query_edge_index, query_edge_type = build_edge_index(
            query_triples,
            base_num_rels,
            full_num_rels,
            config.add_reverse_edges,
        )
        edge_index = torch.cat([base_edge_index, query_edge_index], dim=1)
        edge_type = torch.cat([base_edge_type, query_edge_type], dim=0)
    else:
        edge_index, edge_type = base_edge_index, base_edge_type

    store_categories = build_store_category_map(base_triples, idx_to_node_id, rel_to_idx)
    store_indices = [idx for idx, node_type in enumerate(node_types) if node_type == "Store"]
    if not store_indices:
        raise RuntimeError("No Store nodes found in nodes.csv")

    model = RGCN(
        num_nodes=base_num_nodes,
        num_rels=ckpt_message_rels,
        emb_dim=config.emb_dim,
        num_bases=config.num_bases,
        dropout=config.dropout,
    )

    if checkpoint_state is not None:
        model.load_state_dict(checkpoint_state["model_state"], strict=True)

    model.emb = expand_embedding(model.emb, full_num_nodes)
    if ckpt_message_rels != full_message_rels:
        expand_rgcn_conv(model.conv1, full_message_rels)
        expand_rgcn_conv(model.conv2, full_message_rels)

    scorer = InteractionScoringHead(
        config.emb_dim,
        pair_feature_dim=PAIR_FEATURE_DIM,
        hidden_dim=config.scorer_hidden_dim,
    )
    kg_rel_emb = nn.Embedding(base_num_rels, config.emb_dim)
    nn.init.xavier_uniform_(kg_rel_emb.weight)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    scorer = scorer.to(device)
    kg_rel_emb = kg_rel_emb.to(device)
    edge_index = edge_index.to(device=device, dtype=torch.long)
    edge_type = edge_type.to(device=device, dtype=torch.long)
    base_triples_device = base_triples.to(device=device, dtype=torch.long)

    optimizer = torch.optim.Adam(
        list(model.parameters()) + list(scorer.parameters()) + list(kg_rel_emb.parameters()),
        lr=config.lr,
        weight_decay=config.weight_decay,
    )

    pretrained_store_z = None
    if args.pretrained_embeddings and args.pretrained_embeddings.exists():
        emb_state = torch.load(args.pretrained_embeddings, map_location="cpu", weights_only=False)
        pretrain_map = emb_state.get("node_id_to_idx", {})
        pretrain_emb = emb_state.get("embeddings")
        if isinstance(pretrain_emb, torch.Tensor) and pretrain_map:
            if pretrain_emb.size(1) != config.emb_dim:
                write_log(log_path, "Skip pretrained embeddings: embedding dim mismatch.")
                pretrain_emb = None
        if isinstance(pretrain_emb, torch.Tensor) and pretrain_map:
            pretrained_store_z = torch.zeros((full_num_nodes, pretrain_emb.size(1)), dtype=pretrain_emb.dtype)
            for node_id, idx in node_id_to_idx.items():
                if node_id in pretrain_map:
                    pretrained_store_z[idx] = pretrain_emb[pretrain_map[node_id]]
            pretrained_store_z = pretrained_store_z.to(device)
            write_log(log_path, f"Loaded pretrained embeddings: {args.pretrained_embeddings}")

    for epoch in range(1, config.epochs + 1):
        rng = random.Random(config.seed + epoch)
        model.train()
        scorer.train()
        optimizer.zero_grad(set_to_none=True)
        z = model(edge_index, edge_type)

        total_loss = z.new_tensor(0.0)
        total_items = 0
        pos_neg_loss_sum = 0.0
        pos_order_loss_sum = 0.0
        hard_negative_count = 0
        total_negative_count = 0
        pos_order_eligible = 0

        for query in queries:
            if not query.pos_store_indices:
                continue
            pos_set = set(query.pos_store_indices)
            if len(query.pos_store_indices) >= 2:
                pos_order_eligible += 1
            num_neg = config.num_neg * len(query.pos_store_indices)
            if num_neg <= 0:
                continue
            candidate_indices = build_candidate_store_indices(query, store_indices, store_meta, store_categories)
            with torch.no_grad():
                candidate_tensor = torch.tensor(candidate_indices, dtype=torch.long, device=device)
                candidate_features = build_pair_features(query, candidate_indices, store_meta, store_categories).to(device)
                candidate_scores = score_pairs(z[query.query_idx], z[candidate_tensor], candidate_features, scorer)
                model_scores = {
                    store_idx: float(candidate_scores[i].detach().cpu())
                    for i, store_idx in enumerate(candidate_indices)
                }
            neg_indices, neg_stats = sample_scenario_negatives_with_stats(
                query,
                candidate_indices,
                pos_set,
                num_neg,
                rng,
                store_meta,
                store_categories,
                model_scores=model_scores,
                hard_ratio=config.hard_negative_ratio,
                model_hard_ratio=config.model_hard_negative_ratio,
            )
            if not neg_indices:
                continue
            hard_negative_count += neg_stats["metadata_hard"] + neg_stats["model_hard"]
            total_negative_count += len(neg_indices)

            pos_features = build_pair_features(query, query.pos_store_indices, store_meta, store_categories).to(device)
            neg_features = build_pair_features(query, neg_indices, store_meta, store_categories).to(device)

            z_q = z[query.query_idx]
            z_pos = z[torch.tensor(query.pos_store_indices, device=device)]
            z_neg = z[torch.tensor(neg_indices, device=device)]

            pos_scores = score_pairs(z_q, z_pos, pos_features, scorer)
            neg_scores = score_pairs(z_q, z_neg, neg_features, scorer)
            neg_scores = neg_scores.view(len(query.pos_store_indices), -1)

            weights = torch.tensor(query.pos_weights, dtype=torch.float32, device=device)
            pair_loss = -F.logsigmoid(pos_scores.unsqueeze(1) - neg_scores).mean(dim=1)
            pos_order = positive_order_loss(pos_scores, weights)
            pos_neg_loss = (weights * pair_loss).mean()
            loss_q = pos_neg_loss + config.pos_order_alpha * pos_order
            pos_neg_loss_sum += float(pos_neg_loss.detach().cpu())
            pos_order_loss_sum += float(pos_order.detach().cpu())
            total_loss = total_loss + loss_q
            total_items += 1

        if total_items == 0:
            raise RuntimeError("No training pairs found in query data.")

        loss = total_loss / total_items
        if pretrained_store_z is not None and config.reg_lambda > 0:
            store_idx_tensor = torch.tensor(store_indices, device=device)
            reg_loss = F.mse_loss(z[store_idx_tensor], pretrained_store_z[store_idx_tensor])
            loss = loss + config.reg_lambda * reg_loss
        if config.kg_aux_beta > 0:
            loss = loss + config.kg_aux_beta * kg_auxiliary_loss(
                z,
                kg_rel_emb,
                base_triples_device,
                num_nodes=base_num_nodes,
                batch_size=config.kg_aux_batch_size,
                rng=rng,
            )

        loss.backward()
        torch.nn.utils.clip_grad_norm_(list(model.parameters()) + list(scorer.parameters()), 1.0)
        optimizer.step()

        write_log(
            log_path,
            (
                f"Epoch {epoch} Loss: {loss.item():.4f} "
                f"PosNeg: {pos_neg_loss_sum / max(total_items, 1):.4f} "
                f"PosOrder: {pos_order_loss_sum / max(total_items, 1):.4f} "
                f"HardNeg: {hard_negative_count}/{max(total_negative_count, 1)} "
                f"PosOrderEligible: {pos_order_eligible}/{len(queries)}"
            ),
        )

        if config.eval_every > 0 and epoch % config.eval_every == 0:
            metrics = evaluate(
                model,
                edge_index,
                edge_type,
                queries,
                store_indices,
                store_meta,
                store_categories,
                scorer,
                config.topk,
            )
            write_log(log_path, f"Eval hit@{config.topk}: {metrics['hit@k']:.4f}")
            write_log(log_path, f"Eval recall@{config.topk}: {metrics['recall@k']:.4f}")

    model.eval()
    with torch.no_grad():
        z_final = model(edge_index, edge_type).detach().cpu()

    model_path = output_dir / "phase2_model.pt"
    torch.save(
        {
            "model_state": model.state_dict(),
            "scoring_head_state": scorer.state_dict(),
            "kg_rel_emb": kg_rel_emb.state_dict(),
            "node_id_to_idx": node_id_to_idx,
            "idx_to_node_id": idx_to_node_id,
            "node_types": node_types,
            "rel_to_idx": rel_to_idx,
            "idx_to_rel": idx_to_rel,
            "config": config.__dict__,
        },
        model_path,
    )

    emb_path = output_dir / "phase2_embeddings.pt"
    torch.save(
        {
            "embeddings": z_final,
            "node_id_to_idx": node_id_to_idx,
            "idx_to_node_id": idx_to_node_id,
            "node_types": node_types,
            "rel_to_idx": rel_to_idx,
            "idx_to_rel": idx_to_rel,
        },
        emb_path,
    )

    write_log(log_path, f"Saved phase2 model: {model_path}")
    write_log(log_path, f"Saved phase2 embeddings: {emb_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
