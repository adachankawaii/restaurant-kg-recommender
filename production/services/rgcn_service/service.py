from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import torch

from services.rgcn_service.model_loader import load_active_model
from services.query_parser.parser import normalize_query
from services.distance import as_float, distance_km, distance_meters
from settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
RGCN_ROOT = REPO_ROOT / "rgcn_pipeline"
if str(RGCN_ROOT) not in sys.path:
    sys.path.append(str(RGCN_ROOT))

from phase2_finetune import (  # type: ignore  # noqa: E402
    InteractionScoringHead,
    QueryExample,
    build_pair_features,
    build_store_category_map,
    score_pairs,
    slugify,
    split_aspect_tokens,
    split_tokens,
)
from src.data_loader import load_graph_data  # type: ignore  # noqa: E402
from src.model import RGCN  # type: ignore  # noqa: E402


def _split_tokens(value: str) -> set[str]:
    return {normalize_query(part.strip()) for part in str(value or "").split("|") if part.strip()}


def _price_bucket(max_price: int | None) -> str | None:
    if max_price is None:
        return None
    if max_price <= 50000:
        return "under_50k"
    if max_price <= 100000:
        return "50k_100k"
    return "over_100k"


def _as_int(value) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(str(value)))
    except ValueError:
        return None


def _relation_edge_index(triples: torch.Tensor, rel_count: int, add_reverse_edges: bool) -> tuple[torch.Tensor, torch.Tensor]:
    if triples.numel() == 0:
        return torch.empty((2, 0), dtype=torch.long), torch.empty((0,), dtype=torch.long)
    edge_index = triples[:, [0, 2]].t().contiguous()
    edge_type = triples[:, 1].contiguous()
    if not add_reverse_edges:
        return edge_index, edge_type
    reverse_index = triples[:, [2, 0]].t().contiguous()
    reverse_type = edge_type + rel_count
    return torch.cat([edge_index, reverse_index], dim=1), torch.cat([edge_type, reverse_type], dim=0)


class RGCNRuntimeRanker:
    def __init__(self, settings: Settings, model_config: dict):
        artifact_path = Path(str(model_config.get("artifact_path", "")))
        if not artifact_path.exists():
            raise FileNotFoundError(f"Active R-GCN artifact not found: {artifact_path}")

        self.settings = settings
        self.artifact_path = artifact_path
        self.checkpoint = torch.load(artifact_path, map_location="cpu", weights_only=False)
        self.node_id_to_idx = dict(self.checkpoint["node_id_to_idx"])
        self.idx_to_node_id = list(self.checkpoint["idx_to_node_id"])
        self.node_types = list(self.checkpoint["node_types"])
        self.rel_to_idx = dict(self.checkpoint["rel_to_idx"])
        self.idx_to_rel = list(self.checkpoint["idx_to_rel"])
        self.config = dict(self.checkpoint.get("config", {}))
        self.emb_dim = int(self.config.get("emb_dim", 64))
        self.hidden_dim = int(self.config.get("scorer_hidden_dim", 128))
        self.add_reverse_edges = bool(self.config.get("add_reverse_edges", True))

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.store_indices = [idx for idx, node_type in enumerate(self.node_types) if node_type == "Store"]
        if not self.store_indices:
            raise RuntimeError("Active R-GCN checkpoint has no Store nodes.")

        self.store_id_by_idx = {
            idx: self.idx_to_node_id[idx].split(":", 1)[1]
            for idx in self.store_indices
            if self.idx_to_node_id[idx].startswith("store:")
        }
        self.store_idx_by_id = {store_id: idx for idx, store_id in self.store_id_by_idx.items()}
        self.store_meta = self._load_store_metadata()
        self.store_payloads = self._load_store_payloads()

        rel_count = len(self.idx_to_rel)
        message_rel_count = rel_count * (2 if self.add_reverse_edges else 1)
        self.model = RGCN(
            num_nodes=len(self.idx_to_node_id),
            num_rels=message_rel_count,
            emb_dim=self.emb_dim,
            num_bases=self.config.get("num_bases"),
            dropout=float(self.config.get("dropout", 0.1)),
        )
        self.model.load_state_dict(self.checkpoint["model_state"], strict=True)
        self.scoring_head = InteractionScoringHead(self.emb_dim, hidden_dim=self.hidden_dim)
        self.scoring_head.load_state_dict(self.checkpoint["scoring_head_state"], strict=True)
        self.model = self.model.to(self.device).eval()
        self.scoring_head = self.scoring_head.to(self.device).eval()

        edge_index, edge_type = self._load_message_graph(rel_count)
        with torch.no_grad():
            self.node_embeddings = self.model(edge_index.to(self.device), edge_type.to(self.device)).detach()

        self.store_categories = self._load_store_categories()

    def _latest_snapshot(self) -> Path | None:
        candidates = [path for path in self.settings.paths.rgcn_root.iterdir() if path.is_dir()] if self.settings.paths.rgcn_root.exists() else []
        for candidate in reversed(sorted(candidates)):
            if (candidate / "nodes.csv").exists() and (candidate / "edges.csv").exists():
                return candidate
        return None

    def _load_message_graph(self, rel_count: int) -> tuple[torch.Tensor, torch.Tensor]:
        snapshot = self._latest_snapshot()
        if snapshot is None:
            return torch.empty((2, 0), dtype=torch.long), torch.empty((0,), dtype=torch.long)

        data = load_graph_data(snapshot / "nodes.csv", snapshot / "edges.csv", add_reverse_edges=False)
        triples = []
        for src_idx, rel_idx, dst_idx in data["all_positive_edges"].tolist():
            src_id = data["idx_to_node_id"][src_idx]
            dst_id = data["idx_to_node_id"][dst_idx]
            rel_name = data["idx_to_rel"][rel_idx]
            if src_id in self.node_id_to_idx and dst_id in self.node_id_to_idx and rel_name in self.rel_to_idx:
                triples.append((self.node_id_to_idx[src_id], self.rel_to_idx[rel_name], self.node_id_to_idx[dst_id]))
        if not triples:
            return torch.empty((2, 0), dtype=torch.long), torch.empty((0,), dtype=torch.long)
        return _relation_edge_index(torch.tensor(triples, dtype=torch.long), rel_count, self.add_reverse_edges)

    def _load_store_categories(self) -> dict[int, set[str]]:
        snapshot = self._latest_snapshot()
        if snapshot is None:
            return {}
        data = load_graph_data(snapshot / "nodes.csv", snapshot / "edges.csv", add_reverse_edges=False)
        triples = []
        for src_idx, rel_idx, dst_idx in data["all_positive_edges"].tolist():
            src_id = data["idx_to_node_id"][src_idx]
            dst_id = data["idx_to_node_id"][dst_idx]
            rel_name = data["idx_to_rel"][rel_idx]
            if src_id in self.node_id_to_idx and dst_id in self.node_id_to_idx and rel_name in self.rel_to_idx:
                triples.append((self.node_id_to_idx[src_id], self.rel_to_idx[rel_name], self.node_id_to_idx[dst_id]))
        if not triples:
            return {}
        return build_store_category_map(torch.tensor(triples, dtype=torch.long), self.idx_to_node_id, self.rel_to_idx)

    def _load_store_metadata(self) -> dict[int, dict[str, float]]:
        metadata: dict[int, dict[str, float]] = {}
        snapshot = self._latest_snapshot()
        paths = []
        if snapshot is not None:
            paths.append(snapshot / "store_metadata.csv")
        processed_dirs = [path for path in self.settings.paths.processed_root.iterdir() if path.is_dir()] if self.settings.paths.processed_root.exists() else []
        if processed_dirs:
            paths.append(sorted(processed_dirs)[-1] / "canonical_restaurants.csv")
        for path in paths:
            if not path.exists():
                continue
            frame = pd.read_csv(path).fillna("")
            for _, row in frame.iterrows():
                store_id = str(row.get("store_node_id") or row.get("store_id") or row.get("restaurant_id") or "").strip()
                if store_id.startswith("store:"):
                    store_id = store_id.split(":", 1)[1]
                store_idx = self.store_idx_by_id.get(store_id)
                if store_idx is None:
                    continue
                meta = metadata.get(store_idx, {})
                lat = as_float(row.get("latitude") or row.get("lat"))
                lng = as_float(row.get("longitude") or row.get("lng"))
                price = as_float(row.get("median_price") or row.get("menu_price_median") or row.get("price_median"))
                if price is None:
                    prices = [as_float(row.get("price_min") or row.get("menu_price_min")), as_float(row.get("price_max") or row.get("menu_price_max"))]
                    vals = [value for value in prices if value is not None]
                    price = sum(vals) / len(vals) if vals else None
                rating = as_float(row.get("rating"))
                review_count = as_float(row.get("review_count"))
                if lat is not None and lng is not None:
                    meta["lat"] = lat
                    meta["lng"] = lng
                if price is not None:
                    meta["median_price"] = price
                if rating is not None:
                    meta["rating"] = rating
                if review_count is not None:
                    meta["review_count"] = review_count
                if meta:
                    metadata[store_idx] = meta
        return metadata

    def _load_store_payloads(self) -> dict[str, dict]:
        processed_dirs = [path for path in self.settings.paths.processed_root.iterdir() if path.is_dir()] if self.settings.paths.processed_root.exists() else []
        if not processed_dirs:
            return {}
        processed_dir = sorted(processed_dirs)[-1]
        path = processed_dir / "canonical_restaurants.csv"
        if not path.exists():
            return {}
        frame = pd.read_csv(path).fillna("")
        payloads = {}
        for _, row in frame.iterrows():
            restaurant_id = str(row.get("restaurant_id", "")).strip()
            if restaurant_id:
                payloads[restaurant_id] = {
                    "restaurant_id": restaurant_id,
                    "name": str(row.get("name", "")).strip(),
                    "latitude": as_float(row.get("latitude")),
                    "longitude": as_float(row.get("longitude")),
                    "rating": as_float(row.get("rating")) or 0.0,
                    "review_count": float(row.get("review_count") or 0),
                }
        return payloads

    def _query_example(self, query: str, rules: dict) -> QueryExample:
        food = rules.get("food") or query
        term_tokens = split_tokens(str(food).replace(" ", "|"))
        aspect_tokens = split_aspect_tokens("|".join(rules.get("priority", [])))
        price_range_id = _price_bucket(_as_int(rules.get("max_price"))) or ""
        return QueryExample(
            query_node_id="runtime_query",
            query_idx=-1,
            area_id=slugify(str(rules.get("location") or "")),
            time_slot_id=str(rules.get("time_constraint") or ""),
            term_tokens=term_tokens,
            aspect_tokens=aspect_tokens,
            price_range_id=price_range_id,
            query_lat=as_float(rules.get("query_lat")),
            query_lng=as_float(rules.get("query_lng")),
            distance_tolerance_m=float(rules.get("distance_tolerance_m") or 1500.0),
            pos_store_indices=[],
            pos_weights=[],
            pos_row_by_store={},
        )

    def _query_embedding(self, query: QueryExample) -> torch.Tensor:
        feature_ids = []
        if query.area_id:
            feature_ids.append(f"area:{query.area_id}")
        for token in query.term_tokens:
            feature_ids.extend([f"category:{token}", f"dish:{token}"])
        for token in query.aspect_tokens:
            feature_ids.extend([f"aspect:{token}", f"context:{token}"])
        indices = [self.node_id_to_idx[node_id] for node_id in feature_ids if node_id in self.node_id_to_idx]
        if indices:
            return self.node_embeddings[torch.tensor(indices, dtype=torch.long, device=self.device)].mean(dim=0)
        return self.node_embeddings[torch.tensor(self.store_indices, dtype=torch.long, device=self.device)].mean(dim=0)

    def recommend(self, query: str, rules: dict, top_k: int) -> list[dict]:
        q = self._query_example(query, rules)
        candidate_indices = list(self.store_indices)
        if not candidate_indices:
            return []
        with torch.no_grad():
            q_emb = self._query_embedding(q)
            candidate_tensor = torch.tensor(candidate_indices, dtype=torch.long, device=self.device)
            features = build_pair_features(q, candidate_indices, self.store_meta, self.store_categories).to(self.device)
            scores = score_pairs(q_emb, self.node_embeddings[candidate_tensor], features, self.scoring_head)
            k = min(max(top_k, 1), len(candidate_indices))
            top_scores, top_positions = torch.topk(scores, k=k)
        rows = []
        for score, position in zip(top_scores.tolist(), top_positions.tolist()):
            store_idx = candidate_indices[position]
            restaurant_id = self.store_id_by_idx.get(store_idx, "")
            payload = dict(self.store_payloads.get(restaurant_id, {"restaurant_id": restaurant_id, "name": restaurant_id}))
            distance_m = distance_meters(q.query_lat, q.query_lng, payload.get("latitude"), payload.get("longitude"))
            payload.update(
                {
                    "restaurant_id": restaurant_id,
                    "rgcn_score": round(float(score), 6),
                    "distance_m": round(distance_m, 1) if distance_m is not None else "",
                    "distance_km": distance_km(distance_m),
                    "reason": "R-GCN query ranker inference",
                }
            )
            rows.append(payload)
        return rows


class RGCNService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.model_config = load_active_model(settings.paths.models_root)
        self.ranker = None
        if self.model_config.get("artifact_path"):
            try:
                self.ranker = RGCNRuntimeRanker(settings, self.model_config)
            except Exception:
                self.ranker = None
        processed_dirs = [path for path in settings.paths.processed_root.iterdir() if path.is_dir()]
        self.processed_dir = sorted(processed_dirs)[-1] if processed_dirs else None
        self.scenarios = pd.read_csv(self.processed_dir / "scenario_features.csv").fillna("") if self.processed_dir and (self.processed_dir / "scenario_features.csv").exists() else pd.DataFrame()

    def recommend(self, query: str, rules: dict, top_k: int = 5) -> list[dict]:
        if self.ranker is not None:
            return self.ranker.recommend(query, rules, top_k)
        return []
