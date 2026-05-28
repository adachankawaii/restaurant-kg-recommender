from __future__ import annotations

import argparse
import copy
import math
import os
import random
import sys
from pathlib import Path

import torch
import torch.nn as nn
import torch.nn.functional as F
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent))
    from phase2_finetune import (  # type: ignore
        PAIR_FEATURE_DIM,
        InteractionScoringHead,
        Phase2Config,
        QueryExample,
        build_edge_index,
        build_candidate_store_indices,
        build_pair_features,
        build_queries_and_graph,
        build_store_category_map,
        evaluate,
        expand_embedding,
        expand_rgcn_conv,
        infer_checkpoint_num_relations,
        kg_auxiliary_loss,
        positive_order_loss,
        read_csv_rows,
        sample_scenario_negatives,
        sample_scenario_negatives_with_stats,
        score_pairs,
    )
    from src.data_loader import (  # type: ignore
        DEFAULT_EXCLUDED_RELATIONS,
        export_from_neo4j,
        export_graphrag_from_neo4j,
        load_graph_data,
        normalize_graph_csvs,
        summarize_graph_csvs,
    )
    from src.model import RGCN  # type: ignore
    from src.utils import ensure_dir, set_seed, write_log  # type: ignore
else:
    from .phase2_finetune import (
        PAIR_FEATURE_DIM,
        InteractionScoringHead,
        Phase2Config,
        QueryExample,
        build_edge_index,
        build_candidate_store_indices,
        build_pair_features,
        build_queries_and_graph,
        build_store_category_map,
        evaluate,
        expand_embedding,
        expand_rgcn_conv,
        infer_checkpoint_num_relations,
        kg_auxiliary_loss,
        positive_order_loss,
        read_csv_rows,
        sample_scenario_negatives,
        sample_scenario_negatives_with_stats,
        score_pairs,
    )
    from .src.data_loader import (
        DEFAULT_EXCLUDED_RELATIONS,
        export_from_neo4j,
        export_graphrag_from_neo4j,
        load_graph_data,
        normalize_graph_csvs,
        summarize_graph_csvs,
    )
    from .src.model import RGCN
    from .src.utils import ensure_dir, set_seed, write_log


PIPELINE_DIR = Path(__file__).resolve().parent
REPO_ROOT = PIPELINE_DIR.parent


def print_kg_summary(nodes_path: Path, edges_path: Path) -> None:
    summary = summarize_graph_csvs(nodes_path, edges_path)
    print(
        "R-GCN KG summary: "
        f"nodes={summary['nodes']}, edges={summary['edges']}, "
        f"communities={summary['communities']}, "
        f"community_reports={summary['community_reports']}, "
        f"extracted_entities={summary['extracted_entities']}, "
        f"extracted_relations={summary['extracted_relations']}, "
        f"in_community_edges={summary['in_community_edges']}, "
        f"has_report_edges={summary['has_report_edges']}, "
        f"restaurants_with_community={summary['restaurants_with_community']}"
    )


def parse_args() -> argparse.Namespace:
    load_dotenv(REPO_ROOT / ".env", override=False)
    load_dotenv(REPO_ROOT / ".env.graphrag", override=False)

    parser = argparse.ArgumentParser(description="Train R-GCN link prediction on the restaurant KG.")
    parser.add_argument("--nodes", type=Path, default=PIPELINE_DIR / "data" / "graphrag_nodes.csv")
    parser.add_argument("--edges", type=Path, default=PIPELINE_DIR / "data" / "graphrag_edges.csv")
    parser.add_argument("--outputs", type=Path, default=PIPELINE_DIR / "outputs_graphrag")

    parser.add_argument("--source-nodes", type=Path, default=REPO_ROOT / "kg_tables_all" / "kg_graph" / "nodes.csv")
    parser.add_argument("--source-edges", type=Path, default=REPO_ROOT / "kg_tables_all" / "kg_graph" / "edges.csv")
    parser.add_argument(
        "--kg-source",
        choices=("graphrag-neo4j", "legacy-csv", "legacy-neo4j"),
        default="graphrag-neo4j",
        help="Source KG used when preparing R-GCN CSVs.",
    )
    parser.add_argument("--prepare-data", action="store_true", help="Prepare KG CSVs into rgcn_pipeline/data.")
    parser.add_argument("--prepare-data-only", action="store_true", help="Prepare CSVs and exit before training.")

    parser.add_argument("--from-neo4j", action="store_true", help="Alias for --kg-source legacy-neo4j.")
    parser.add_argument("--from-graphrag-neo4j", action="store_true", help="Alias for --kg-source graphrag-neo4j.")
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j")))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "password123"))

    parser.add_argument("--include-metadata-relations", action="store_true")
    parser.add_argument("--add-reverse-edges", action="store_true", help="Add inverse edges for message passing.")
    parser.add_argument("--epochs", type=int, default=100)
    parser.add_argument("--emb-dim", type=int, default=64)
    parser.add_argument("--lr", type=float, default=0.01)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--dropout", type=float, default=0.1)
    parser.add_argument("--patience", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-bases", type=int, default=None)

    parser.add_argument(
        "--queries",
        type=Path,
        default=PIPELINE_DIR / "query_store_top5_merged.csv",
        help="Top-k query/store labels used for supervised ranking fine-tuning.",
    )
    parser.add_argument(
        "--store-metadata",
        type=Path,
        default=PIPELINE_DIR / "data" / "store_metadata.csv",
        help="Optional store metadata CSV with store_id/store_node_id, latitude, longitude and price columns.",
    )
    parser.add_argument("--skip-query-finetune", action="store_true")
    parser.add_argument("--query-epochs", type=int, default=50)
    parser.add_argument("--query-lr", type=float, default=0.005)
    parser.add_argument("--query-weight-decay", type=float, default=1e-4)
    parser.add_argument("--query-num-neg", type=int, default=10)
    parser.add_argument("--query-reg-lambda", type=float, default=0.05)
    parser.add_argument("--query-pos-order-alpha", type=float, default=0.0)
    parser.add_argument("--query-kg-aux-beta", type=float, default=0.05)
    parser.add_argument("--query-hard-negative-ratio", type=float, default=0.5)
    parser.add_argument("--query-model-hard-negative-ratio", type=float, default=0.25)
    parser.add_argument("--query-scorer-hidden-dim", type=int, default=128)
    parser.add_argument("--query-eval-ratio", type=float, default=0.2)
    parser.add_argument("--online-ratio", type=float, default=0.2)
    parser.add_argument("--online-epochs", type=int, default=10)
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument(
        "--relation-init",
        choices=("bert", "copy", "xavier"),
        default="bert",
        help="How to initialize query-only relation kernels before ranking fine-tune.",
    )
    parser.add_argument(
        "--relation-bert-model",
        default=os.getenv("RELATION_BERT_MODEL", os.getenv("EMBED_MODEL", "bkai-foundation-models/vietnamese-bi-encoder")),
        help="SentenceTransformer model used to match new query relations to existing KG relations.",
    )
    return parser.parse_args()


def _query_id(row: dict[str, str]) -> str:
    return row.get("query_node_id") or row.get("query_id") or ""


def split_query_rows(
    rows: list[dict[str, str]],
    *,
    eval_ratio: float,
    online_ratio: float,
    seed: int,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    query_ids = sorted({qid for row in rows if (qid := _query_id(row))})
    rng = random.Random(seed)
    rng.shuffle(query_ids)

    n_total = len(query_ids)
    n_online = max(1, int(round(n_total * online_ratio))) if n_total >= 3 and online_ratio > 0 else 0
    n_eval = max(1, int(round(n_total * eval_ratio))) if n_total >= 2 and eval_ratio > 0 else 0
    if n_online + n_eval >= n_total:
        n_online = max(0, min(n_online, n_total - 2))
        n_eval = max(1, min(n_eval, n_total - n_online - 1))

    online_ids = set(query_ids[:n_online])
    eval_ids = set(query_ids[n_online : n_online + n_eval])
    train_ids = set(query_ids[n_online + n_eval :])

    train_rows = [row for row in rows if _query_id(row) in train_ids]
    eval_rows = [row for row in rows if _query_id(row) in eval_ids]
    online_rows = [row for row in rows if _query_id(row) in online_ids]
    return train_rows, eval_rows, online_rows


def ranking_metrics(
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
    recalls: list[float] = []
    mrrs: list[float] = []
    ndcgs: list[float] = []

    for query in queries:
        positives = set(query.pos_store_indices)
        if not positives:
            continue

        candidate_indices = build_candidate_store_indices(query, store_indices, store_meta, store_categories)
        candidate_tensor = torch.tensor(candidate_indices, dtype=torch.long, device=device)
        features = build_pair_features(query, candidate_indices, store_meta, store_categories).to(device)
        scores = score_pairs(z[query.query_idx], z[candidate_tensor], features, scorer)
        k = min(topk, scores.numel())
        _, top_idx = torch.topk(scores, k=k)
        ranked = [candidate_indices[i] for i in top_idx.tolist()]

        gain_by_store = {
            store_idx: float(query.pos_weights[idx]) if idx < len(query.pos_weights) else 1.0
            for idx, store_idx in enumerate(query.pos_store_indices)
        }
        hits = [1.0 if store_idx in positives else 0.0 for store_idx in ranked]
        recalls.append(sum(hits) / max(len(positives), 1))

        first_hit = next((idx + 1 for idx, hit in enumerate(hits) if hit > 0), None)
        mrrs.append(0.0 if first_hit is None else 1.0 / first_hit)

        dcg = sum(gain_by_store.get(store_idx, 0.0) / math.log2(rank + 2) for rank, store_idx in enumerate(ranked))
        ideal_gains = sorted(gain_by_store.values(), reverse=True)[:k]
        idcg = sum(gain / math.log2(rank + 2) for rank, gain in enumerate(ideal_gains))
        ndcgs.append(dcg / idcg if idcg > 0 else 0.0)

    if not recalls:
        return {"recall@k": 0.0, "mrr@k": 0.0, "ndcg@k": 0.0}
    return {
        "recall@k": float(sum(recalls) / len(recalls)),
        "mrr@k": float(sum(mrrs) / len(mrrs)),
        "ndcg@k": float(sum(ndcgs) / len(ndcgs)),
    }


def train_query_ranker(
    *,
    model: nn.Module,
    edge_index: torch.Tensor,
    edge_type: torch.Tensor,
    train_queries: list[QueryExample],
    eval_queries: list[QueryExample],
    store_indices: list[int],
    store_meta: dict[int, dict[str, float]],
    store_categories: dict[int, set[str]],
    kg_edges: torch.Tensor,
    base_num_nodes: int,
    pretrained_store_z: torch.Tensor | None,
    config: Phase2Config,
    log_path: Path,
    log_prefix: str,
    scoring_head: InteractionScoringHead | None = None,
    kg_rel_emb: nn.Embedding | None = None,
) -> tuple[nn.Module, InteractionScoringHead, nn.Embedding, dict[str, float]]:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    edge_index = edge_index.to(device=device, dtype=torch.long)
    edge_type = edge_type.to(device=device, dtype=torch.long)

    if scoring_head is None:
        scoring_head = InteractionScoringHead(
            config.emb_dim,
            pair_feature_dim=PAIR_FEATURE_DIM,
            hidden_dim=config.scorer_hidden_dim,
        )
    else:
        scoring_head = copy.deepcopy(scoring_head)
    scoring_head = scoring_head.to(device)

    if kg_rel_emb is None:
        kg_rel_emb = nn.Embedding(max(int(kg_edges[:, 1].max().item()) + 1 if kg_edges.numel() else 1, 1), config.emb_dim)
        nn.init.xavier_uniform_(kg_rel_emb.weight)
    else:
        kg_rel_emb = copy.deepcopy(kg_rel_emb)
    kg_rel_emb = kg_rel_emb.to(device)

    pretrained_store_z = pretrained_store_z.to(device) if pretrained_store_z is not None else None
    kg_edges = kg_edges.to(device=device, dtype=torch.long)
    optimizer = torch.optim.Adam(
        list(model.parameters()) + list(scoring_head.parameters()) + list(kg_rel_emb.parameters()),
        lr=config.lr,
        weight_decay=config.weight_decay,
    )
    best_epoch = 0
    best_key = (-1.0, -1.0, -1.0)
    best_metrics: dict[str, float] | None = None
    best_model_state: dict[str, torch.Tensor] | None = None
    best_scoring_state: dict[str, torch.Tensor] | None = None
    best_kg_rel_state: dict[str, torch.Tensor] | None = None

    for epoch in range(1, config.epochs + 1):
        rng = random.Random(config.seed + epoch)
        model.train()
        scoring_head.train()
        optimizer.zero_grad(set_to_none=True)
        z = model(edge_index, edge_type)

        total_loss = z.new_tensor(0.0)
        total_items = 0
        pos_neg_loss_sum = 0.0
        pos_order_loss_sum = 0.0
        hard_negative_count = 0
        total_negative_count = 0
        pos_order_eligible = 0
        for query in train_queries:
            if not query.pos_store_indices:
                continue
            positives = set(query.pos_store_indices)
            if len(query.pos_store_indices) >= 2:
                pos_order_eligible += 1
            candidate_indices = build_candidate_store_indices(query, store_indices, store_meta, store_categories)
            with torch.no_grad():
                candidate_tensor = torch.tensor(candidate_indices, dtype=torch.long, device=device)
                candidate_features = build_pair_features(query, candidate_indices, store_meta, store_categories).to(device)
                candidate_scores = score_pairs(z[query.query_idx], z[candidate_tensor], candidate_features, scoring_head)
                model_scores = {
                    store_idx: float(candidate_scores[i].detach().cpu())
                    for i, store_idx in enumerate(candidate_indices)
                }
            neg_indices, neg_stats = sample_scenario_negatives_with_stats(
                query,
                candidate_indices,
                positives,
                config.num_neg * len(query.pos_store_indices),
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
            pos_scores = score_pairs(
                z[query.query_idx],
                z[torch.tensor(query.pos_store_indices, device=device)],
                pos_features,
                scoring_head,
            )
            neg_scores = score_pairs(
                z[query.query_idx],
                z[torch.tensor(neg_indices, device=device)],
                neg_features,
                scoring_head,
            ).view(len(query.pos_store_indices), -1)

            weights = torch.tensor(query.pos_weights, dtype=torch.float32, device=device)
            pair_loss = -F.logsigmoid(pos_scores.unsqueeze(1) - neg_scores).mean(dim=1)
            pos_order = positive_order_loss(pos_scores, weights)
            pos_neg_loss = (weights * pair_loss).mean()
            total_loss = total_loss + pos_neg_loss + config.pos_order_alpha * pos_order
            pos_neg_loss_sum += float(pos_neg_loss.detach().cpu())
            pos_order_loss_sum += float(pos_order.detach().cpu())
            total_items += 1

        if total_items == 0:
            raise RuntimeError(f"{log_prefix}: no training pairs found.")

        loss = total_loss / total_items
        if pretrained_store_z is not None and config.reg_lambda > 0:
            store_idx_tensor = torch.tensor(store_indices, device=device)
            loss = loss + config.reg_lambda * F.mse_loss(z[store_idx_tensor], pretrained_store_z[store_idx_tensor])
        if config.kg_aux_beta > 0:
            loss = loss + config.kg_aux_beta * kg_auxiliary_loss(
                z,
                kg_rel_emb,
                kg_edges,
                num_nodes=base_num_nodes,
                batch_size=config.kg_aux_batch_size,
                rng=rng,
            )

        loss.backward()
        torch.nn.utils.clip_grad_norm_(list(model.parameters()) + list(scoring_head.parameters()), 1.0)
        optimizer.step()

        if epoch == 1 or epoch == config.epochs or epoch % max(config.eval_every, 1) == 0:
            metrics = ranking_metrics(
                model,
                edge_index,
                edge_type,
                eval_queries,
                store_indices,
                store_meta,
                store_categories,
                scoring_head,
                config.topk,
            )
            write_log(
                log_path,
                (
                    f"{log_prefix} Epoch {epoch} Loss: {loss.item():.4f} "
                    f"PosNeg: {pos_neg_loss_sum / max(total_items, 1):.4f} "
                    f"PosOrder: {pos_order_loss_sum / max(total_items, 1):.4f} "
                    f"HardNeg: {hard_negative_count}/{max(total_negative_count, 1)} "
                    f"PosOrderEligible: {pos_order_eligible}/{len(train_queries)} "
                    f"Recall@{config.topk}: {metrics['recall@k']:.4f} "
                    f"MRR@{config.topk}: {metrics['mrr@k']:.4f} "
                    f"nDCG@{config.topk}: {metrics['ndcg@k']:.4f}"
                ),
            )
            metric_key = (metrics["ndcg@k"], metrics["recall@k"], metrics["mrr@k"])
            if metric_key > best_key:
                best_key = metric_key
                best_epoch = epoch
                best_metrics = metrics
                best_model_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
                best_scoring_state = {k: v.detach().cpu().clone() for k, v in scoring_head.state_dict().items()}
                best_kg_rel_state = {k: v.detach().cpu().clone() for k, v in kg_rel_emb.state_dict().items()}

    if best_model_state is not None and best_scoring_state is not None and best_kg_rel_state is not None and best_metrics is not None:
        model.load_state_dict({k: v.to(device) for k, v in best_model_state.items()})
        scoring_head.load_state_dict({k: v.to(device) for k, v in best_scoring_state.items()})
        kg_rel_emb.load_state_dict({k: v.to(device) for k, v in best_kg_rel_state.items()})
        final_metrics = best_metrics
        write_log(log_path, f"{log_prefix} Best epoch: {best_epoch}")
    else:
        final_metrics = ranking_metrics(
            model,
            edge_index,
            edge_type,
            eval_queries,
            store_indices,
            store_meta,
            store_categories,
            scoring_head,
            config.topk,
        )
    return model, scoring_head, kg_rel_emb, final_metrics


def _relation_text(name: str) -> str:
    aliases = {
        "HAS_CATEGORY": "restaurant has category cuisine food type",
        "HAS_PRIMARY_CATEGORY": "restaurant primary category cuisine food type",
        "IN_AREA": "restaurant located in area district city",
        "LOCATED_IN": "restaurant located in area district city",
        "HAS_PRICE_BAND": "restaurant has price band budget",
        "HAS_CONTEXT_TAG": "restaurant has context tag aspect preference",
        "HAS_ATTRIBUTE": "restaurant has aspect attribute sentiment",
        "HAS_ATMOSPHERE": "restaurant has atmosphere space context",
        "QUERY_HAS_AREA": "query asks for area district location",
        "QUERY_TIME_SLOT": "query asks for time slot context",
        "QUERY_PRICE_RANGE": "query asks for price range budget",
        "QUERY_HAS_TERM": "query asks for category cuisine food term",
        "QUERY_HAS_ASPECT": "query asks for aspect attribute preference",
    }
    return aliases.get(name, name.replace("_", " ").lower())


def _copy_relation_kernel(conv: nn.Module, dst_idx: int, src_idx: int) -> bool:
    if hasattr(conv, "comp") and isinstance(conv.comp, torch.Tensor):
        if dst_idx < conv.comp.data.size(0) and src_idx < conv.comp.data.size(0):
            conv.comp.data[dst_idx].copy_(conv.comp.data[src_idx])
            return True
    if hasattr(conv, "weight") and isinstance(conv.weight, torch.Tensor) and conv.weight.dim() == 3:
        if dst_idx < conv.weight.data.size(0) and src_idx < conv.weight.data.size(0):
            conv.weight.data[dst_idx].copy_(conv.weight.data[src_idx])
            return True
    return False


def _semantic_relation_matches(
    base_rel_names: list[str],
    query_rel_names: list[str],
    *,
    mode: str,
    model_name: str,
    log_path: Path,
) -> dict[str, str]:
    if not query_rel_names:
        return {}
    if mode == "xavier":
        write_log(log_path, "Query relation init: xavier for all new relation kernels.")
        return {}
    if mode == "copy":
        fallback = {
            "QUERY_HAS_AREA": "IN_AREA" if "IN_AREA" in base_rel_names else "LOCATED_IN",
            "QUERY_TIME_SLOT": "HAS_CONTEXT_TAG",
            "QUERY_PRICE_RANGE": "HAS_PRICE_BAND",
            "QUERY_HAS_TERM": "HAS_CATEGORY" if "HAS_CATEGORY" in base_rel_names else "HAS_PRIMARY_CATEGORY",
            "QUERY_HAS_ASPECT": "HAS_ATTRIBUTE" if "HAS_ATTRIBUTE" in base_rel_names else "HAS_CONTEXT_TAG",
        }
        matches = {q: src for q, src in fallback.items() if q in query_rel_names and src in base_rel_names}
        write_log(log_path, "Query relation init: lexical copy " + str(matches))
        return matches

    try:
        from sentence_transformers import SentenceTransformer
        import numpy as np

        encoder = SentenceTransformer(model_name)
        base_texts = [_relation_text(name) for name in base_rel_names]
        query_texts = [_relation_text(name) for name in query_rel_names]
        base_vec = encoder.encode(base_texts, normalize_embeddings=True)
        query_vec = encoder.encode(query_texts, normalize_embeddings=True)
        sim = np.asarray(query_vec) @ np.asarray(base_vec).T
        matches = {}
        for row_idx, query_name in enumerate(query_rel_names):
            best_idx = int(np.argmax(sim[row_idx]))
            matches[query_name] = base_rel_names[best_idx]
        write_log(log_path, f"Query relation init: BERT semantic copy with {model_name}: {matches}")
        return matches
    except Exception as exc:
        write_log(log_path, f"Query relation init: BERT unavailable ({exc}); falling back to lexical copy.")
        return _semantic_relation_matches(
            base_rel_names,
            query_rel_names,
            mode="copy",
            model_name=model_name,
            log_path=log_path,
        )


def initialize_query_relation_kernels(
    model: nn.Module,
    *,
    idx_to_rel: list[str],
    base_num_rels: int,
    full_message_rels: int,
    mode: str,
    model_name: str,
    log_path: Path,
) -> None:
    query_rel_names = idx_to_rel[base_num_rels:]
    base_rel_names = idx_to_rel[:base_num_rels]
    rel_matches = _semantic_relation_matches(
        base_rel_names,
        query_rel_names,
        mode=mode,
        model_name=model_name,
        log_path=log_path,
    )
    if not rel_matches:
        return

    query_rel_count = len(query_rel_names)
    copied = []
    for query_name, source_name in rel_matches.items():
        if query_name not in idx_to_rel or source_name not in idx_to_rel:
            continue
        query_offset = idx_to_rel.index(query_name) - base_num_rels
        source_idx = idx_to_rel.index(source_name)
        dst_forward = base_num_rels * 2 + query_offset
        dst_reverse = base_num_rels * 2 + query_rel_count + query_offset
        src_forward = source_idx
        src_reverse = base_num_rels + source_idx
        for conv in (model.conv1, model.conv2):
            _copy_relation_kernel(conv, dst_forward, src_forward)
            if dst_reverse < full_message_rels:
                _copy_relation_kernel(conv, dst_reverse, src_reverse)
        copied.append(f"{query_name}<-{source_name}")
    if copied:
        write_log(log_path, "Initialized query relation kernels: " + ", ".join(copied))


def run_supervised_query_pipeline(args: argparse.Namespace, data: dict[str, object], checkpoint_path: Path) -> None:
    output_dir = ensure_dir(args.outputs)
    log_path = output_dir / "pipeline_logs.txt"
    write_log(log_path, "\n=== Stage 2: supervised query-store ranking ===")

    query_rows = read_csv_rows(args.queries)
    store_metadata_rows = read_csv_rows(args.store_metadata) if args.store_metadata.exists() else []
    train_rows, eval_rows, online_rows = split_query_rows(
        query_rows,
        eval_ratio=args.query_eval_ratio,
        online_ratio=args.online_ratio,
        seed=args.seed,
    )
    stage_rows = train_rows + eval_rows + online_rows
    write_log(
        log_path,
        (
            f"Query split: train_rows={len(train_rows)}, eval_rows={len(eval_rows)}, "
            f"online_rows={len(online_rows)}"
        ),
    )
    if store_metadata_rows:
        write_log(log_path, f"Loaded store metadata rows: {len(store_metadata_rows)} from {args.store_metadata}")

    node_id_to_idx = dict(data["node_id_to_idx"])
    idx_to_node_id = list(data["idx_to_node_id"])
    node_types = list(data["node_types"])
    rel_to_idx = dict(data["rel_to_idx"])
    idx_to_rel = list(data["idx_to_rel"])

    all_queries, query_edges, store_meta = build_queries_and_graph(
        stage_rows,
        node_id_to_idx,
        idx_to_node_id,
        node_types,
        rel_to_idx,
        idx_to_rel,
        store_metadata_rows=store_metadata_rows,
    )
    write_log(
        log_path,
        (
            f"Scenario graph: query_nodes={len(all_queries)}, query_feature_edges={len(query_edges)}, "
            "query_store_label_edges=0, reverse_edges=enabled"
        ),
    )
    query_by_id = {q.query_node_id: q for q in all_queries}
    train_queries = [query_by_id[qid] for qid in sorted({_query_id(row) for row in train_rows}) if qid in query_by_id]
    eval_queries = [query_by_id[qid] for qid in sorted({_query_id(row) for row in eval_rows}) if qid in query_by_id]
    online_queries = [query_by_id[qid] for qid in sorted({_query_id(row) for row in online_rows}) if qid in query_by_id]
    if not train_queries or not eval_queries:
        raise RuntimeError("Need at least one train query and one eval query for stage 2.")

    base_num_nodes = int(data["num_nodes"])
    base_num_rels = int(data["num_rels"])
    full_num_nodes = len(idx_to_node_id)
    full_num_rels = len(idx_to_rel)
    query_rel_count = max(full_num_rels - base_num_rels, 0)
    full_message_rels = base_num_rels * 2 + query_rel_count * 2

    base_triples = data["all_positive_edges"]
    base_edge_index, base_edge_type = build_edge_index(base_triples, base_num_rels, full_num_rels, True)
    if query_edges:
        query_triples = torch.tensor(query_edges, dtype=torch.long)
        query_edge_index, query_edge_type = build_edge_index(query_triples, base_num_rels, full_num_rels, True)
        edge_index = torch.cat([base_edge_index, query_edge_index], dim=1)
        edge_type = torch.cat([base_edge_type, query_edge_type], dim=0)
    else:
        edge_index, edge_type = base_edge_index, base_edge_type

    store_categories = build_store_category_map(base_triples, idx_to_node_id, rel_to_idx)
    store_indices = [idx for idx, node_type in enumerate(node_types) if node_type == "Store"]
    if not store_indices:
        raise RuntimeError("No Store nodes found in prepared KG.")

    checkpoint = torch.load(checkpoint_path, map_location="cpu", weights_only=False)
    ckpt_config = checkpoint.get("config", {})
    emb_dim = int(ckpt_config.get("emb_dim", args.emb_dim))
    num_bases = ckpt_config.get("num_bases", args.num_bases)
    ckpt_message_rels = infer_checkpoint_num_relations(checkpoint["model_state"]) or int(data["num_message_rels"])
    model = RGCN(
        num_nodes=base_num_nodes,
        num_rels=ckpt_message_rels,
        emb_dim=emb_dim,
        num_bases=num_bases,
        dropout=args.dropout,
    )
    model.load_state_dict(checkpoint["model_state"], strict=True)
    model.emb = expand_embedding(model.emb, full_num_nodes)
    expand_rgcn_conv(model.conv1, full_message_rels)
    expand_rgcn_conv(model.conv2, full_message_rels)
    initialize_query_relation_kernels(
        model,
        idx_to_rel=idx_to_rel,
        base_num_rels=base_num_rels,
        full_message_rels=full_message_rels,
        mode=args.relation_init,
        model_name=args.relation_bert_model,
        log_path=log_path,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    with torch.no_grad():
        model = model.to(device)
        base_edge_index_device = base_edge_index.to(device=device, dtype=torch.long)
        base_edge_type_device = base_edge_type.to(device=device, dtype=torch.long)
        pretrained_store_z = model(base_edge_index_device, base_edge_type_device).detach()

    phase2_config = Phase2Config(
        epochs=args.query_epochs,
        lr=args.query_lr,
        weight_decay=args.query_weight_decay,
        num_neg=args.query_num_neg,
        eval_every=max(1, min(5, args.query_epochs)),
        reg_lambda=args.query_reg_lambda,
        seed=args.seed,
        add_reverse_edges=True,
        topk=args.topk,
        emb_dim=emb_dim,
        dropout=args.dropout,
        num_bases=num_bases,
        scorer_hidden_dim=args.query_scorer_hidden_dim,
        pos_order_alpha=args.query_pos_order_alpha,
        kg_aux_beta=args.query_kg_aux_beta,
        hard_negative_ratio=args.query_hard_negative_ratio,
        model_hard_negative_ratio=args.query_model_hard_negative_ratio,
    )
    write_log(
        log_path,
        (
            f"Stage2 config: pos_order_alpha={phase2_config.pos_order_alpha}, "
            f"hard_negative_ratio={phase2_config.hard_negative_ratio}, "
            f"model_hard_negative_ratio={phase2_config.model_hard_negative_ratio}, "
            f"kg_aux_beta={phase2_config.kg_aux_beta}"
        ),
    )
    model, scoring_head, kg_rel_emb, eval_metrics = train_query_ranker(
        model=model,
        edge_index=edge_index,
        edge_type=edge_type,
        train_queries=train_queries,
        eval_queries=eval_queries,
        store_indices=store_indices,
        store_meta=store_meta,
        store_categories=store_categories,
        kg_edges=base_triples,
        base_num_nodes=base_num_nodes,
        pretrained_store_z=pretrained_store_z,
        config=phase2_config,
        log_path=log_path,
        log_prefix="Stage2",
    )
    write_log(
        log_path,
        (
            f"Stage2 Final Recall@{args.topk}: {eval_metrics['recall@k']:.4f}, "
            f"MRR@{args.topk}: {eval_metrics['mrr@k']:.4f}, "
            f"nDCG@{args.topk}: {eval_metrics['ndcg@k']:.4f}"
        ),
    )

    stage2_path = output_dir / "query_ranker.pt"
    torch.save(
        {
            "model_state": model.state_dict(),
            "scoring_head_state": scoring_head.state_dict(),
            "kg_rel_emb_state": kg_rel_emb.state_dict(),
            "node_id_to_idx": node_id_to_idx,
            "idx_to_node_id": idx_to_node_id,
            "node_types": node_types,
            "rel_to_idx": rel_to_idx,
            "idx_to_rel": idx_to_rel,
            "config": phase2_config.__dict__,
        },
        stage2_path,
    )
    write_log(log_path, f"Saved query ranker: {stage2_path}")

    if not online_queries:
        write_log(log_path, "Online fine-tuning eval skipped: no online query split.")
        return

    write_log(log_path, "\n=== Stage 3: online fine-tuning eval on new queries ===")
    before_online = ranking_metrics(
        model,
        edge_index.to(device=device, dtype=torch.long),
        edge_type.to(device=device, dtype=torch.long),
        online_queries,
        store_indices,
        store_meta,
        store_categories,
        scoring_head,
        args.topk,
    )
    write_log(
        log_path,
        (
            f"Online Before Recall@{args.topk}: {before_online['recall@k']:.4f}, "
            f"MRR@{args.topk}: {before_online['mrr@k']:.4f}, "
            f"nDCG@{args.topk}: {before_online['ndcg@k']:.4f}"
        ),
    )

    online_model = copy.deepcopy(model).to("cpu")
    online_config = copy.copy(phase2_config)
    online_config.epochs = args.online_epochs
    online_config.lr = args.query_lr * 0.5
    online_config.seed = args.seed + 99_000
    online_model, online_scorer, online_kg_rel_emb, after_online = train_query_ranker(
        model=online_model,
        edge_index=edge_index,
        edge_type=edge_type,
        train_queries=online_queries,
        eval_queries=online_queries,
        store_indices=store_indices,
        store_meta=store_meta,
        store_categories=store_categories,
        kg_edges=base_triples,
        base_num_nodes=base_num_nodes,
        pretrained_store_z=pretrained_store_z,
        config=online_config,
        log_path=log_path,
        log_prefix="OnlineFT",
        scoring_head=scoring_head,
        kg_rel_emb=kg_rel_emb,
    )
    write_log(
        log_path,
        (
            f"Online After Recall@{args.topk}: {after_online['recall@k']:.4f}, "
            f"MRR@{args.topk}: {after_online['mrr@k']:.4f}, "
            f"nDCG@{args.topk}: {after_online['ndcg@k']:.4f}"
        ),
    )
    torch.save(
        {
            "model_state": online_model.state_dict(),
            "scoring_head_state": online_scorer.state_dict(),
            "kg_rel_emb_state": online_kg_rel_emb.state_dict(),
            "online_before": before_online,
            "online_after": after_online,
            "config": online_config.__dict__,
        },
        output_dir / "query_ranker_online_eval.pt",
    )


def main() -> int:
    args = parse_args()
    excluded = set() if args.include_metadata_relations else DEFAULT_EXCLUDED_RELATIONS

    if args.from_neo4j:
        args.kg_source = "legacy-neo4j"
        args.prepare_data = True
    if args.from_graphrag_neo4j:
        args.kg_source = "graphrag-neo4j"
        args.prepare_data = True
    if args.prepare_data_only:
        args.prepare_data = True

    if args.prepare_data or not args.nodes.exists() or not args.edges.exists():
        if args.kg_source == "graphrag-neo4j":
            print("Preparing R-GCN CSVs from GraphRAG Neo4j KG...")
            export_graphrag_from_neo4j(
                args.neo4j_uri,
                args.neo4j_user,
                args.neo4j_password,
                args.nodes,
                args.edges,
                exclude_relations=excluded,
            )
        elif args.kg_source == "legacy-neo4j":
            print("Preparing R-GCN CSVs from legacy Neo4j KG...")
            export_from_neo4j(args.neo4j_uri, args.neo4j_user, args.neo4j_password, args.nodes, args.edges)
        else:
            print("Preparing R-GCN CSVs from legacy KG graph files...")
            normalize_graph_csvs(
                args.source_nodes,
                args.source_edges,
                args.nodes,
                args.edges,
                exclude_relations=excluded,
            )
        print(f"Prepared: {args.nodes}")
        print(f"Prepared: {args.edges}")

    if args.nodes.exists() and args.edges.exists():
        print_kg_summary(args.nodes, args.edges)

    if args.prepare_data_only:
        return 0

    data = load_graph_data(
        args.nodes,
        args.edges,
        seed=args.seed,
        exclude_relations=excluded,
        add_reverse_edges=args.add_reverse_edges,
    )

    if __package__ in {None, ""}:
        from src.train import TrainingConfig, train_model  # type: ignore
    else:
        from .src.train import TrainingConfig, train_model

    config = TrainingConfig(
        epochs=args.epochs,
        emb_dim=args.emb_dim,
        lr=args.lr,
        weight_decay=args.weight_decay,
        batch_size=args.batch_size,
        dropout=args.dropout,
        patience=args.patience,
        seed=args.seed,
        num_bases=args.num_bases,
    )
    train_result = train_model(data, args.outputs, config)

    if not args.skip_query_finetune:
        if not args.queries.exists():
            raise FileNotFoundError(f"Query CSV not found: {args.queries}")
        run_supervised_query_pipeline(args, data, Path(train_result["checkpoint_path"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
