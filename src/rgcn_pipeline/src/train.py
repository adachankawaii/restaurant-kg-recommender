from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import torch
import torch.nn as nn
import torch.nn.functional as F

from .model import RGCN, score_edges
from .utils import ensure_dir, iter_batches, negative_sampling, set_seed, write_log


@dataclass
class TrainingConfig:
    epochs: int = 100
    emb_dim: int = 64
    lr: float = 0.01
    weight_decay: float = 1e-4
    batch_size: int = 4096
    dropout: float = 0.1
    patience: int = 20
    min_delta: float = 1e-4
    seed: int = 42
    eval_every: int = 1
    num_bases: int | None = None
    use_full_graph_for_saved_embeddings: bool = True
    grad_clip_norm: float = 1.0


def _to_device(edges: torch.Tensor, device: torch.device) -> torch.Tensor:
    return edges.to(device=device, dtype=torch.long, non_blocking=True)


def _pairwise_rank_loss(
    z: torch.Tensor,
    rel_emb: nn.Embedding,
    pos_edges: torch.Tensor,
    neg_edges: torch.Tensor,
    batch_size: int,
) -> torch.Tensor:
    total_loss = z.new_tensor(0.0)
    total_items = pos_edges.size(0)

    for start, end in iter_batches(total_items, batch_size):
        pos_score = score_edges(z, pos_edges[start:end], rel_emb)
        neg_score = score_edges(z, neg_edges[start:end], rel_emb)
        batch_loss = -F.logsigmoid(pos_score - neg_score).mean()
        total_loss = total_loss + batch_loss * (end - start)

    return total_loss / max(total_items, 1)


@torch.no_grad()
def evaluate_edges(
    z: torch.Tensor,
    rel_emb: nn.Embedding,
    pos_edges: torch.Tensor,
    neg_edges: torch.Tensor,
    batch_size: int,
) -> dict[str, float]:
    labels: list[torch.Tensor] = []
    probs: list[torch.Tensor] = []

    for edges, label_value in ((pos_edges, 1.0), (neg_edges, 0.0)):
        for start, end in iter_batches(edges.size(0), batch_size):
            batch_scores = score_edges(z, edges[start:end], rel_emb)
            batch_probs = torch.sigmoid(batch_scores)
            probs.append(batch_probs.detach().cpu())
            labels.append(torch.full_like(batch_probs, label_value, device="cpu"))

    y_prob = torch.cat(probs)
    y_true = torch.cat(labels)
    accuracy = ((y_prob > 0.5) == (y_true > 0.5)).float().mean().item()

    try:
        from sklearn.metrics import roc_auc_score

        auc = float(roc_auc_score(y_true.numpy(), y_prob.numpy()))
    except Exception:
        auc = float("nan")

    return {"acc": accuracy, "auc": auc}


def _plot_loss_curve(history: list[dict[str, float]], output_path: Path, log_path: Path) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        write_log(log_path, f"Skip loss plot: matplotlib unavailable ({exc})")
        return

    try:
        epochs = [row["epoch"] for row in history]
        losses = [row["loss"] for row in history]
        plt.figure(figsize=(8, 4.5))
        plt.plot(epochs, losses, label="train_loss")
        plt.xlabel("Epoch")
        plt.ylabel("Loss")
        plt.title("R-GCN Link Prediction Loss")
        plt.grid(True, alpha=0.25)
        plt.legend()
        plt.tight_layout()
        plt.savefig(output_path, dpi=140)
        plt.close()
    except Exception as exc:
        write_log(log_path, f"Skip loss plot: {exc}")


def train_model(data: dict[str, Any], output_dir: Path | str, config: TrainingConfig) -> dict[str, Any]:
    set_seed(config.seed)
    output_dir = ensure_dir(output_dir)
    log_path = output_dir / "logs.txt"
    log_path.write_text("", encoding="utf-8")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    write_log(log_path, f"Device: {device}")
    write_log(
        log_path,
        (
            f"Graph: nodes={data['num_nodes']}, relations={data['num_rels']}, "
            f"train_edges={data['train_edges'].size(0)}, val_edges={data['val_edges'].size(0)}, "
            f"test_edges={data['test_edges'].size(0)}"
        ),
    )

    model = RGCN(
        num_nodes=data["num_nodes"],
        num_rels=data["num_message_rels"],
        emb_dim=config.emb_dim,
        num_bases=config.num_bases,
        dropout=config.dropout,
    ).to(device)

    rel_emb = nn.Embedding(data["num_rels"], config.emb_dim).to(device)
    nn.init.xavier_uniform_(rel_emb.weight)

    optimizer = torch.optim.Adam(
        list(model.parameters()) + list(rel_emb.parameters()),
        lr=config.lr,
        weight_decay=config.weight_decay,
    )

    train_edge_index = _to_device(data["train_edge_index"], device)
    train_edge_type = _to_device(data["train_edge_type"], device)
    train_edges = _to_device(data["train_edges"], device)
    val_edges = _to_device(data["val_edges"], device)
    test_edges = _to_device(data["test_edges"], device)
    all_positive_edges_cpu = data["all_positive_edges"].cpu()

    val_neg = _to_device(
        negative_sampling(
            data["val_edges"],
            data["num_nodes"],
            existing_edges=all_positive_edges_cpu,
            seed=config.seed + 10_000,
        ),
        device,
    )
    test_neg = _to_device(
        negative_sampling(
            data["test_edges"],
            data["num_nodes"],
            existing_edges=all_positive_edges_cpu,
            seed=config.seed + 20_000,
        ),
        device,
    )

    best_metric = -float("inf")
    best_epoch = 0
    bad_epochs = 0
    history: list[dict[str, float]] = []
    checkpoint_path = output_dir / "best_model.pt"

    for epoch in range(1, config.epochs + 1):
        model.train()
        rel_emb.train()
        optimizer.zero_grad(set_to_none=True)

        z = model(train_edge_index, train_edge_type)
        train_neg = _to_device(
            negative_sampling(
                data["train_edges"],
                data["num_nodes"],
                existing_edges=all_positive_edges_cpu,
                seed=config.seed + epoch,
            ),
            device,
        )
        loss = _pairwise_rank_loss(z, rel_emb, train_edges, train_neg, config.batch_size)
        loss.backward()
        if config.grad_clip_norm > 0:
            torch.nn.utils.clip_grad_norm_(
                list(model.parameters()) + list(rel_emb.parameters()),
                config.grad_clip_norm,
            )
        optimizer.step()

        if epoch % config.eval_every != 0 and epoch != config.epochs:
            continue

        model.eval()
        rel_emb.eval()
        with torch.no_grad():
            z_eval = model(train_edge_index, train_edge_type)
            train_metrics = evaluate_edges(z_eval, rel_emb, train_edges, train_neg, config.batch_size)
            val_metrics = evaluate_edges(z_eval, rel_emb, val_edges, val_neg, config.batch_size)

        val_auc = val_metrics["auc"]
        monitor = val_auc if not math.isnan(val_auc) else val_metrics["acc"]
        row = {
            "epoch": float(epoch),
            "loss": float(loss.item()),
            "train_acc": float(train_metrics["acc"]),
            "val_acc": float(val_metrics["acc"]),
            "val_auc": float(val_auc),
        }
        history.append(row)

        write_log(
            log_path,
            (
                f"Epoch {epoch}\n"
                f"Loss: {loss.item():.4f}\n"
                f"Train Acc: {train_metrics['acc']:.4f}\n"
                f"Val Acc: {val_metrics['acc']:.4f}\n"
                f"AUC: {val_auc:.4f}"
            ),
        )

        if monitor > best_metric + config.min_delta:
            best_metric = monitor
            best_epoch = epoch
            bad_epochs = 0
            torch.save(
                {
                    "epoch": epoch,
                    "model_state": model.state_dict(),
                    "rel_emb_state": rel_emb.state_dict(),
                    "best_metric": best_metric,
                    "config": config.__dict__,
                    "idx_to_node_id": data["idx_to_node_id"],
                    "idx_to_rel": data["idx_to_rel"],
                },
                checkpoint_path,
            )
        else:
            bad_epochs += 1

        if config.patience > 0 and bad_epochs >= config.patience:
            write_log(log_path, f"Early stopping at epoch {epoch}. Best epoch: {best_epoch}")
            break

    if checkpoint_path.exists():
        checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=False)
        model.load_state_dict(checkpoint["model_state"])
        rel_emb.load_state_dict(checkpoint["rel_emb_state"])

    model.eval()
    rel_emb.eval()
    with torch.no_grad():
        z_test = model(train_edge_index, train_edge_type)
        test_metrics = evaluate_edges(z_test, rel_emb, test_edges, test_neg, config.batch_size)

    write_log(
        log_path,
        (
            "Final Test\n"
            f"Test Acc: {test_metrics['acc']:.4f}\n"
            f"Test AUC: {test_metrics['auc']:.4f}\n"
            f"Best Epoch: {best_epoch}"
        ),
    )

    with torch.no_grad():
        if config.use_full_graph_for_saved_embeddings:
            save_edge_index = _to_device(data["message_edge_index"], device)
            save_edge_type = _to_device(data["message_edge_type"], device)
        else:
            save_edge_index = train_edge_index
            save_edge_type = train_edge_type
        z_save = model(save_edge_index, save_edge_type).detach().cpu()

    embedding_path = output_dir / "embeddings.pt"
    torch.save(
        {
            "embeddings": z_save,
            "node_id_to_idx": data["node_id_to_idx"],
            "idx_to_node_id": data["idx_to_node_id"],
            "node_types": data["node_types"],
            "rel_to_idx": data["rel_to_idx"],
            "idx_to_rel": data["idx_to_rel"],
            "rel_embeddings": rel_emb.weight.detach().cpu(),
        },
        embedding_path,
    )
    write_log(log_path, f"Saved embeddings: {embedding_path}")

    _plot_loss_curve(history, output_dir / "loss_curve.png", log_path)
    return {
        "model": model,
        "relation_embedding": rel_emb,
        "history": history,
        "test_metrics": test_metrics,
        "embedding_path": embedding_path,
        "checkpoint_path": checkpoint_path,
        "log_path": log_path,
    }
