from __future__ import annotations

import random
from pathlib import Path
from typing import Iterable

import numpy as np
import torch


def set_seed(seed: int) -> None:
    """Fix RNG state for reproducible splits, negatives, and training."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def ensure_dir(path: Path | str) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_log(log_path: Path | str, message: str, *, print_console: bool = True) -> None:
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as file:
        file.write(message.rstrip() + "\n")
    if print_console:
        print(message)


def edge_tensor_to_set(edges: torch.Tensor) -> set[tuple[int, int, int]]:
    if edges.numel() == 0:
        return set()
    return {tuple(int(v) for v in row) for row in edges.detach().cpu().tolist()}


def negative_sampling(
    positive_edges: torch.Tensor,
    num_nodes: int,
    existing_edges: torch.Tensor | Iterable[tuple[int, int, int]] | None = None,
    num_samples: int | None = None,
    seed: int | None = None,
) -> torch.Tensor:
    """Generate relation-aware corrupted triples that are not known positives.

    Positive and negative triples have shape ``[num_edges, 3]`` with columns
    ``src_idx, relation_idx, dst_idx``. The relation is kept from a positive
    edge while either source or destination is corrupted.
    """
    positive_cpu = positive_edges.detach().cpu().long()
    if positive_cpu.numel() == 0:
        return torch.empty((0, 3), dtype=torch.long)

    target_count = int(num_samples or positive_cpu.size(0))
    rng = random.Random(seed)

    if existing_edges is None:
        existing = edge_tensor_to_set(positive_cpu)
    elif isinstance(existing_edges, torch.Tensor):
        existing = edge_tensor_to_set(existing_edges)
    else:
        existing = set(existing_edges)

    negatives: list[tuple[int, int, int]] = []
    negatives_seen: set[tuple[int, int, int]] = set()
    positive_rows = positive_cpu.tolist()
    max_attempts = max(target_count * 80, 1000)
    attempts = 0

    while len(negatives) < target_count and attempts < max_attempts:
        attempts += 1
        src, rel, dst = positive_rows[len(negatives) % len(positive_rows)]

        if rng.random() < 0.5:
            cand_src = rng.randrange(num_nodes)
            cand_dst = int(dst)
        else:
            cand_src = int(src)
            cand_dst = rng.randrange(num_nodes)

        triple = (cand_src, int(rel), cand_dst)
        if triple in existing or triple in negatives_seen:
            continue
        negatives.append(triple)
        negatives_seen.add(triple)

    # Very dense graphs can make strict rejection slow. Fill the remainder with
    # valid random corruptions, still avoiding known positives where possible.
    while len(negatives) < target_count:
        src, rel, dst = positive_rows[len(negatives) % len(positive_rows)]
        cand_src = rng.randrange(num_nodes)
        cand_dst = rng.randrange(num_nodes)
        triple = (cand_src, int(rel), cand_dst)
        if triple in existing:
            continue
        negatives.append(triple)

    return torch.tensor(negatives, dtype=torch.long)


def iter_batches(num_items: int, batch_size: int):
    batch_size = max(1, int(batch_size))
    for start in range(0, num_items, batch_size):
        end = min(start + batch_size, num_items)
        yield start, end

