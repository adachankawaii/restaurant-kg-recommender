from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F

try:
    from torch_geometric.nn import RGCNConv
except ImportError:  # pragma: no cover - handled at runtime with a clear error
    RGCNConv = None


class SimpleRGCNConv(nn.Module):
    """Small PyTorch fallback for environments without PyG.

    It follows the R-GCN idea of relation-specific transforms and mean
    aggregation. PyG's ``RGCNConv`` is still used automatically when installed.
    """

    def __init__(self, in_channels: int, out_channels: int, num_relations: int, num_bases: int | None = None):
        super().__init__()
        self.in_channels = in_channels
        self.out_channels = out_channels
        self.num_relations = num_relations
        self.weight = nn.Parameter(torch.empty(num_relations, in_channels, out_channels))
        self.root = nn.Linear(in_channels, out_channels, bias=False)
        self.bias = nn.Parameter(torch.zeros(out_channels))
        self.reset_parameters()

    def reset_parameters(self) -> None:
        nn.init.xavier_uniform_(self.weight)
        nn.init.xavier_uniform_(self.root.weight)
        nn.init.zeros_(self.bias)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, edge_type: torch.Tensor) -> torch.Tensor:
        out = x.new_zeros((x.size(0), self.out_channels))
        src_all, dst_all = edge_index

        for rel_idx in range(self.num_relations):
            mask = edge_type == rel_idx
            if not bool(mask.any()):
                continue
            src = src_all[mask]
            dst = dst_all[mask]
            msg = x[src] @ self.weight[rel_idx]
            out.index_add_(0, dst, msg)

        degree = x.new_zeros((x.size(0),))
        degree.index_add_(0, dst_all, torch.ones_like(dst_all, dtype=x.dtype))
        out = out / degree.clamp_min(1.0).unsqueeze(1)
        return out + self.root(x) + self.bias


class RGCN(nn.Module):
    def __init__(
        self,
        num_nodes: int,
        num_rels: int,
        emb_dim: int = 64,
        num_bases: int | None = None,
        dropout: float = 0.1,
    ):
        super().__init__()

        self.emb = nn.Embedding(num_nodes, emb_dim)
        conv_cls = RGCNConv if RGCNConv is not None else SimpleRGCNConv
        self.conv1 = conv_cls(emb_dim, emb_dim, num_rels, num_bases=num_bases)
        self.conv2 = conv_cls(emb_dim, emb_dim, num_rels, num_bases=num_bases)
        self.dropout = nn.Dropout(dropout)
        nn.init.xavier_uniform_(self.emb.weight)

    def forward(self, edge_index: torch.Tensor, edge_type: torch.Tensor) -> torch.Tensor:
        x = self.emb.weight
        x = self.conv1(x, edge_index, edge_type)
        x = F.relu(x)
        x = self.dropout(x)
        x = self.conv2(x, edge_index, edge_type)
        return x


def score(
    z: torch.Tensor,
    src: torch.Tensor,
    rel: torch.Tensor,
    dst: torch.Tensor,
    rel_emb: nn.Embedding | torch.Tensor,
) -> torch.Tensor:
    """DistMult score for ``(src, relation, dst)`` triples."""
    relation_z = rel_emb(rel) if isinstance(rel_emb, nn.Embedding) else rel_emb[rel]
    return (z[src] * relation_z * z[dst]).sum(dim=1)


def score_edges(
    z: torch.Tensor,
    edges: torch.Tensor,
    rel_emb: nn.Embedding | torch.Tensor,
) -> torch.Tensor:
    return score(z, edges[:, 0], edges[:, 1], edges[:, 2], rel_emb)
