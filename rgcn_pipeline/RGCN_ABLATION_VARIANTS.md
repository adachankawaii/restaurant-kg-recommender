# R-GCN Ablation Variants

This note describes the current ablation setup against the `full` model.

Evaluation uses the merged scenario file built from:

- `rgcn_pipeline/user_scenarios_70_befood_bachkhoa.csv`
- `rgcn_pipeline/data/user_scenarios_phase2_top5.csv`

The KG graph and store metadata are exported directly from Neo4j before the run.
Metrics are reported on the eval split with `topk=5`: `nDCG@5`, `MRR@5`, and `HR@5`.

## Full

`full` is the reference model.

It does all of the following:

- Trains phase 1 R-GCN link prediction on the Neo4j KG snapshot.
- Adds query nodes and query feature edges in phase 2.
- Initializes query relation kernels from semantic/lexical relation matching.
- Trains phase 2 representation and the ranking scoring head with query-store labels.
- Uses hard negative sampling:
  - metadata hard negatives
  - model-score hard negatives
- Keeps KG auxiliary loss during phase 2.

Latest result:

| Variant | nDCG@5 | MRR@5 | HR@5 |
|---|---:|---:|---:|
| `full` | 0.6463 | 1.0000 | 1.0000 |

## `phase2_repr_only`

This variant tests whether the phase 2 ranking loss is necessary.

Compared with `full`, it still:

- Trains phase 1 R-GCN on the Neo4j KG snapshot.
- Adds query nodes and query feature edges in phase 2.
- Learns phase 2 representations on the expanded graph.

But it removes:

- query-store ranking loss
- ranking scoring head training
- hard negative sampling for query-store ranking

Evaluation ranks stores by cosine similarity between the learned query representation
and store representations.

Latest result:

| Variant | nDCG@5 | MRR@5 | HR@5 |
|---|---:|---:|---:|
| `phase2_repr_only` | 0.2490 | 0.5012 | 0.8571 |

Interpretation:

This is the fair representation-only baseline for phase 2. It shows that learning
query representations helps, but the explicit ranking loss in `full` is still the
main source of ranking quality.

## `random_neg`

This variant tests whether hard negative sampling improves phase 2 ranking.

Compared with `full`, it keeps:

- phase 1 R-GCN training
- query nodes and query feature edges
- phase 2 ranking loss
- ranking scoring head training
- KG auxiliary loss

But it replaces hard negative sampling with fully random negative sampling:

- `query-hard-negative-ratio = 0`
- `query-model-hard-negative-ratio = 0`

Latest result:

| Variant | nDCG@5 | MRR@5 | HR@5 |
|---|---:|---:|---:|
| `random_neg` | 0.6315 | 1.0000 | 1.0000 |

Interpretation:

This isolates the value of hard negatives. In the latest run, hard negatives improve
`nDCG@5` by `+0.0148` over random negatives while keeping `MRR@5` and `HR@5` tied.

## `score_based`

This variant is a non-learning baseline.

Compared with `full`, it does not train a phase 2 ranking model. Instead, it uses a
fixed score based on hand-coded query-store signals:

- distance
- price difference
- open flag
- category match
- aspect/rating signal
- review confidence
- radius match

It still runs the graph preparation path so the eval data and candidate pool are
consistent, but ranking is produced by the fixed score instead of learned ranking.

Latest result:

| Variant | nDCG@5 | MRR@5 | HR@5 |
|---|---:|---:|---:|
| `score_based` | 0.4329 | 0.7500 | 0.9286 |

Interpretation:

This baseline shows how far a simple score heuristic can go without learned ranking.
It is stronger than representation-only on this split, but clearly below `full` on
`nDCG@5` and `MRR@5`.

## Latest Summary

| Variant | Difference From `full` | nDCG@5 | MRR@5 | HR@5 |
|---|---|---:|---:|---:|
| `full` | Reference model | 0.6463 | 1.0000 | 1.0000 |
| `phase2_repr_only` | Removes ranking loss and scoring head; learns phase 2 representations only | 0.2490 | 0.5012 | 0.8571 |
| `random_neg` | Replaces hard negatives with random negatives | 0.6315 | 1.0000 | 1.0000 |
| `score_based` | Replaces learned ranking with fixed heuristic scoring | 0.4329 | 0.7500 | 0.9286 |

