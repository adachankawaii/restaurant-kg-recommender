# Production GraphRAG + KG + R-GCN

This folder contains a production-oriented wrapper around the existing experimental repository.

Default mode is `offline`.

Key points:

- `offline` mode reuses local CSV/KG artifacts already present in this repository.
- `online` mode is implemented with production adapters and feature flags, but is not runtime-verified here.
- The recommendation API does not expose a UI toggle for `offline` vs `online`; mode is controlled in config/code.
- A separate monitoring UI is provided under `/monitoring`.

Common commands:

```bash
cd production
make offline-ingest
make build-kg MODE=offline
make build-indexes MODE=offline
make export-rgcn MODE=offline
make api MODE=offline
```

Or:

```bash
cd production
make offline-all
```
