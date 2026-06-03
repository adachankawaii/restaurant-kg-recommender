MODE ?= offline
PYTHON ?= python

offline-ingest:
	$(PYTHON) scripts/run_offline_ingest.py --config configs/offline.yaml

online-ingest:
	$(PYTHON) scripts/run_online_ingest.py --config configs/online.yaml

build-kg:
	$(PYTHON) scripts/build_kg.py --mode $(MODE)

build-indexes:
	$(PYTHON) scripts/build_indexes.py --mode $(MODE)

export-rgcn:
	$(PYTHON) scripts/export_rgcn_snapshot.py --mode $(MODE)

train-rgcn:
	$(PYTHON) scripts/train_rgcn.py --mode $(MODE)

smoke-test:
	$(PYTHON) scripts/smoke_test.py --mode $(MODE)

api:
	$(PYTHON) -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8000

offline-all:
	$(MAKE) offline-ingest
	$(MAKE) build-kg MODE=offline
	$(MAKE) build-indexes MODE=offline
	$(MAKE) export-rgcn MODE=offline

offline-train:
	$(MAKE) offline-all
	$(MAKE) train-rgcn MODE=offline

online-all:
	$(MAKE) online-ingest
	$(MAKE) build-kg MODE=online
	$(MAKE) build-indexes MODE=online
	$(MAKE) export-rgcn MODE=online

online-train:
	$(MAKE) online-all
	$(MAKE) train-rgcn MODE=online

up:
	docker compose up -d

down:
	docker compose down

status:
	docker compose ps
