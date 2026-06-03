from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipelines.ingestion.online_ingest import run_online_ingest
from settings import load_settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="online.yaml")
    args = parser.parse_args()
    settings = load_settings(mode="online", config_name=args.config.split("/")[-1])
    print(run_online_ingest(settings))


if __name__ == "__main__":
    main()
