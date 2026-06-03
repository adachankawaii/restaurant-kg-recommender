from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipelines.kg_builder.build_kg import build_kg_snapshot
from settings import load_settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="offline")
    args = parser.parse_args()
    settings = load_settings(mode=args.mode)
    print(build_kg_snapshot(settings))


if __name__ == "__main__":
    main()
