from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipelines.rgcn_exporter.export_rgcn_snapshot import export_rgcn_snapshot
from settings import load_settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="offline")
    args = parser.parse_args()
    settings = load_settings(mode=args.mode)
    print(export_rgcn_snapshot(settings))


if __name__ == "__main__":
    main()
