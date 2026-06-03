from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pipelines.vector_builder.build_vector_index import build_local_vector_index
from settings import load_settings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="offline")
    args = parser.parse_args()
    settings = load_settings(mode=args.mode)
    print(build_local_vector_index(settings))


if __name__ == "__main__":
    main()
