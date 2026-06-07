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
    result = build_local_vector_index(settings)
    print(
        {
            "graph_version": result.get("graph_version"),
            "record_count": len(result.get("records", [])),
            "created_at": result.get("created_at"),
        }
    )


if __name__ == "__main__":
    main()
