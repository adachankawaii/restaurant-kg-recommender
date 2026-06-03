from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from settings import load_settings
from training.rgcn.train import train_rgcn


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="offline")
    args = parser.parse_args()
    settings = load_settings(mode=args.mode)
    print(train_rgcn(settings))


if __name__ == "__main__":
    main()
