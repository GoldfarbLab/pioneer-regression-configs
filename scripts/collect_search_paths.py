#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def collect_paths(config_root: Path) -> list[str]:
    paths: set[str] = set()
    for path in config_root.rglob("search*.json"):
        with path.open() as handle:
            data = json.load(handle)
        search_paths = data.get("paths", {})
        for key in ("ms_data", "library"):
            value = search_paths.get(key)
            if value:
                paths.add(value)
    return sorted(paths)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect ms_data and library paths from search JSON files."
    )
    parser.add_argument(
        "config_root",
        type=Path,
        help="Root directory to scan for search*.json files.",
    )
    args = parser.parse_args()

    for path in collect_paths(args.config_root):
        print(path)


if __name__ == "__main__":
    main()
