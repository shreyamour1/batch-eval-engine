#!/usr/bin/env python3
"""Generate a sample_batch.json with N prompt items (default 1000).

Usage:
    python3 scripts/generate_sample_batch.py 1000 > sample_batch.json
"""
import json
import sys


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    items = [
        {
            "id": f"p-{i:05d}",
            "prompt": f"In one sentence, summarize the key benefit of approach number {i}.",
        }
        for i in range(1, n + 1)
    ]
    json.dump(items, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
