"""List models known to a running LM Studio server (OpenAI-compatible
`/v1/models` endpoint), for use by run_demo.bat's model-selection step.

Usage:
    python scripts/list_lm_studio_models.py               # print numbered list
    python scripts/list_lm_studio_models.py --select 2     # print model id at index 2 (1-based)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


def fetch_models(base_url: str) -> list[str]:
    url = base_url.rstrip("/") + "/models"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.load(resp)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"error: could not reach LM Studio at {url}: {exc}", file=sys.stderr)
        sys.exit(1)
    return [m["id"] for m in data.get("data", []) if "id" in m]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--select", type=int, default=None, help="1-based index to print alone")
    parser.add_argument(
        "--base-url",
        default=os.getenv("LM_STUDIO_BASE_URL", "http://localhost:1234/v1"),
    )
    args = parser.parse_args()

    models = fetch_models(args.base_url)

    if not models:
        print("no models found", file=sys.stderr)
        sys.exit(1)

    if args.select is not None:
        if not (1 <= args.select <= len(models)):
            print(f"error: index {args.select} out of range (1-{len(models)})", file=sys.stderr)
            sys.exit(1)
        print(models[args.select - 1])
        return

    for i, model_id in enumerate(models, start=1):
        print(f"{i}. {model_id}")


if __name__ == "__main__":
    main()
