"""Facts read directly from ``config/*.yaml``.

Tests assert against these instead of hardcoded values so the suite stays
green (and meaningful) when the configuration evolves. This is an
independent re-reading of the YAML files — it deliberately does not go
through :mod:`mercury.core.config` so loader regressions are caught too.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"


def load(name: str) -> dict[str, Any]:
    with (CONFIG_DIR / name).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def base_yaml() -> dict[str, Any]:
    return load("base.yaml")


def environments_yaml() -> dict[str, Any]:
    return load("environments.yaml")


def default_environment_name() -> str:
    """The environment ``load_config()`` resolves when no env vars/args apply
    (base.yaml ``environment:`` wins over environments.yaml ``default:``)."""
    return base_yaml().get("environment") or environments_yaml()["default"]


def environment_profile(name: str | None = None) -> dict[str, Any]:
    name = name or default_environment_name()
    return environments_yaml()["environments"][name]


def strategy_ids() -> list[str]:
    """Strategy ids in loader order: every ``config/strategy_*.yaml`` merged in
    sorted-filename order (mirrors ``_load_strategy_entries``)."""
    ids: list[str] = []
    for path in sorted(CONFIG_DIR.glob("strategy_*.yaml")):
        for entry in load(path.name).get("strategies") or []:
            ids.append(entry["id"])
    return ids


def database_name(environment: str | None = None) -> str:
    profile = environment_profile(environment)
    return profile.get("database_name") or f"mercury_{environment}"
