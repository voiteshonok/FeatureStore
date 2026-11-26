"""Load ``infra/.env`` for host-side clients in this suite.

Compose reads that file. pytest does not, unless a variable is already set.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

_ENV_FILE = Path(__file__).resolve().parents[2] / "infra" / ".env"


def _load_infra_env(path: Path) -> None:
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value


@pytest.fixture(scope="session")
def infra_env() -> None:
    if not _ENV_FILE.is_file():
        pytest.fail(f"{_ENV_FILE} is missing; run make secret")
    _load_infra_env(_ENV_FILE)


@pytest.fixture(autouse=True)
def _infra_env_for_e2e(request: pytest.FixtureRequest) -> None:
    """The OpenAPI schema check in this folder runs with the unit suite."""
    if request.node.get_closest_marker("e2e") is None:
        return
    request.getfixturevalue("infra_env")
