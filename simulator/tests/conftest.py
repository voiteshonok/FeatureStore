"""Expose the src package on the test package that shares its name.

Pytest's importlib mode names these tests ``simulator.tests.*``, so
``import simulator`` is the directory that contains ``tests/`` and not
``src/simulator``. Extend that package with the real sources before the
tests import it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import simulator

_SRC = Path(__file__).resolve().parents[1] / "src" / "simulator"
if str(_SRC) not in simulator.__path__:
    simulator.__path__.append(str(_SRC))

if "generate" not in simulator.__dict__:
    init = _SRC / "__init__.py"
    exec(compile(init.read_text(encoding="utf-8"), init, "exec"), simulator.__dict__)  # noqa: S102
    sys.modules["simulator"] = simulator
