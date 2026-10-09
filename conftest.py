"""Root pytest configuration.

The ``tests/dags`` suite imports Airflow, which is only available inside the
Docker stack. On a no-Docker local machine we skip those tests instead of
failing collection, so ``pytest tests`` stays green with the local extras
installed.
"""

from __future__ import annotations

import importlib.util

collect_ignore: list[str] = []

if importlib.util.find_spec("airflow") is None:
    collect_ignore.append("tests/dags")