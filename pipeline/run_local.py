"""Compatibility entry point for the local pipeline.

Kept so the original command still works::

    python pipeline/run_local.py

It now delegates to the incremental runner, which reads sources through
checkpoints, applies quality rules, upserts into DuckDB and refreshes the
metrics CSV + dashboard.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow ``python pipeline/run_local.py`` (script) as well as
# ``python -m pipeline.run_local`` (module) to import the ``pipeline`` package.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import PipelineSettings  # noqa: E402
from pipeline.runner import run_pipeline  # noqa: E402


def main() -> int:
    run_id = run_pipeline(PipelineSettings())
    print(f"[pipeline] run {run_id} finished")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())