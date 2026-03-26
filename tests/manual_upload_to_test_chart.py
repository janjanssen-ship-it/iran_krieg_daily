#!/usr/bin/env python3
"""Manual helper to run the uploader against the test chart."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_test_chart_id() -> str:
    sys.path.insert(0, str(REPO_ROOT))
    from scripts import upload_iran_krieg_to_datawrapper as uploader

    uploader.load_env_file(REPO_ROOT / ".env")
    chart_id = uploader.os.environ.get("DATAWRAPPER_TEST_CHART_ID", "").strip()
    if not chart_id:
        raise ValueError("DATAWRAPPER_TEST_CHART_ID is required in .env for the test-chart helper.")
    return chart_id


def run() -> int:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "upload_iran_krieg_to_datawrapper.py"),
        "--chart-id",
        resolve_test_chart_id(),
    ]
    cmd.extend(sys.argv[1:])
    completed = subprocess.run(cmd, cwd=REPO_ROOT)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(run())
