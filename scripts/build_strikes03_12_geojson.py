#!/usr/bin/env python3
"""Compatibility wrapper for the reusable strike processor."""

from __future__ import annotations

import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from process_strikes import process_strikes


def run() -> int:
    process_strikes(
        input_path=Path("data/strikes_geojson/Strikes03_12.csv"),
        output_dir=Path("data/strikes_geojson"),
        archive_dir=Path("data/strikes_geojson/OG and Old Groups"),
        target_max_records=496,
        threshold_start_km=1.0,
        threshold_step_km=0.25,
        threshold_max_km=20.0,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
