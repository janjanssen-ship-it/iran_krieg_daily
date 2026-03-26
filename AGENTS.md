# AGENTS

## Purpose

This repository maintains a daily SRF-to-Datawrapper automation for the Iran war locator map.

The active production path is narrow:

1. `scripts/reduce_iran_krieg_points.py`
2. `scripts/upload_iran_krieg_to_datawrapper.py`
3. `.github/workflows/iran-krieg-daily.yml`

Treat those as the primary system.

## What The Workflow Does

- Pull the latest source dataset from SRF.
- Resolve the current upstream Datawrapper dataset from the SRF topic/article flow.
- Group points by actor and by shared spatial threshold until the total fits Datawrapper limits.
- Preserve actor separation.
- Upload the reduced markers into the production Datawrapper chart.
- Preserve manual city markers already present in the chart.
- Update the intro date to one day before the local run date.
- Publish the chart after upload.

## Runtime Configuration

Use environment variables, not hardcoded credentials.

Expected keys:

- `DATAWRAPPER_API_ROOT`
- `DATAWRAPPER_ACCESS_TOKEN`
- `DATAWRAPPER_CHART_ID`
- `DATAWRAPPER_TEST_CHART_ID`
- `IRAN_KRIEG_SOURCE_URL`

Local development reads from `.env`.
GitHub Actions reads from repository secrets.

## Safe Editing Rules

- Do not commit `.env`.
- Do not commit generated runtime files from:
  - `data/iran_krieg_daily/input/`
  - `data/iran_krieg_daily/logs/`
  - `data/iran_krieg_daily/output/`
- Preserve `.gitkeep` placeholders in those directories.
- Keep the production workflow simple: reducer first, uploader second.
- Do not reintroduce local scheduler automation such as `launchd`.

## Test And Verification

Before changing the production workflow, prefer verifying:

- local dry-run of `scripts/upload_iran_krieg_to_datawrapper.py`
- local dry-run of `tests/manual_upload_to_test_chart.py`
- focused local tests in `tests/`
- GitHub Actions run logs after workflow changes

## Non-Goals

- This repo is not a general-purpose geospatial processing toolbox.
- Legacy strike-processing utilities have been removed from the active repo path.
- Avoid adding unrelated datasets, export bundles, or local-machine automation back into the repository.
