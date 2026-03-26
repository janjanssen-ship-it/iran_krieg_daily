# Iran Krieg Daily

This repository automates a daily map update workflow for the Iran war coverage.

It does two things:

1. pulls the latest strike dataset from the source
2. reduces and uploads that data into a production Datawrapper locator map

## Production Flow

The active production scripts are:

- `scripts/reduce_iran_krieg_points.py`
- `scripts/upload_iran_krieg_to_datawrapper.py`

The production automation runs through:

- `.github/workflows/iran-krieg-daily.yml`

That workflow:

1. fetches the latest source dataset
2. reduces the point count to fit Datawrapper limits
3. uploads the reduced result to the production chart
4. republishes the chart
5. uploads logs and output CSVs as workflow artifacts

## Configuration

Local development uses `.env`.

Expected keys:

- `DATAWRAPPER_API_ROOT`
- `DATAWRAPPER_ACCESS_TOKEN`
- `DATAWRAPPER_CHART_ID`
- `DATAWRAPPER_TEST_CHART_ID`
- `IRAN_KRIEG_SOURCE_URL`

GitHub Actions should use repository secrets for the production workflow.

## Local Usage

Fetch and reduce from the source:

```bash
python3 scripts/reduce_iran_krieg_points.py
```

Dry-run the production upload:

```bash
python3 scripts/upload_iran_krieg_to_datawrapper.py --dry-run
```

Dry-run the test-chart helper:

```bash
python3 tests/manual_upload_to_test_chart.py --dry-run
```

## Repository Layout

- `scripts/` contains the active production scripts.
- `tests/` contains focused tests plus the test-chart helper.
- `data/iran_krieg_daily/` contains workflow-specific config, plus local runtime folders for input, logs, and output.

More detailed workflow notes live in:

- `data/iran_krieg_daily/README.md`
