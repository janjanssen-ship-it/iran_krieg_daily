# Iran Krieg Daily

This workspace contains the daily SRF strike-reduction and Datawrapper upload workflow.

## Folders

- `input/`: dated raw CSV downloads or local source files
- `output/`: latest dated reduced CSVs for Datawrapper
- `output/old/`: archived older output CSVs from previous runs
- `logs/`: latest and dated run summaries

## Main command

Run from the repo root:

```bash
python3 scripts/reduce_iran_krieg_points.py
```

Upload the latest reduced outputs to the test Datawrapper chart:

```bash
python3 tests/manual_upload_to_test_chart.py --dry-run
python3 tests/manual_upload_to_test_chart.py
```

Upload the latest reduced outputs to the production chart:

```bash
python3 scripts/upload_iran_krieg_to_datawrapper.py --dry-run
python3 scripts/upload_iran_krieg_to_datawrapper.py
```

Upload a specific run to the production chart:

```bash
python3 scripts/upload_iran_krieg_to_datawrapper.py --date 2026_03_26
```

## GitHub Actions

The production automation now lives in:

```text
.github/workflows/iran-krieg-daily.yml
```

It runs the two production scripts in sequence:

1. `python3 scripts/reduce_iran_krieg_points.py`
2. `python3 scripts/upload_iran_krieg_to_datawrapper.py`

The workflow uses GitHub repository secrets for environment variables:

- `DATAWRAPPER_API_ROOT`
- `DATAWRAPPER_ACCESS_TOKEN`
- `DATAWRAPPER_CHART_ID`
- `IRAN_KRIEG_SOURCE_URL`

GitHub Actions cron is UTC-only, so the workflow is scheduled at `05:00 UTC`.
That corresponds to `06:00` in `Europe/Zurich` during standard time.
If you want it to stay at `06:00` local time after DST changes, update the cron manually.

Use an existing local CSV instead of downloading:

```bash
python3 scripts/reduce_iran_krieg_points.py --use-local-input --input 2026_03_17_iran_krieg_angriffe_srf_latest.csv
```

You can also point to an existing CSV elsewhere in the repo:

```bash
python3 scripts/reduce_iran_krieg_points.py --use-local-input --input data/strikes_geojson/Strikes03_15.csv
```

## Configuration

Default source URL resolution order:

1. `--source-url`
2. `IRAN_KRIEG_SOURCE_URL` environment variable
3. `data/iran_krieg_daily/config.json`
4. built-in default SRF article URL

Recommended local `.env` keys:

- `DATAWRAPPER_API_ROOT`
- `DATAWRAPPER_ACCESS_TOKEN`
- `DATAWRAPPER_CHART_ID`
- `DATAWRAPPER_TEST_CHART_ID`
- `IRAN_KRIEG_SOURCE_URL`

`source_url` can be:

- a stable SRF topic page such as `https://www.srf.ch/news/iran-krieg`
- an SRF article URL that embeds the chart
- a Datawrapper chart URL or bare chart id
- a direct `https://datawrapper.dwcdn.net/<chart>/<version>/dataset.csv` URL

If you point to the stable SRF topic page, the script will try the page itself
first and then walk its linked SRF news articles until it finds the embedded
Iran-Krieg Datawrapper chart. If SRF changes that structure again, prefer
setting `chart_id` or `dataset_url` in the config so the daily job keeps
targeting the correct source directly.

Example config file:

```json
{
  "source_url": "https://www.srf.ch/news/iran-krieg",
  "chart_id": "YOURCHARTID",
  "min_actor_count": 3,
  "required_actors": [
    "Iran und Verbündete",
    "USA und Israel im Iran",
    "Israel im Libanon"
  ]
}
```

## Notes

- Rows with a zero latitude or zero longitude are excluded from the reduced output and counted in the run summary.
- The outputs stay actor-specific. Rows with different `Actor` values are never grouped together.
- Output CSVs are created dynamically for every actor label found in the source CSV, while the combined total still has to stay at or below `496`.
- After a successful run, previously published files in `output/` are moved into `output/old/`, and only the latest run's files remain in `output/`.
- `min_actor_count` and `required_actors` are optional safety checks. Use them to fail fast if the upstream download suddenly exposes the wrong chart or older actor labels.
- The production uploader reads `DATAWRAPPER_CHART_ID` from the environment or `.env`. The test helper reads `DATAWRAPPER_TEST_CHART_ID` from `.env`.
- The uploader keeps the manual city markers in the destination Datawrapper chart, replaces only the attack markers, drops `grouped_source_rows`, updates the intro date to one day before the run date, and publishes automatically unless `--dry-run` is used.
- The retired local `launchd` automation files have been archived in `old_workflow/`.
- Generated SRF inputs, outputs, and logs are local runtime artifacts and are ignored by Git.
