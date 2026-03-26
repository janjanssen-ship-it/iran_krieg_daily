# Iran Krieg Daily

This workspace contains the daily SRF strike-reduction workflow for Datawrapper.

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
