#!/usr/bin/env python3
"""Fetch or load SRF strike data and reduce it to actor-specific CSVs for Datawrapper."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import os
import re
import shutil
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

EARTH_RADIUS_M = 6378137.0
DEFAULT_SOURCE_URL = (
    "https://www.srf.ch/news/international/"
    "krieg-in-nahost-macron-irans-angriffe-auf-nachbarn-muessen-aufhoeren"
)
DEFAULT_INPUT_BASENAME = "iran_krieg_angriffe_srf_latest.csv"
USER_AGENT = "Mozilla/5.0 (compatible; SRF-Datawrapper-Scraper/1.0)"
DATAWRAPPER_DATASET_RE = re.compile(
    r"^https://datawrapper\.dwcdn\.net/([A-Za-z0-9]+)/(\d+)/dataset\.csv(?:\?.*)?$"
)
DATAWRAPPER_CHART_RE = re.compile(
    r"^https://datawrapper\.dwcdn\.net/([A-Za-z0-9]+)(?:/\d+/?)?(?:\?.*)?$"
)

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = REPO_ROOT / "data" / "iran_krieg_daily"
INPUT_DIR = WORKSPACE_DIR / "input"
OUTPUT_DIR = WORKSPACE_DIR / "output"
OLD_OUTPUT_DIR = OUTPUT_DIR / "old"
LOG_DIR = WORKSPACE_DIR / "logs"
CONFIG_PATH = WORKSPACE_DIR / "config.json"
ENV_SOURCE_URL = "IRAN_KRIEG_SOURCE_URL"


@dataclass(frozen=True)
class ProcessingIssue:
    row_number: int
    reason: str


@dataclass(frozen=True)
class ProcessingStats:
    total_rows: int
    valid_rows: int
    missing_time_rows: int
    zero_coordinate_rows: int
    actor_counts: dict[str, int]
    issues: list[ProcessingIssue]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_BASENAME,
        help=(
            "Local CSV filename inside the dedicated input folder, or a repo-relative/"
            f"absolute path when using --use-local-input. Default: {DEFAULT_INPUT_BASENAME}"
        ),
    )
    parser.add_argument(
        "--source-url",
        help="Override the configured SRF article URL.",
    )
    parser.add_argument(
        "--use-local-input",
        action="store_true",
        help="Skip download and use the local input CSV from the dedicated input folder.",
    )
    parser.add_argument(
        "--target-max-records",
        type=int,
        default=496,
        help="Maximum number of output points. Default: 496",
    )
    parser.add_argument(
        "--threshold-start-km",
        type=float,
        default=1.0,
        help="Threshold sweep start in kilometers. Default: 1.0",
    )
    parser.add_argument(
        "--threshold-step-km",
        type=float,
        default=0.25,
        help="Threshold sweep step in kilometers. Default: 0.25",
    )
    parser.add_argument(
        "--threshold-max-km",
        type=float,
        default=20.0,
        help="Threshold sweep max in kilometers. Default: 20.0",
    )
    return parser.parse_args()


def ensure_workspace_dirs() -> None:
    for path in (INPUT_DIR, OUTPUT_DIR, OLD_OUTPUT_DIR, LOG_DIR):
        path.mkdir(parents=True, exist_ok=True)


def load_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if not isinstance(data, dict):
        raise ValueError(f"{CONFIG_PATH} must contain a JSON object.")
    return data


def resolve_source_url(cli_value: str | None) -> str:
    if cli_value:
        return cli_value
    env_value = os.environ.get(ENV_SOURCE_URL, "").strip()
    if env_value:
        return env_value
    config = load_config()
    config_value = str(config.get("source_url", "")).strip()
    return config_value or DEFAULT_SOURCE_URL


def resolve_dataset_url(cli_value: str | None) -> str:
    if cli_value:
        return ""
    config = load_config()
    return str(config.get("dataset_url", "")).strip()


def resolve_chart_id(cli_value: str | None) -> str:
    if cli_value:
        chart_match = DATAWRAPPER_CHART_RE.match(cli_value)
        if chart_match:
            return chart_match.group(1)
        if re.fullmatch(r"[A-Za-z0-9]+", cli_value.strip()):
            return cli_value.strip()
        return ""
    config = load_config()
    config_value = str(config.get("chart_id", "")).strip()
    return config_value


def resolve_min_actor_count() -> int | None:
    config = load_config()
    raw_value = config.get("min_actor_count")
    if raw_value in (None, ""):
        return None
    value = int(raw_value)
    if value <= 0:
        raise ValueError("min_actor_count must be greater than 0.")
    return value


def resolve_required_actors() -> list[str]:
    config = load_config()
    raw_value = config.get("required_actors", [])
    if raw_value in (None, ""):
        return []
    if not isinstance(raw_value, list):
        raise ValueError("required_actors must be a JSON array of actor labels.")
    actors = [str(item).strip() for item in raw_value if str(item).strip()]
    return actors


def ensure_local_filename(filename: str, base_dir: Path) -> Path:
    path = Path(filename)
    if path.is_absolute():
        raise ValueError("Please pass only a filename, not an absolute path.")
    if path.parent != Path("."):
        raise ValueError("Please pass only a filename, without subfolders.")
    resolved = (base_dir / path.name).resolve()
    if resolved.parent != base_dir.resolve():
        raise ValueError("Input and output files must stay in their intended folders.")
    return resolved


def resolve_local_input_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    if path.parent == Path("."):
        candidate = INPUT_DIR / path.name
        if candidate.exists():
            return candidate
    return (REPO_ROOT / path).resolve()


def build_dated_input_filename(now: datetime, basename: str) -> str:
    return f"{now:%Y_%m_%d}_{basename}"


def extract_date_prefix(filename: str) -> str | None:
    match = re.match(r"^(\d{4}_\d{2}_\d{2})_", filename)
    if match:
        return match.group(1)
    return None


def build_dated_output_filename(date_prefix: str, basename: str) -> str:
    return f"{date_prefix}_{basename}"


def build_thresholds(start_km: float, step_km: float, max_km: float) -> list[float]:
    if start_km <= 0 or step_km <= 0 or max_km < start_km:
        raise ValueError("Invalid threshold settings.")
    values: list[float] = []
    current = start_km
    while current <= max_km + 1e-9:
        values.append(round(current, 6))
        current += step_km
    return values


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except HTTPError as exc:
        raise ValueError(f"HTTP error while fetching {url}: {exc.code}") from exc
    except URLError as exc:
        raise ValueError(f"Network error while fetching {url}: {exc.reason}") from exc


def download_binary(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request) as response:
            return response.read()
    except HTTPError as exc:
        raise ValueError(f"HTTP error while downloading {url}: {exc.code}") from exc
    except URLError as exc:
        raise ValueError(f"Network error while downloading {url}: {exc.reason}") from exc


def extract_chart_id(article_html: str) -> str:
    pattern = re.compile(
        r'<div[^>]+id="datawrapper-vis-([A-Za-z0-9]+)".*?<noscript><img[^>]+alt="([^"]+)"',
        re.DOTALL,
    )
    matches = [
        (chart_id, html.unescape(alt_text))
        for chart_id, alt_text in pattern.findall(article_html)
    ]
    if not matches:
        raise ValueError("No Datawrapper chart found on the SRF page.")

    for chart_id, alt_text in matches:
        if "Iran-Krieg" in alt_text and "Angriffe" in alt_text:
            return chart_id

    return matches[0][0]


def extract_hub_timestamp_minutes(label_text: str) -> int | None:
    match = re.search(r"\b(\d{1,2}):(\d{2})\b", label_text)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        return None
    return hour * 60 + minute


def extract_candidate_article_urls(page_html: str, *, base_url: str) -> list[str]:
    anchor_pattern = re.compile(
        r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )
    candidates_with_order: list[tuple[int, int, str]] = []
    seen: set[str] = set()
    for index, (href, label_html) in enumerate(anchor_pattern.findall(page_html)):
        absolute_url = urljoin(base_url, href)
        parsed = urlparse(absolute_url)
        if parsed.netloc != "www.srf.ch":
            continue
        if not parsed.path.startswith("/news/"):
            continue
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if normalized == base_url.rstrip("/"):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        label_text = html.unescape(re.sub(r"<[^>]+>", " ", label_html))
        label_text = re.sub(r"\s+", " ", label_text).strip()
        timestamp_minutes = extract_hub_timestamp_minutes(label_text)
        # Prefer entries with a visible hub-page timestamp, then the newest time, then page order.
        sort_minutes = timestamp_minutes if timestamp_minutes is not None else -1
        candidates_with_order.append((sort_minutes, -index, normalized))
    candidates_with_order.sort(reverse=True)
    return [normalized for _, _, normalized in candidates_with_order]


def resolve_chart_from_page(page_url: str, page_html: str) -> tuple[str, str]:
    try:
        return extract_chart_id(page_html), page_url
    except ValueError:
        pass

    for candidate_url in extract_candidate_article_urls(page_html, base_url=page_url):
        candidate_html = fetch_text(candidate_url)
        try:
            return extract_chart_id(candidate_html), candidate_url
        except ValueError:
            continue

    raise ValueError(f"No Datawrapper chart found on {page_url} or its linked SRF news pages.")


def resolve_chart_version(chart_id: str) -> int:
    chart_root_html = fetch_text(f"https://datawrapper.dwcdn.net/{chart_id}/")
    match = re.search(rf"/{re.escape(chart_id)}/(\d+)/", chart_root_html)
    if not match:
        raise ValueError(f"Could not resolve current Datawrapper version for chart {chart_id}.")
    return int(match.group(1))


def build_dataset_url(*, source_url: str, dataset_url: str, chart_id: str) -> tuple[str, int, str, str]:
    direct_dataset = dataset_url or source_url
    direct_dataset_match = DATAWRAPPER_DATASET_RE.match(direct_dataset)
    if direct_dataset_match:
        return (
            direct_dataset_match.group(1),
            int(direct_dataset_match.group(2)),
            direct_dataset,
            direct_dataset,
        )

    explicit_chart_id = chart_id or resolve_chart_id(source_url)
    if explicit_chart_id:
        version = resolve_chart_version(explicit_chart_id)
        return (
            explicit_chart_id,
            version,
            f"https://datawrapper.dwcdn.net/{explicit_chart_id}/{version}/dataset.csv",
            f"https://datawrapper.dwcdn.net/{explicit_chart_id}/",
        )

    article_html = fetch_text(source_url)
    article_chart_id, resolved_page_url = resolve_chart_from_page(source_url, article_html)
    version = resolve_chart_version(article_chart_id)
    dataset_url = f"https://datawrapper.dwcdn.net/{article_chart_id}/{version}/dataset.csv"
    return article_chart_id, version, dataset_url, resolved_page_url


def download_latest_input(
    *,
    source_url: str,
    dataset_url: str,
    chart_id: str,
    destination: Path,
) -> tuple[str, int, str, str]:
    chart_id, version, dataset_url, resolved_source_url = build_dataset_url(
        source_url=source_url,
        dataset_url=dataset_url,
        chart_id=chart_id,
    )
    dataset_bytes = download_binary(dataset_url)
    destination.write_bytes(dataset_bytes)
    return chart_id, version, dataset_url, resolved_source_url


def validate_actor_expectations(
    actor_counts: dict[str, int],
    *,
    min_actor_count: int | None,
    required_actors: list[str],
) -> None:
    actors = sorted(actor_counts)
    if min_actor_count is not None and len(actors) < min_actor_count:
        raise ValueError(
            f"Expected at least {min_actor_count} actors, but found {len(actors)}: "
            + ", ".join(actors)
        )
    missing_actors = [actor for actor in required_actors if actor not in actor_counts]
    if missing_actors:
        raise ValueError(
            "Downloaded dataset is missing required actors: " + ", ".join(missing_actors)
        )


def mercator_xy_m(lon: float, lat: float) -> tuple[float, float]:
    lat_clamped = max(min(lat, 85.05112878), -85.05112878)
    lon_rad = math.radians(lon)
    lat_rad = math.radians(lat_clamped)
    x = EARTH_RADIUS_M * lon_rad
    y = EARTH_RADIUS_M * math.log(math.tan(math.pi / 4.0 + lat_rad / 2.0))
    return (x, y)


def slugify_name(value: str) -> str:
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    prepared = "".join(replacements.get(char, char) for char in value.strip().lower())
    normalized = unicodedata.normalize("NFKD", prepared)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    pieces = []
    previous_sep = False
    for char in ascii_only:
        if char.isalnum():
            pieces.append(char)
            previous_sep = False
        elif not previous_sep:
            pieces.append("_")
            previous_sep = True
    return "".join(pieces).strip("_") or "latest"


def build_actor_output_filename(actor: str) -> str:
    return f"{slugify_name(actor)}_attacken.reduced_for_datawrapper.csv"


def load_rows(path: Path) -> tuple[list[dict[str, object]], ProcessingStats]:
    rows: list[dict[str, object]] = []
    issues: list[ProcessingIssue] = []
    actor_counts: Counter[str] = Counter()
    total_rows = 0
    missing_time_rows = 0
    zero_coordinate_rows = 0

    with path.open("r", newline="", encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        expected_fields = {"Time", "Latitude", "Longitude", "Actor"}
        if not reader.fieldnames or not expected_fields.issubset(set(reader.fieldnames)):
            raise ValueError(
                "Input CSV must include Time, Latitude, Longitude, and Actor columns."
            )

        for source_row, row in enumerate(reader, start=2):
            total_rows += 1
            actor = str(row.get("Actor", "")).strip()
            time_value = str(row.get("Time", "")).strip()
            if not time_value:
                missing_time_rows += 1
            if not actor:
                issues.append(ProcessingIssue(source_row, "missing_actor"))
                continue

            try:
                lat = float(str(row.get("Latitude", "")).strip())
                lon = float(str(row.get("Longitude", "")).strip())
            except ValueError:
                issues.append(ProcessingIssue(source_row, "invalid_coordinate"))
                continue

            if not -90 <= lat <= 90 or not -180 <= lon <= 180:
                issues.append(ProcessingIssue(source_row, "coordinate_out_of_range"))
                continue

            if lat == 0 or lon == 0:
                zero_coordinate_rows += 1
                continue

            rows.append(
                {
                    "source_row": source_row,
                    "Time": time_value,
                    "Actor": actor,
                    "Latitude": lat,
                    "Longitude": lon,
                }
            )
            actor_counts[actor] += 1

    stats = ProcessingStats(
        total_rows=total_rows,
        valid_rows=len(rows),
        missing_time_rows=missing_time_rows,
        zero_coordinate_rows=zero_coordinate_rows,
        actor_counts=dict(sorted(actor_counts.items())),
        issues=issues,
    )
    return rows, stats


def cluster_rows(rows: list[dict[str, object]], threshold_km: float) -> list[dict[str, object]]:
    cell_m = threshold_km * 1000.0
    binned: dict[tuple[str, int, int], list[dict[str, object]]] = defaultdict(list)

    for row in rows:
        actor = str(row["Actor"])
        lat = float(row["Latitude"])
        lon = float(row["Longitude"])
        x_m, y_m = mercator_xy_m(lon, lat)
        key = (actor, math.floor(x_m / cell_m), math.floor(y_m / cell_m))
        binned[key].append(row)

    grouped_rows: list[dict[str, object]] = []
    for actor, cell_x, cell_y in sorted(binned.keys()):
        members = binned[(actor, cell_x, cell_y)]
        lons = [float(member["Longitude"]) for member in members]
        lats = [float(member["Latitude"]) for member in members]
        times = [str(member["Time"]) for member in members if str(member["Time"]).strip()]
        source_rows = [str(int(member["source_row"])) for member in members]
        grouped_rows.append(
            {
                "Time": times[0] if times else "",
                "Actor": actor,
                "Latitude": round(sum(lats) / len(lats), 6),
                "Longitude": round(sum(lons) / len(lons), 6),
                "group_size": len(members),
                "grouped_source_rows": ",".join(source_rows),
                "grouping_threshold_km": threshold_km,
                "_grid_key": f"{cell_x}_{cell_y}",
            }
        )
    return grouped_rows


def split_rows_by_actor(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    by_actor: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_actor[str(row["Actor"])].append(row)
    return dict(by_actor)


def choose_shared_grouping(
    rows_by_actor: dict[str, list[dict[str, object]]],
    *,
    target_max_records: int,
    threshold_start_km: float,
    threshold_step_km: float,
    threshold_max_km: float,
) -> tuple[float, dict[str, list[dict[str, object]]], int, list[dict[str, object]]]:
    thresholds = build_thresholds(threshold_start_km, threshold_step_km, threshold_max_km)
    summaries: list[dict[str, object]] = []
    for threshold_km in thresholds:
        grouped_by_actor = {
            actor: cluster_rows(actor_rows, threshold_km)
            for actor, actor_rows in rows_by_actor.items()
        }
        total_records = sum(len(grouped_rows) for grouped_rows in grouped_by_actor.values())
        summaries.append(
            {
                "threshold_km": threshold_km,
                "total_records": total_records,
                "per_actor_counts": {
                    actor: len(grouped_rows) for actor, grouped_rows in grouped_by_actor.items()
                },
            }
        )
        if total_records <= target_max_records:
            return threshold_km, grouped_by_actor, total_records, summaries
    raise ValueError(
        f"No threshold from {threshold_start_km:g} km to {threshold_max_km:g} km "
        f"reduced the combined output to {target_max_records} or fewer records."
    )


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = [
        "Time",
        "Actor",
        "Latitude",
        "Longitude",
        "group_size",
        "grouped_source_rows",
        "grouping_threshold_km",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row[key] for key in fieldnames})


def archive_existing_outputs(output_dir: Path, archive_dir: Path) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    for path in sorted(output_dir.iterdir()):
        if not path.is_file():
            continue
        shutil.move(str(path), str(archive_dir / path.name))


def write_run_summary(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def process_iran_krieg(
    *,
    source_url: str,
    dataset_url: str,
    chart_id: str,
    use_local_input: bool,
    input_filename: str,
    target_max_records: int,
    threshold_start_km: float,
    threshold_step_km: float,
    threshold_max_km: float,
    min_actor_count: int | None = None,
    required_actors: list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    ensure_workspace_dirs()
    now = now or datetime.now()

    if use_local_input:
        input_path = resolve_local_input_path(input_filename)
        if not input_path.exists():
            raise ValueError(f"Local input file not found: {input_path}")
        source_label = f"local file {input_path.name}"
        resolved_source_url = source_url
        date_prefix = extract_date_prefix(input_path.name) or now.strftime("%Y_%m_%d")
        chart_id = ""
        chart_version = 0
        dataset_url = ""
    else:
        dated_input_name = build_dated_input_filename(now, DEFAULT_INPUT_BASENAME)
        input_path = ensure_local_filename(dated_input_name, INPUT_DIR)
        chart_id, chart_version, dataset_url, resolved_source_url = download_latest_input(
            source_url=source_url,
            dataset_url=dataset_url,
            chart_id=chart_id,
            destination=input_path,
        )
        source_label = f"{resolved_source_url} -> chart {chart_id} v{chart_version}"
        date_prefix = extract_date_prefix(input_path.name) or now.strftime("%Y_%m_%d")

    rows, stats = load_rows(input_path)
    if not rows:
        raise ValueError("No valid rows found in the input CSV.")
    validate_actor_expectations(
        stats.actor_counts,
        min_actor_count=min_actor_count,
        required_actors=required_actors or [],
    )

    rows_by_actor = split_rows_by_actor(rows)
    threshold_km, grouped_by_actor, total_records, summaries = choose_shared_grouping(
        rows_by_actor,
        target_max_records=target_max_records,
        threshold_start_km=threshold_start_km,
        threshold_step_km=threshold_step_km,
        threshold_max_km=threshold_max_km,
    )

    staging_dir = OUTPUT_DIR / ".staging"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    staged_output_paths: dict[str, Path] = {}
    for actor, grouped_rows in sorted(grouped_by_actor.items()):
        filename = build_actor_output_filename(actor)
        output_path = ensure_local_filename(build_dated_output_filename(date_prefix, filename), staging_dir)
        write_csv(output_path, grouped_rows)
        staged_output_paths[actor] = output_path

    archive_existing_outputs(OUTPUT_DIR, OLD_OUTPUT_DIR)
    output_paths: dict[str, Path] = {}
    for actor, staged_path in staged_output_paths.items():
        destination = OUTPUT_DIR / staged_path.name
        shutil.move(str(staged_path), str(destination))
        output_paths[actor] = destination
    staging_dir.rmdir()

    stamp = now.strftime("%Y%m%dT%H%M%S")
    latest_summary_path = LOG_DIR / "latest_run_summary.txt"
    dated_summary_path = LOG_DIR / f"{stamp}_run_summary.txt"
    issue_counts = Counter(issue.reason for issue in stats.issues)

    summary_lines = [
        f"Run timestamp: {now.isoformat(timespec='seconds')}",
        f"Source: {source_label}",
        f"Source URL: {source_url}",
        f"Resolved source URL: {resolved_source_url}",
        f"Dataset URL: {dataset_url or 'n/a'}",
        f"Input file: {input_path.name}",
        f"Total CSV rows: {stats.total_rows}",
        f"Valid rows used: {stats.valid_rows}",
        f"Missing Time values: {stats.missing_time_rows}",
        f"Rows excluded for zero coordinates: {stats.zero_coordinate_rows}",
        f"Selected threshold: {threshold_km:g} km",
        f"Combined output rows: {total_records}",
        "Actor counts:",
    ]
    for actor, count in stats.actor_counts.items():
        summary_lines.append(f"- {actor}: {count}")
    summary_lines.append("Threshold sweep:")
    for item in summaries:
        per_actor = ", ".join(
            f"{actor}={count}" for actor, count in sorted(item["per_actor_counts"].items())
        )
        summary_lines.append(
            f"- {item['threshold_km']:g} km -> {item['total_records']} total ({per_actor})"
        )
    summary_lines.append("Output files:")
    for actor, path in output_paths.items():
        summary_lines.append(f"- {actor}: {path.name}")
    summary_lines.append("Skipped rows:")
    if issue_counts:
        for reason, count in sorted(issue_counts.items()):
            summary_lines.append(f"- {reason}: {count}")
    else:
        summary_lines.append("- none")

    write_run_summary(latest_summary_path, summary_lines)
    write_run_summary(dated_summary_path, summary_lines)

    return {
        "input_path": input_path,
        "output_paths": output_paths,
        "summary_path": dated_summary_path,
        "latest_summary_path": latest_summary_path,
        "selected_threshold_km": threshold_km,
        "combined_output_rows": total_records,
        "zero_coordinate_rows": stats.zero_coordinate_rows,
        "chart_id": chart_id,
        "chart_version": chart_version,
    }


def run() -> int:
    args = parse_args()
    source_url = resolve_source_url(args.source_url)
    dataset_url = resolve_dataset_url(args.source_url)
    chart_id = resolve_chart_id(args.source_url)
    result = process_iran_krieg(
        source_url=source_url,
        dataset_url=dataset_url,
        chart_id=chart_id,
        use_local_input=args.use_local_input,
        input_filename=args.input,
        target_max_records=args.target_max_records,
        threshold_start_km=args.threshold_start_km,
        threshold_step_km=args.threshold_step_km,
        threshold_max_km=args.threshold_max_km,
        min_actor_count=resolve_min_actor_count(),
        required_actors=resolve_required_actors(),
    )
    print(f"Input: {result['input_path'].name}")
    print(f"Selected threshold: {result['selected_threshold_km']:g} km")
    print(f"Combined output rows: {result['combined_output_rows']}")
    print(f"Rows excluded for zero coordinates: {result['zero_coordinate_rows']}")
    for actor, path in result["output_paths"].items():
        print(f"Output: {actor} -> {path}")
    print(f"Summary: {result['summary_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
