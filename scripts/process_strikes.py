#!/usr/bin/env python3
"""Process strike CSV files into grouped per-actor GeoJSON outputs."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

EARTH_RADIUS_M = 6378137.0


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
    duplicate_actor_coordinate_keys: int
    max_duplicate_actor_coordinate_count: int
    actor_counts: dict[str, int]
    issues: list[ProcessingIssue]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        help="Path to the source CSV file.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/strikes_geojson",
        help="Directory where run outputs are written.",
    )
    parser.add_argument(
        "--archive-dir",
        default="data/strikes_geojson/OG and Old Groups",
        help="Directory where intermediate outputs are archived.",
    )
    parser.add_argument(
        "--target-max-records",
        type=int,
        default=496,
        help="Maximum allowed grouped output records.",
    )
    parser.add_argument(
        "--threshold-start-km",
        type=float,
        default=1.0,
        help="Threshold sweep start in kilometers.",
    )
    parser.add_argument(
        "--threshold-step-km",
        type=float,
        default=0.25,
        help="Threshold sweep step in kilometers.",
    )
    parser.add_argument(
        "--threshold-max-km",
        type=float,
        default=20.0,
        help="Threshold sweep max in kilometers.",
    )
    return parser.parse_args()


def mercator_xy_m(lon: float, lat: float) -> tuple[float, float]:
    lat_clamped = max(min(lat, 85.05112878), -85.05112878)
    lon_rad = math.radians(lon)
    lat_rad = math.radians(lat_clamped)
    x = EARTH_RADIUS_M * lon_rad
    y = EARTH_RADIUS_M * math.log(math.tan(math.pi / 4.0 + lat_rad / 2.0))
    return (x, y)


def threshold_label(threshold_km: float) -> str:
    rounded = round(threshold_km, 6)
    if float(rounded).is_integer():
        return f"{int(rounded)}km"
    return f"{str(rounded).replace('.', '_')}km"


def actor_slug(actor: str) -> str:
    normalized = unicodedata.normalize("NFKD", actor.strip().lower())
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    cleaned = []
    previous_was_sep = False
    for char in ascii_only:
        if char.isalnum():
            cleaned.append(char)
            previous_was_sep = False
        elif not previous_was_sep:
            cleaned.append("_")
            previous_was_sep = True
    return "".join(cleaned).strip("_") or "unknown_actor"


def build_thresholds(start_km: float, step_km: float, max_km: float) -> list[float]:
    if start_km <= 0 or step_km <= 0 or max_km < start_km:
        raise ValueError("Invalid threshold settings.")
    values: list[float] = []
    current = start_km
    while current <= max_km + 1e-9:
        values.append(round(current, 6))
        current += step_km
    return values


def load_csv_features(path: Path) -> tuple[list[dict[str, Any]], ProcessingStats]:
    features: list[dict[str, Any]] = []
    issues: list[ProcessingIssue] = []
    actor_counts: Counter[str] = Counter()
    missing_time_rows = 0
    zero_coordinate_rows = 0
    coord_counter: Counter[tuple[float, float, str]] = Counter()
    total_rows = 0

    with path.open("r", newline="", encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        expected_fields = {"Time", "Latitude", "Longitude", "Actor"}
        if not reader.fieldnames or not expected_fields.issubset(set(reader.fieldnames)):
            raise ValueError(
                "Input CSV must include Time, Latitude, Longitude, and Actor columns."
            )

        for row_number, row in enumerate(reader, start=2):
            total_rows += 1
            time_value = str(row.get("Time", "")).strip()
            if not time_value:
                missing_time_rows += 1

            actor = str(row.get("Actor", "")).strip()
            if not actor:
                issues.append(ProcessingIssue(row_number=row_number, reason="missing_actor"))
                continue

            try:
                lat = float(str(row.get("Latitude", "")).strip())
                lon = float(str(row.get("Longitude", "")).strip())
            except ValueError:
                issues.append(ProcessingIssue(row_number=row_number, reason="invalid_coordinate"))
                continue

            if not -90 <= lat <= 90 or not -180 <= lon <= 180:
                issues.append(ProcessingIssue(row_number=row_number, reason="coordinate_out_of_range"))
                continue

            if lat == 0 or lon == 0:
                zero_coordinate_rows += 1
                issues.append(ProcessingIssue(row_number=row_number, reason="zero_coordinate"))
                continue

            actor_counts[actor] += 1
            coord_counter[(round(lat, 6), round(lon, 6), actor)] += 1
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "Actor": actor,
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat],
                    },
                    "_source_index": row_number,
                }
            )

    duplicate_keys = sum(1 for count in coord_counter.values() if count > 1)
    max_duplicate_count = max(coord_counter.values(), default=0)
    stats = ProcessingStats(
        total_rows=total_rows,
        valid_rows=len(features),
        missing_time_rows=missing_time_rows,
        zero_coordinate_rows=zero_coordinate_rows,
        duplicate_actor_coordinate_keys=duplicate_keys,
        max_duplicate_actor_coordinate_count=max_duplicate_count,
        actor_counts=dict(sorted(actor_counts.items())),
        issues=issues,
    )
    return features, stats


def cluster_features(features: list[dict[str, Any]], threshold_km: float) -> list[dict[str, Any]]:
    cell_m = threshold_km * 1000.0
    binned: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)

    for feature in features:
        actor = str(feature.get("properties", {}).get("Actor", "")).strip()
        lon, lat = feature["geometry"]["coordinates"]
        x_m, y_m = mercator_xy_m(float(lon), float(lat))
        key = (actor, math.floor(x_m / cell_m), math.floor(y_m / cell_m))
        binned[key].append(feature)

    grouped_features: list[dict[str, Any]] = []
    for actor, cell_x, cell_y in sorted(binned.keys()):
        members = binned[(actor, cell_x, cell_y)]
        lons = [float(member["geometry"]["coordinates"][0]) for member in members]
        lats = [float(member["geometry"]["coordinates"][1]) for member in members]
        member_ids = [int(member["_source_index"]) for member in members]
        group_size = len(members)
        grouped_features.append(
            {
                "type": "Feature",
                "properties": {
                    "Actor": actor,
                    "is_grouped": group_size > 1,
                    "group_size": group_size,
                    "group_id": (
                        f"{actor_slug(actor)}__{threshold_label(threshold_km)}__{cell_x}_{cell_y}"
                    ),
                    "grouped_source_indices": ",".join(str(value) for value in sorted(member_ids)),
                    "grouping_method": "grid_binning",
                    "grouping_threshold_km": threshold_km,
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [sum(lons) / group_size, sum(lats) / group_size],
                },
            }
        )
    return grouped_features


def validate_groups(
    grouped: list[dict[str, Any]],
    expected_total_members: int,
    allowed_actors: set[str],
    threshold_km: float,
) -> dict[str, Any]:
    total_members = sum(int(feature["properties"]["group_size"]) for feature in grouped)
    grouped_records = sum(1 for feature in grouped if feature["properties"]["is_grouped"])
    max_group_size = max((int(feature["properties"]["group_size"]) for feature in grouped), default=0)
    actor_violations = sum(
        1
        for feature in grouped
        if str(feature["properties"].get("Actor", "")).strip() not in allowed_actors
    )
    return {
        "threshold_km": threshold_km,
        "output_records": len(grouped),
        "grouped_records": grouped_records,
        "max_group_size": max_group_size,
        "total_members": total_members,
        "member_total_ok": total_members == expected_total_members,
        "actor_violations": actor_violations,
    }


def write_geojson(path: Path, features: list[dict[str, Any]]) -> None:
    clean_features = []
    for feature in features:
        clean_features.append(
            {
                "type": feature["type"],
                "properties": dict(feature["properties"]),
                "geometry": feature["geometry"],
            }
        )
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "name": path.stem,
                "features": clean_features,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def write_grouped_csv(path: Path, features: list[dict[str, Any]]) -> None:
    columns = [
        "Actor",
        "is_grouped",
        "group_size",
        "group_id",
        "grouped_source_indices",
        "grouping_method",
        "grouping_threshold_km",
        "longitude",
        "latitude",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        writer.writeheader()
        for feature in features:
            props = feature["properties"]
            lon, lat = feature["geometry"]["coordinates"]
            writer.writerow(
                {
                    **props,
                    "longitude": lon,
                    "latitude": lat,
                }
            )


def write_sweep_summary(path: Path, summaries: list[dict[str, Any]]) -> None:
    columns = [
        "threshold_km",
        "output_records",
        "grouped_records",
        "max_group_size",
        "total_members",
        "member_total_ok",
        "actor_violations",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=columns)
        writer.writeheader()
        writer.writerows(summaries)


def split_features_by_actor(features: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_actor: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for feature in features:
        actor = str(feature["properties"].get("Actor", "")).strip()
        by_actor[actor].append(feature)
    return dict(sorted(by_actor.items()))


def write_report(
    path: Path,
    *,
    input_path: Path,
    stats: ProcessingStats,
    summaries: list[dict[str, Any]],
    final_threshold: float,
    final_features: list[dict[str, Any]],
    split_paths: dict[str, Path],
) -> None:
    final_counts = {actor: len(features) for actor, features in split_features_by_actor(final_features).items()}
    lines = [
        f"Input file: {input_path.name}",
        f"Total CSV rows: {stats.total_rows}",
        f"Valid rows converted to GeoJSON: {stats.valid_rows}",
        f"Skipped rows: {len(stats.issues)}",
        f"Missing Time values ignored: {stats.missing_time_rows}",
        f"Rows excluded for zero coordinates: {stats.zero_coordinate_rows}",
        "Actors:",
    ]
    for actor, count in stats.actor_counts.items():
        lines.append(f"- {actor}: {count}")

    lines.extend(
        [
            "",
            "Threshold sweep:",
        ]
    )
    for summary in summaries:
        lines.append(
            "- "
            f"{summary['threshold_km']:g} km -> {summary['output_records']} records, "
            f"grouped records {summary['grouped_records']}, "
            f"max group size {summary['max_group_size']}, "
            f"member sum ok {summary['member_total_ok']}, "
            f"actor violations {summary['actor_violations']}"
        )

    lines.extend(
        [
            "",
            f"Selected threshold: {final_threshold:g} km",
            f"Final grouped total: {len(final_features)}",
            "Final split outputs:",
        ]
    )
    for actor, output_path in split_paths.items():
        lines.append(f"- {actor}: {final_counts[actor]} features -> {output_path.name}")

    lines.extend(
        [
            "",
            "Irregularities:",
            f"- Missing Time values ignored: {stats.missing_time_rows}",
            f"- Rows excluded for zero coordinates: {stats.zero_coordinate_rows}",
            (
                "- Duplicate actor+coordinate keys: "
                f"{stats.duplicate_actor_coordinate_keys} "
                f"(max repeats for one key: {stats.max_duplicate_actor_coordinate_count})"
            ),
        ]
    )
    if stats.issues:
        reason_counts = Counter(issue.reason for issue in stats.issues)
        for reason, count in sorted(reason_counts.items()):
            lines.append(f"- {reason}: {count}")
        example_rows = ", ".join(str(issue.row_number) for issue in stats.issues[:10])
        lines.append(f"- Example skipped row numbers: {example_rows}")
    else:
        lines.append("- No rows were skipped during conversion.")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def archive_outputs(paths: list[Path], archive_dir: Path) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    for path in paths:
        if not path.exists():
            continue
        destination = archive_dir / path.name
        if destination.exists():
            destination.unlink()
        shutil.move(str(path), str(destination))


def process_strikes(
    *,
    input_path: Path,
    output_dir: Path,
    archive_dir: Path,
    target_max_records: int,
    threshold_start_km: float,
    threshold_step_km: float,
    threshold_max_km: float,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    base_name = input_path.stem
    thresholds = build_thresholds(threshold_start_km, threshold_step_km, threshold_max_km)
    source_features, stats = load_csv_features(input_path)
    allowed_actors = {str(feature["properties"]["Actor"]).strip() for feature in source_features}

    base_geojson_path = output_dir / f"{base_name}.geojson"
    write_geojson(base_geojson_path, source_features)

    summaries: list[dict[str, Any]] = []
    intermediate_paths: list[Path] = [base_geojson_path]
    final_threshold: float | None = None
    final_features: list[dict[str, Any]] | None = None

    for threshold_km in thresholds:
        grouped = cluster_features(source_features, threshold_km)
        summary = validate_groups(grouped, len(source_features), allowed_actors, threshold_km)
        summaries.append(summary)

        label = threshold_label(threshold_km)
        grouped_geojson_path = output_dir / f"{base_name}.grouped.grid_{label}.geojson"
        grouped_csv_path = output_dir / f"{base_name}.grouped.grid_{label}.csv"
        write_geojson(grouped_geojson_path, grouped)
        write_grouped_csv(grouped_csv_path, grouped)
        intermediate_paths.extend([grouped_geojson_path, grouped_csv_path])

        if summary["output_records"] <= target_max_records and final_threshold is None:
            final_threshold = threshold_km
            final_features = grouped
            break

    if final_threshold is None or final_features is None:
        raise ValueError(
            f"No threshold from {threshold_start_km:g} km to {threshold_max_km:g} km "
            f"reduced the grouped output to {target_max_records} or fewer records."
        )

    summary_path = output_dir / f"{base_name}.grouped.threshold_summary.csv"
    write_sweep_summary(summary_path, summaries)
    combined_final_geojson_path = output_dir / f"{base_name}.grouped.final.geojson"
    combined_final_csv_path = output_dir / f"{base_name}.grouped.final.csv"
    write_geojson(combined_final_geojson_path, final_features)
    write_grouped_csv(combined_final_csv_path, final_features)
    intermediate_paths.extend([summary_path, combined_final_geojson_path, combined_final_csv_path])

    split_paths: dict[str, Path] = {}
    for actor, actor_features in split_features_by_actor(final_features).items():
        actor_path = output_dir / f"{base_name}.grouped.final.actor_{actor_slug(actor)}.geojson"
        write_geojson(actor_path, actor_features)
        split_paths[actor] = actor_path

    report_path = output_dir / f"{base_name}.processing_report.txt"
    write_report(
        report_path,
        input_path=input_path,
        stats=stats,
        summaries=summaries,
        final_threshold=final_threshold,
        final_features=final_features,
        split_paths=split_paths,
    )

    archive_outputs(intermediate_paths, archive_dir)

    return {
        "base_name": base_name,
        "input_rows": stats.total_rows,
        "valid_rows": stats.valid_rows,
        "selected_threshold_km": final_threshold,
        "split_paths": split_paths,
        "report_path": report_path,
        "summary_count": len(summaries),
    }


def run() -> int:
    args = parse_args()
    result = process_strikes(
        input_path=Path(args.input),
        output_dir=Path(args.output_dir),
        archive_dir=Path(args.archive_dir),
        target_max_records=args.target_max_records,
        threshold_start_km=args.threshold_start_km,
        threshold_step_km=args.threshold_step_km,
        threshold_max_km=args.threshold_max_km,
    )
    print(f"Processed: {result['base_name']}")
    print(f"Input rows: {result['input_rows']} | valid rows: {result['valid_rows']}")
    print(f"Selected threshold: {result['selected_threshold_km']:g} km")
    for actor, path in result["split_paths"].items():
        print(f"Final actor output: {actor} -> {path}")
    print(f"Report: {result['report_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
