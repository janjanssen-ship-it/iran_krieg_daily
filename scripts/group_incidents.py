#!/usr/bin/env python3
"""Group strike incidents into spatial bins per attacking_side and export outputs."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

EARTH_RADIUS_M = 6378137.0
DEFAULT_THRESHOLDS_KM = (1.0, 1.5, 2.0, 3.0)


def mercator_xy_m(lon: float, lat: float) -> tuple[float, float]:
    """Convert lon/lat to Web Mercator meters for grid binning."""
    lat_clamped = max(min(lat, 85.05112878), -85.05112878)
    lon_rad = math.radians(lon)
    lat_rad = math.radians(lat_clamped)
    x = EARTH_RADIUS_M * lon_rad
    y = EARTH_RADIUS_M * math.log(math.tan(math.pi / 4.0 + lat_rad / 2.0))
    return (x, y)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="data/strikes_geojson/Strikes Eco.geojson",
        help="Path to source GeoJSON.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/strikes_geojson",
        help="Directory where grouped files are written.",
    )
    parser.add_argument(
        "--thresholds-km",
        default=",".join(str(v) for v in DEFAULT_THRESHOLDS_KM),
        help="Comma-separated grid cell sizes in km (e.g. 1,1.5,2,3).",
    )
    parser.add_argument(
        "--prefix",
        default="Strikes Eco.grouped.grid",
        help="Output filename prefix.",
    )
    return parser.parse_args()


def threshold_label(threshold_km: float) -> str:
    if float(threshold_km).is_integer():
        return f"{int(threshold_km)}km"
    return f"{str(threshold_km).replace('.', '_')}km"


def pipe_join(values: set[str]) -> str:
    return " | ".join(sorted(v for v in values if v))


def cluster_features(
    features: list[dict[str, Any]],
    threshold_km: float,
) -> list[dict[str, Any]]:
    cell_m = threshold_km * 1000.0
    binned: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)

    # Hard business rule: never mix attacking_side values.
    for feature in features:
        props = feature.get("properties", {})
        attacking_side = str(props.get("attacking_side", "")).strip()
        geometry = feature.get("geometry", {})
        coords = geometry.get("coordinates", [])
        lon = float(coords[0])
        lat = float(coords[1])
        x_m, y_m = mercator_xy_m(lon, lat)
        key = (attacking_side, math.floor(x_m / cell_m), math.floor(y_m / cell_m))
        binned[key].append(feature)

    grouped_features: list[dict[str, Any]] = []

    for key in sorted(binned.keys()):
        attacking_side, cell_x, cell_y = key
        members = binned[key]
        group_size = len(members)
        lons: list[float] = []
        lats: list[float] = []
        event_ids: list[str] = []
        event_dates: list[str] = []
        event_types: set[str] = set()
        countries: set[str] = set()
        sources: set[str] = set()
        locations: set[str] = set()

        for member in members:
            props = member.get("properties", {})
            coords = member.get("geometry", {}).get("coordinates", [])
            lon = float(coords[0])
            lat = float(coords[1])
            lons.append(lon)
            lats.append(lat)
            event_ids.append(str(props.get("event_id", "")))

            event_date = str(props.get("event_date", "")).strip()
            if event_date:
                event_dates.append(event_date)

            event_type = str(props.get("event_type", "")).strip()
            if event_type:
                event_types.add(event_type)

            country = str(props.get("country", "")).strip()
            if country:
                countries.add(country)

            source = str(props.get("source", "")).strip()
            if source:
                sources.add(source)

            location = str(props.get("location", "")).strip()
            if location:
                locations.add(location)

        mean_lon = sum(lons) / group_size
        mean_lat = sum(lats) / group_size
        is_grouped = group_size > 1

        group_id = (
            f"{attacking_side.lower().replace(' ', '_').replace(',', '')}"
            f"__{threshold_label(threshold_km)}__{cell_x}_{cell_y}"
        )
        grouped_features.append(
            {
                "type": "Feature",
                "properties": {
                    "attacking_side": attacking_side,
                    "is_grouped": is_grouped,
                    "group_size": group_size,
                    "group_id": group_id,
                    "grouped_event_ids": "|".join(sorted(event_ids, key=lambda x: (len(x), x))),
                    "grouping_method": "grid_binning",
                    "grouping_threshold_km": threshold_km,
                    "group_note": (
                        f"Grouped incident (n={group_size}), mean coordinate"
                        if is_grouped
                        else "Single incident (not grouped)"
                    ),
                    "event_date_min": min(event_dates) if event_dates else "",
                    "event_date_max": max(event_dates) if event_dates else "",
                    "event_type_values": pipe_join(event_types),
                    "country_values": pipe_join(countries),
                    "source_values": pipe_join(sources),
                    "location_values": pipe_join(locations),
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [mean_lon, mean_lat],
                },
            }
        )
    return grouped_features


def write_geojson(path: Path, features: list[dict[str, Any]]) -> None:
    out_fc = {
        "type": "FeatureCollection",
        "name": path.stem,
        "features": features,
    }
    path.write_text(json.dumps(out_fc, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, features: list[dict[str, Any]]) -> None:
    base_cols = [
        "attacking_side",
        "is_grouped",
        "group_size",
        "group_id",
        "grouped_event_ids",
        "grouping_method",
        "grouping_threshold_km",
        "group_note",
        "event_date_min",
        "event_date_max",
        "event_type_values",
        "country_values",
        "source_values",
        "location_values",
        "geometry_type",
        "longitude",
        "latitude",
    ]
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=base_cols)
        writer.writeheader()
        for feature in features:
            props = feature["properties"]
            coords = feature["geometry"]["coordinates"]
            row = {
                **props,
                "geometry_type": feature["geometry"]["type"],
                "longitude": coords[0],
                "latitude": coords[1],
            }
            writer.writerow(row)


def validate_groups(
    grouped: list[dict[str, Any]],
    expected_total_members: int,
    threshold_km: float,
) -> dict[str, Any]:
    total_members = sum(int(f["properties"]["group_size"]) for f in grouped)
    grouped_records = sum(1 for f in grouped if f["properties"]["is_grouped"])
    max_group_size = max((int(f["properties"]["group_size"]) for f in grouped), default=0)

    side_mix_violations = 0
    for f in grouped:
        side = str(f["properties"].get("attacking_side", "")).strip()
        if not side:
            side_mix_violations += 1

    return {
        "threshold_km": threshold_km,
        "output_records": len(grouped),
        "grouped_records": grouped_records,
        "max_group_size": max_group_size,
        "total_members": total_members,
        "member_total_ok": total_members == expected_total_members,
        "attacking_side_violations": side_mix_violations,
    }


def run() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    thresholds = [float(x.strip()) for x in args.thresholds_km.split(",") if x.strip()]

    with input_path.open("r", encoding="utf-8") as fp:
        source_fc = json.load(fp)

    source_features = source_fc.get("features", [])
    print(f"Input incidents: {len(source_features)}")
    print("Grouping rule: grid_binning, per attacking_side")
    print("")

    summaries: list[dict[str, Any]] = []
    for threshold_km in thresholds:
        grouped = cluster_features(source_features, threshold_km=threshold_km)
        label = threshold_label(threshold_km)
        geojson_path = output_dir / f"{args.prefix}_{label}.geojson"
        csv_path = output_dir / f"{args.prefix}_{label}.csv"
        write_geojson(geojson_path, grouped)
        write_csv(csv_path, grouped)

        summary = validate_groups(
            grouped,
            expected_total_members=len(source_features),
            threshold_km=threshold_km,
        )
        summaries.append(summary)
        print(
            f"{threshold_km:g} km -> {summary['output_records']} grouped points | "
            f"grouped records: {summary['grouped_records']} | "
            f"max group size: {summary['max_group_size']} | "
            f"member sum ok: {summary['member_total_ok']} | "
            f"side violations: {summary['attacking_side_violations']}"
        )
        print(f"  wrote: {geojson_path}")
        print(f"  wrote: {csv_path}")

    under_500 = [s for s in summaries if s["output_records"] <= 500]
    print("")
    if under_500:
        best = sorted(under_500, key=lambda x: x["output_records"], reverse=True)[0]
        print(
            f"Recommended threshold: {best['threshold_km']:g} km "
            f"({best['output_records']} records, closest to 500 without exceeding)"
        )
    else:
        print("No threshold in this run produced 500 or fewer records.")

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
