from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.process_strikes import actor_slug, build_thresholds, cluster_features, process_strikes


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=["Time", "Latitude", "Longitude", "Actor"])
        writer.writeheader()
        writer.writerows(rows)


def read_geojson(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_actor_slug_normalizes_unicode() -> None:
    assert actor_slug("Iran und Verbündete") == "iran_und_verbundete"
    assert actor_slug("USA und Israel") == "usa_und_israel"


def test_build_thresholds_validates_inputs() -> None:
    assert build_thresholds(1.0, 0.25, 1.5) == [1.0, 1.25, 1.5]


def test_cluster_features_never_mixes_actors() -> None:
    features = [
        {
            "type": "Feature",
            "properties": {"Actor": "A"},
            "geometry": {"type": "Point", "coordinates": [35.0, 33.0]},
            "_source_index": 2,
        },
        {
            "type": "Feature",
            "properties": {"Actor": "B"},
            "geometry": {"type": "Point", "coordinates": [35.0001, 33.0001]},
            "_source_index": 3,
        },
    ]
    grouped = cluster_features(features, threshold_km=20.0)
    assert len(grouped) == 2
    assert sorted(feature["properties"]["Actor"] for feature in grouped) == ["A", "B"]
    assert all(feature["properties"]["group_size"] == 1 for feature in grouped)


def test_process_strikes_archives_intermediates_and_keeps_split_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "strikes_geojson"
    archive_dir = output_dir / "OG and Old Groups"
    input_path = output_dir / "Sample.csv"
    output_dir.mkdir(parents=True)

    rows = [
        {"Time": "", "Latitude": "0", "Longitude": "35.0000", "Actor": "Iran und Verbündete"},
        {"Time": "01:00", "Latitude": "33.0002", "Longitude": "35.0002", "Actor": "Iran und Verbündete"},
        {"Time": "02:00", "Latitude": "33.5000", "Longitude": "35.5000", "Actor": "Iran und Verbündete"},
        {"Time": "03:00", "Latitude": "34.0000", "Longitude": "36.0000", "Actor": "USA und Israel"},
        {"Time": "03:30", "Latitude": "34.5000", "Longitude": "0", "Actor": "USA und Israel"},
        {"Time": "04:00", "Latitude": "34.0002", "Longitude": "36.0002", "Actor": "USA und Israel"},
        {"Time": "05:00", "Latitude": "95.0000", "Longitude": "36.2000", "Actor": "USA und Israel"},
    ]
    write_csv(input_path, rows)

    result = process_strikes(
        input_path=input_path,
        output_dir=output_dir,
        archive_dir=archive_dir,
        target_max_records=3,
        threshold_start_km=1.0,
        threshold_step_km=0.5,
        threshold_max_km=5.0,
    )

    assert result["selected_threshold_km"] == 1.0

    remaining_files = sorted(path.name for path in output_dir.iterdir() if path.is_file())
    assert remaining_files == [
        "Sample.csv",
        "Sample.grouped.final.actor_iran_und_verbundete.geojson",
        "Sample.grouped.final.actor_usa_und_israel.geojson",
        "Sample.processing_report.txt",
    ]

    archived_files = sorted(path.name for path in archive_dir.iterdir() if path.is_file())
    assert archived_files == [
        "Sample.geojson",
        "Sample.grouped.final.csv",
        "Sample.grouped.final.geojson",
        "Sample.grouped.grid_1km.csv",
        "Sample.grouped.grid_1km.geojson",
        "Sample.grouped.threshold_summary.csv",
    ]

    iran_geojson = read_geojson(output_dir / "Sample.grouped.final.actor_iran_und_verbundete.geojson")
    usa_geojson = read_geojson(output_dir / "Sample.grouped.final.actor_usa_und_israel.geojson")
    assert len(iran_geojson["features"]) == 2
    assert len(usa_geojson["features"]) == 1
    assert all(
        feature["geometry"]["coordinates"][0] != 0 and feature["geometry"]["coordinates"][1] != 0
        for feature in iran_geojson["features"] + usa_geojson["features"]
    )

    report_text = (output_dir / "Sample.processing_report.txt").read_text(encoding="utf-8")
    assert "Missing Time values ignored: 1" in report_text
    assert "Rows excluded for zero coordinates: 2" in report_text
    assert "coordinate_out_of_range: 1" in report_text
    assert "zero_coordinate: 2" in report_text
