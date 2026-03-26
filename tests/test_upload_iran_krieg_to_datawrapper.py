from __future__ import annotations

import csv
from pathlib import Path

from scripts import upload_iran_krieg_to_datawrapper as uploader


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=[
                "Time",
                "Actor",
                "Latitude",
                "Longitude",
                "group_size",
                "grouped_source_rows",
                "grouping_threshold_km",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_discover_date_prefix_picks_latest_output_set(tmp_path: Path) -> None:
    (tmp_path / "2026_03_25_iran_und_verbuendete_attacken.reduced_for_datawrapper.csv").write_text(
        "x\n", encoding="utf-8"
    )
    (tmp_path / "2026_03_26_iran_und_verbuendete_attacken.reduced_for_datawrapper.csv").write_text(
        "x\n", encoding="utf-8"
    )
    assert uploader.discover_date_prefix(tmp_path, None) == "2026_03_26"


def test_update_intro_text_replaces_stand_date() -> None:
    assert (
        uploader.update_intro_text(
            "Angriffe der verschiedenen Kriegsparteien, <br> Stand: 25. März",
            "26. März",
        )
        == "Angriffe der verschiedenen Kriegsparteien, <br> Stand: 26. März"
    )


def test_format_intro_date_uses_previous_day() -> None:
    assert uploader.format_intro_date("2026_03_26") == "25. März"


def test_build_attack_markers_omits_trace_fields_and_uses_group_mapping() -> None:
    current_data = {
        "markers": [
            {"id": "cities", "type": "group", "title": "Cities"},
            {"id": "city-1", "type": "point", "title": "Teheran"},
            {"id": "group-il", "type": "group", "title": "Israel Libanon"},
            {
                "id": "tpl-il",
                "type": "point",
                "groupId": "group-il",
                "markerColor": "#2659FF",
                "title": "Marker\nText",
                "data": {"Actor": "Israel im Libanon"},
                "coordinates": [35.0, 33.0],
            },
            {"id": "group-iran", "type": "group", "title": "Iran"},
            {
                "id": "tpl-iran",
                "type": "point",
                "groupId": "group-iran",
                "markerColor": "#CA0016",
                "title": "Marker\nText",
                "data": {"Actor": "Iran und Verbündete"},
                "coordinates": [51.0, 35.0],
            },
            {"id": "group-usa", "type": "group", "title": "USA"},
            {
                "id": "tpl-usa",
                "type": "point",
                "groupId": "group-usa",
                "markerColor": "#0D2880",
                "title": "Marker\nText",
                "data": {"Actor": "USA und Israel im Iran"},
                "coordinates": [50.0, 36.0],
            },
        ]
    }
    group_objects, templates = uploader.select_group_objects(current_data["markers"])
    rows_by_actor = {
        "Israel im Libanon": [
            {
                "Time": "",
                "Actor": "Israel im Libanon",
                "Latitude": 33.1,
                "Longitude": 35.2,
                "group_size": 2,
            }
        ],
        "Iran und Verbündete": [
            {
                "Time": "06:00",
                "Actor": "Iran und Verbündete",
                "Latitude": 35.7,
                "Longitude": 51.4,
                "group_size": 3,
            }
        ],
        "USA und Israel im Iran": [
            {
                "Time": "10:30",
                "Actor": "USA und Israel im Iran",
                "Latitude": 36.2,
                "Longitude": 50.7,
                "group_size": 4,
            }
        ],
    }

    attack_markers = uploader.build_attack_markers(
        rows_by_actor=rows_by_actor,
        group_objects=group_objects,
        templates=templates,
    )
    payload = uploader.replace_attack_markers(current_data, attack_markers)
    uploaded = [marker for marker in payload["markers"] if marker.get("data")]

    assert [marker["title"] for marker in payload["markers"] if not marker.get("data")] == [
        "Cities",
        "Teheran",
        "Israel Libanon",
        "Iran",
        "USA",
    ]
    assert len(uploaded) == 3
    assert uploaded[0]["groupId"] == "group-il"
    assert uploaded[0]["data"] == {
        "Time": "",
        "Actor": "Israel im Libanon",
        "Latitude": 33.1,
        "Longitude": 35.2,
        "group_size": 2,
    }
    assert "grouped_source_rows" not in uploaded[0]["data"]
    assert "grouping_threshold_km" not in uploaded[0]["data"]


def test_load_rows_by_actor_reads_expected_output_files(tmp_path: Path) -> None:
    original_output_dir = uploader.OUTPUT_DIR
    try:
        uploader.OUTPUT_DIR = tmp_path
        date_prefix = "2026_03_26"
        for actor in uploader.ACTOR_TO_GROUP_TITLE:
            filename = f"{date_prefix}_{uploader.build_actor_output_filename(actor)}"
            write_csv(
                tmp_path / filename,
                [
                    {
                        "Time": "10:00",
                        "Actor": actor,
                        "Latitude": "35.0",
                        "Longitude": "51.0",
                        "group_size": "2",
                        "grouped_source_rows": "1,2",
                        "grouping_threshold_km": "11.0",
                    }
                ],
            )

        rows_by_actor = uploader.load_rows_by_actor(date_prefix)
        assert sorted(rows_by_actor) == sorted(uploader.ACTOR_TO_GROUP_TITLE)
        assert rows_by_actor["Iran und Verbündete"][0]["group_size"] == 2
    finally:
        uploader.OUTPUT_DIR = original_output_dir


def test_resolve_chart_id_defaults_to_production_chart() -> None:
    original = uploader.os.environ.get("DATAWRAPPER_CHART_ID")
    original_env_path = uploader.ENV_PATH
    try:
        uploader.ENV_PATH = Path("/tmp/nonexistent.env")
        uploader.os.environ.pop("DATAWRAPPER_CHART_ID", None)
        assert uploader.resolve_chart_id("TEST01") == "TEST01"
        uploader.os.environ["DATAWRAPPER_CHART_ID"] = "ABCD1"
        assert uploader.resolve_chart_id(None) == "ABCD1"
        uploader.os.environ.pop("DATAWRAPPER_CHART_ID", None)
        try:
            uploader.resolve_chart_id(None)
        except ValueError as exc:
            assert "DATAWRAPPER_CHART_ID is required" in str(exc)
        else:
            raise AssertionError("Expected resolve_chart_id(None) to fail without env config.")
    finally:
        uploader.ENV_PATH = original_env_path
        if original is None:
            uploader.os.environ.pop("DATAWRAPPER_CHART_ID", None)
        else:
            uploader.os.environ["DATAWRAPPER_CHART_ID"] = original


def test_resolve_api_root_prefers_env_override() -> None:
    original = uploader.os.environ.get("DATAWRAPPER_API_ROOT")
    original_env_path = uploader.ENV_PATH
    try:
        uploader.ENV_PATH = Path("/tmp/nonexistent.env")
        uploader.os.environ.pop("DATAWRAPPER_API_ROOT", None)
        assert uploader.resolve_api_root() == "https://api.datawrapper.de/v3"
        uploader.os.environ["DATAWRAPPER_API_ROOT"] = "https://example.invalid/v3"
        assert uploader.resolve_api_root() == "https://example.invalid/v3"
    finally:
        uploader.ENV_PATH = original_env_path
        if original is None:
            uploader.os.environ.pop("DATAWRAPPER_API_ROOT", None)
        else:
            uploader.os.environ["DATAWRAPPER_API_ROOT"] = original


def test_build_attack_markers_uses_fallback_template_when_chart_was_cleared() -> None:
    current_data = {
        "markers": [
            {"id": "cities", "type": "group", "title": "Cities"},
            {"id": "city-1", "type": "point", "title": "Teheran"},
            {"id": "group-il", "type": "group", "title": "Israel Libanon"},
            {"id": "group-iran", "type": "group", "title": "Iran"},
            {"id": "group-usa", "type": "group", "title": "USA"},
        ]
    }
    group_objects, templates = uploader.select_group_objects(current_data["markers"])
    rows_by_actor = {
        "Israel im Libanon": [
            {
                "Time": "",
                "Actor": "Israel im Libanon",
                "Latitude": 33.1,
                "Longitude": 35.2,
                "group_size": 2,
            }
        ],
        "Iran und Verbündete": [
            {
                "Time": "06:00",
                "Actor": "Iran und Verbündete",
                "Latitude": 35.7,
                "Longitude": 51.4,
                "group_size": 3,
            }
        ],
        "USA und Israel im Iran": [
            {
                "Time": "10:30",
                "Actor": "USA und Israel im Iran",
                "Latitude": 36.2,
                "Longitude": 50.7,
                "group_size": 4,
            }
        ],
    }

    attack_markers = uploader.build_attack_markers(
        rows_by_actor=rows_by_actor,
        group_objects=group_objects,
        templates=templates,
    )

    assert attack_markers["Israel Libanon"][0]["markerColor"] == "#2659FF"
    assert attack_markers["Iran"][0]["markerColor"] == "#CA0016"
    assert attack_markers["USA"][0]["markerColor"] == "#0D2880"
    assert attack_markers["USA"][0]["groupId"] == "group-usa"
