from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from scripts import reduce_iran_krieg_points as reducer


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=["Time", "Latitude", "Longitude", "Actor"])
        writer.writeheader()
        writer.writerows(rows)


def test_extract_chart_id_prefers_iran_krieg_chart() -> None:
    html = """
    <div id="datawrapper-vis-AAAAA"><noscript><img alt="Other chart"></noscript></div>
    <div id="datawrapper-vis-BBBBB"><noscript><img alt="Iran-Krieg Angriffe"></noscript></div>
    """
    assert reducer.extract_chart_id(html) == "BBBBB"


def test_build_thresholds() -> None:
    assert reducer.build_thresholds(1.0, 0.25, 1.5) == [1.0, 1.25, 1.5]


def test_build_actor_output_filename_handles_umlauts_and_new_labels() -> None:
    assert (
        reducer.build_actor_output_filename("Iran und Verbündete")
        == "iran_und_verbuendete_attacken.reduced_for_datawrapper.csv"
    )
    assert (
        reducer.build_actor_output_filename("USA und Israel im Iran")
        == "usa_und_israel_im_iran_attacken.reduced_for_datawrapper.csv"
    )
    assert (
        reducer.build_actor_output_filename("Israel im Libanon")
        == "israel_im_libanon_attacken.reduced_for_datawrapper.csv"
    )


def test_build_dataset_url_supports_direct_dataset_url() -> None:
    chart_id, version, dataset_url, resolved_source_url = reducer.build_dataset_url(
        source_url="https://example.invalid/article",
        dataset_url="https://datawrapper.dwcdn.net/AbC12/17/dataset.csv",
        chart_id="",
    )
    assert chart_id == "AbC12"
    assert version == 17
    assert dataset_url == "https://datawrapper.dwcdn.net/AbC12/17/dataset.csv"
    assert resolved_source_url == "https://datawrapper.dwcdn.net/AbC12/17/dataset.csv"


def test_extract_hub_timestamp_minutes() -> None:
    assert reducer.extract_hub_timestamp_minutes("newsticker 05:41 Krieg in Nahost") == 341
    assert reducer.extract_hub_timestamp_minutes("Krieg in Nahost Macron") is None


def test_extract_candidate_article_urls_prefers_newest_timestamp_and_filters_non_news() -> None:
    html = """
    <a href="/news/international/older">Krieg in Nahost Macron: Irans Angriffe auf Nachbarn müssen aufhören</a>
    <a href="/news/international/current">newsticker 05:41 Krieg in Nahost Trump: Iran ist verhandlungsbereit</a>
    <a href="/sport/foo">Ignore</a>
    <a href="https://www.srf.ch/news/international/current">Duplicate</a>
    """
    assert reducer.extract_candidate_article_urls(
        html,
        base_url="https://www.srf.ch/news/iran-krieg",
    ) == [
        "https://www.srf.ch/news/international/current",
        "https://www.srf.ch/news/international/older",
    ]


def test_resolve_chart_from_page_prefers_newest_timestamped_linked_article() -> None:
    original_fetch_text = reducer.fetch_text
    try:
        reducer.fetch_text = lambda url: (
            '<div id="datawrapper-vis-ZZZ99"><noscript><img alt="Iran-Krieg Angriffe"></noscript></div>'
            if url == "https://www.srf.ch/news/international/current-one"
            else ""
        )
        chart_id, resolved_url = reducer.resolve_chart_from_page(
            "https://www.srf.ch/news/iran-krieg",
            """
            <a href="/news/international/older-one">Krieg in Nahost Macron</a>
            <a href="/news/international/current-one">newsticker 05:41 Krieg in Nahost</a>
            """,
        )
        assert chart_id == "ZZZ99"
        assert resolved_url == "https://www.srf.ch/news/international/current-one"
    finally:
        reducer.fetch_text = original_fetch_text


def test_validate_actor_expectations_rejects_too_few_actors() -> None:
    try:
        reducer.validate_actor_expectations(
            {"Iran und Verbündete": 10, "USA und Israel": 20},
            min_actor_count=3,
            required_actors=[],
        )
    except ValueError as exc:
        assert "Expected at least 3 actors" in str(exc)
    else:
        raise AssertionError("Expected validate_actor_expectations() to fail.")


def test_validate_actor_expectations_rejects_missing_required_actor() -> None:
    try:
        reducer.validate_actor_expectations(
            {"Iran und Verbündete": 10, "USA und Israel im Iran": 20},
            min_actor_count=None,
            required_actors=["Iran und Verbündete", "Israel im Libanon"],
        )
    except ValueError as exc:
        assert "Israel im Libanon" in str(exc)
    else:
        raise AssertionError("Expected validate_actor_expectations() to fail.")


def test_process_iran_krieg_local_input(tmp_path: Path) -> None:
    original_workspace = reducer.WORKSPACE_DIR
    original_input = reducer.INPUT_DIR
    original_output = reducer.OUTPUT_DIR
    original_old_output = reducer.OLD_OUTPUT_DIR
    original_logs = reducer.LOG_DIR
    original_config = reducer.CONFIG_PATH
    try:
        reducer.WORKSPACE_DIR = tmp_path / "iran_krieg_daily"
        reducer.INPUT_DIR = reducer.WORKSPACE_DIR / "input"
        reducer.OUTPUT_DIR = reducer.WORKSPACE_DIR / "output"
        reducer.OLD_OUTPUT_DIR = reducer.OUTPUT_DIR / "old"
        reducer.LOG_DIR = reducer.WORKSPACE_DIR / "logs"
        reducer.CONFIG_PATH = reducer.WORKSPACE_DIR / "config.json"

        reducer.ensure_workspace_dirs()
        reducer.CONFIG_PATH.write_text(
            json.dumps({"source_url": "https://example.invalid/article"}),
            encoding="utf-8",
        )

        input_path = reducer.INPUT_DIR / "2026_03_17_iran_krieg_angriffe_srf_latest.csv"
        rows = [
            {"Time": "", "Latitude": "0", "Longitude": "0", "Actor": "Iran und Verbündete"},
            {"Time": "00:30", "Latitude": "0", "Longitude": "35.1000", "Actor": "Iran und Verbündete"},
            {"Time": "01:00", "Latitude": "33.0000", "Longitude": "35.0000", "Actor": "Iran und Verbündete"},
            {"Time": "01:05", "Latitude": "33.0002", "Longitude": "35.0002", "Actor": "Iran und Verbündete"},
            {"Time": "02:00", "Latitude": "34.0000", "Longitude": "36.0000", "Actor": "USA und Israel im Iran"},
            {"Time": "02:05", "Latitude": "34.0002", "Longitude": "36.0002", "Actor": "USA und Israel im Iran"},
            {"Time": "02:10", "Latitude": "35.0000", "Longitude": "37.0000", "Actor": "Israel im Libanon"},
            {"Time": "03:00", "Latitude": "91", "Longitude": "36.0", "Actor": "USA und Israel im Iran"},
        ]
        write_csv(input_path, rows)
        legacy_output = reducer.OUTPUT_DIR / "2026_03_16_legacy_attacken.reduced_for_datawrapper.csv"
        legacy_output.write_text("legacy\n", encoding="utf-8")

        result = reducer.process_iran_krieg(
            source_url="https://example.invalid/article",
            dataset_url="",
            chart_id="",
            use_local_input=True,
            input_filename=input_path.name,
            target_max_records=4,
            threshold_start_km=1.0,
            threshold_step_km=0.5,
            threshold_max_km=5.0,
            min_actor_count=3,
            required_actors=["Iran und Verbündete", "USA und Israel im Iran", "Israel im Libanon"],
            now=datetime(2026, 3, 17, 6, 0, 0),
        )

        assert result["selected_threshold_km"] == 1.0
        assert result["combined_output_rows"] == 3
        assert result["zero_coordinate_rows"] == 2

        output_files = sorted(path.name for path in reducer.OUTPUT_DIR.iterdir() if path.is_file())
        assert output_files == [
            "2026_03_17_iran_und_verbuendete_attacken.reduced_for_datawrapper.csv",
            "2026_03_17_israel_im_libanon_attacken.reduced_for_datawrapper.csv",
            "2026_03_17_usa_und_israel_im_iran_attacken.reduced_for_datawrapper.csv",
        ]
        archived_output_files = sorted(
            path.name for path in reducer.OLD_OUTPUT_DIR.iterdir() if path.is_file()
        )
        assert archived_output_files == ["2026_03_16_legacy_attacken.reduced_for_datawrapper.csv"]

        latest_summary = (reducer.LOG_DIR / "latest_run_summary.txt").read_text(encoding="utf-8")
        assert "Rows excluded for zero coordinates: 2" in latest_summary
        assert "coordinate_out_of_range: 1" in latest_summary
        assert "Israel im Libanon: 1" in latest_summary
        assert "USA und Israel im Iran: 2" in latest_summary
    finally:
        reducer.WORKSPACE_DIR = original_workspace
        reducer.INPUT_DIR = original_input
        reducer.OUTPUT_DIR = original_output
        reducer.OLD_OUTPUT_DIR = original_old_output
        reducer.LOG_DIR = original_logs
        reducer.CONFIG_PATH = original_config
