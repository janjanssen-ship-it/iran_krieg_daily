#!/usr/bin/env python3
"""Upload the latest reduced Iran-Krieg CSV outputs to a Datawrapper locator map."""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = REPO_ROOT / "data" / "iran_krieg_daily"
OUTPUT_DIR = WORKSPACE_DIR / "output"
ENV_PATH = REPO_ROOT / ".env"

DEFAULT_DATAWRAPPER_API_ROOT = "https://api.datawrapper.de/v3"
USER_AGENT = "Mozilla/5.0 (compatible; Iran-Krieg-Datawrapper-Uploader/1.0)"

ACTOR_TO_GROUP_TITLE = {
    "Israel im Libanon": "Israel Libanon",
    "Iran und Verbündete": "Iran",
    "USA und Israel im Iran": "USA",
}

GROUP_TO_MARKER_COLOR = {
    "Israel Libanon": "#2659FF",
    "Iran": "#CA0016",
    "USA": "#0D2880",
}

GERMAN_MONTHS = {
    1: "Januar",
    2: "Februar",
    3: "März",
    4: "April",
    5: "Mai",
    6: "Juni",
    7: "Juli",
    8: "August",
    9: "September",
    10: "Oktober",
    11: "November",
    12: "Dezember",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--chart-id",
        help="Explicit Datawrapper chart id override. Defaults to the production chart.",
    )
    parser.add_argument(
        "--date",
        help="Upload a specific YYYY_MM_DD run instead of the latest output set.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and validate the upload payload without sending it to Datawrapper.",
    )
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def resolve_access_token() -> str:
    load_env_file(ENV_PATH)
    token = os.environ.get("DATAWRAPPER_ACCESS_TOKEN", "").strip()
    if not token:
        raise ValueError("DATAWRAPPER_ACCESS_TOKEN is required in the environment or .env.")
    return token


def resolve_api_root() -> str:
    load_env_file(ENV_PATH)
    return os.environ.get("DATAWRAPPER_API_ROOT", "").strip() or DEFAULT_DATAWRAPPER_API_ROOT


def resolve_chart_id(explicit_chart_id: str | None) -> str:
    load_env_file(ENV_PATH)
    if explicit_chart_id:
        return explicit_chart_id.strip()
    env_value = os.environ.get("DATAWRAPPER_CHART_ID", "").strip()
    if env_value:
        return env_value
    raise ValueError("DATAWRAPPER_CHART_ID is required in the environment, .env, or via --chart-id.")


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
    pieces: list[str] = []
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


def extract_date_prefix(name: str) -> str | None:
    match = re.match(r"^(\d{4}_\d{2}_\d{2})_", name)
    return match.group(1) if match else None


def discover_date_prefix(output_dir: Path, requested_date: str | None) -> str:
    if requested_date:
        if not re.fullmatch(r"\d{4}_\d{2}_\d{2}", requested_date):
            raise ValueError("Please pass --date in YYYY_MM_DD format.")
        return requested_date

    prefixes = sorted(
        {
            prefix
            for path in output_dir.glob("*.reduced_for_datawrapper.csv")
            if (prefix := extract_date_prefix(path.name))
        }
    )
    if not prefixes:
        raise ValueError(f"No reduced output CSVs found in {output_dir}.")
    return prefixes[-1]


def build_output_paths(date_prefix: str) -> dict[str, Path]:
    output_paths: dict[str, Path] = {}
    for actor in ACTOR_TO_GROUP_TITLE:
        filename = f"{date_prefix}_{build_actor_output_filename(actor)}"
        path = OUTPUT_DIR / filename
        if not path.exists():
            raise ValueError(f"Expected output file not found: {path}")
        output_paths[actor] = path
    return output_paths


def load_actor_rows(path: Path, expected_actor: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", newline="", encoding="utf-8-sig") as fp:
        reader = csv.DictReader(fp)
        expected_fields = {"Time", "Actor", "Latitude", "Longitude", "group_size"}
        if not reader.fieldnames or not expected_fields.issubset(set(reader.fieldnames)):
            raise ValueError(f"{path} must include {sorted(expected_fields)}.")
        for row in reader:
            actor = str(row.get("Actor", "")).strip()
            if actor != expected_actor:
                raise ValueError(f"{path} contains unexpected actor {actor!r}.")
            rows.append(
                {
                    "Time": str(row.get("Time", "")).strip(),
                    "Actor": actor,
                    "Latitude": float(str(row.get("Latitude", "")).strip()),
                    "Longitude": float(str(row.get("Longitude", "")).strip()),
                    "group_size": int(float(str(row.get("group_size", "")).strip())),
                }
            )
    return rows


def load_rows_by_actor(date_prefix: str) -> dict[str, list[dict[str, Any]]]:
    return {
        actor: load_actor_rows(path, actor)
        for actor, path in build_output_paths(date_prefix).items()
    }


def request_json(url: str, *, token: str, method: str = "GET", payload: Any | None = None) -> Any:
    data: bytes | None = None
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": USER_AGENT,
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, headers=headers, method=method, data=data)
    try:
        with urlopen(request) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            body = response.read().decode(charset, errors="replace")
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise ValueError(f"HTTP {exc.code} for {method} {url}: {details}") from exc
    except URLError as exc:
        raise ValueError(f"Network error for {method} {url}: {exc.reason}") from exc
    if not body.strip():
        return None
    return json.loads(body)


def fetch_chart(chart_id: str, *, token: str) -> dict[str, Any]:
    data = request_json(f"{resolve_api_root()}/charts/{chart_id}", token=token)
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected chart metadata response for {chart_id}.")
    return data


def fetch_chart_data(chart_id: str, *, token: str) -> dict[str, Any]:
    data = request_json(f"{resolve_api_root()}/charts/{chart_id}/data", token=token)
    if not isinstance(data, dict) or not isinstance(data.get("markers"), list):
        raise ValueError(f"Unexpected chart data response for {chart_id}.")
    return data


def put_chart_data(chart_id: str, payload: dict[str, Any], *, token: str) -> None:
    request_json(
        f"{resolve_api_root()}/charts/{chart_id}/data",
        token=token,
        method="PUT",
        payload=payload,
    )


def patch_chart_metadata(chart_id: str, payload: dict[str, Any], *, token: str) -> None:
    request_json(
        f"{resolve_api_root()}/charts/{chart_id}",
        token=token,
        method="PATCH",
        payload=payload,
    )


def publish_chart(chart_id: str, *, token: str) -> None:
    request_json(
        f"{resolve_api_root()}/charts/{chart_id}/publish",
        token=token,
        method="POST",
    )


def build_marker_id(actor: str, row: dict[str, Any], index: int) -> str:
    digest = hashlib.sha1(
        f"{actor}|{index}|{row['Time']}|{row['Latitude']}|{row['Longitude']}|{row['group_size']}".encode(
            "utf-8"
        )
    ).hexdigest()
    return digest[:10]


def select_group_objects(markers: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    group_objects: dict[str, dict[str, Any]] = {}
    templates: dict[str, dict[str, Any]] = {}
    group_ids: dict[str, str] = {}
    for marker in markers:
        if marker.get("type") == "group":
            title = str(marker.get("title", ""))
            group_objects[title] = marker
            group_ids[title] = str(marker.get("id", ""))
    for marker in markers:
        data = marker.get("data")
        group_id = str(marker.get("groupId", ""))
        if not data or not group_id:
            continue
        title = next((name for name, current_id in group_ids.items() if current_id == group_id), "")
        if title and title not in templates:
            templates[title] = marker
    return group_objects, templates


def build_fallback_template(group_title: str) -> dict[str, Any]:
    return {
        "type": "point",
        "title": "Marker\nText",
        "icon": {
            "id": "circle-sm",
            "path": "M1000 350a500 500 0 0 0-500-500 500 500 0 0 0-500 500 500 500 0 0 0 500 500 500 500 0 0 0 500-500z",
            "horiz-adv-x": 1000,
            "scale": 0.42,
            "height": 700,
            "width": 1000,
            "enabled": True,
        },
        "scale": 1,
        "markerColor": GROUP_TO_MARKER_COLOR[group_title],
        "markerSymbol": "",
        "anchor": "bottom-center",
        "offsetY": 0,
        "offsetX": 0,
        "labelStyle": "plain",
        "text": {
            "bold": False,
            "italic": False,
            "uppercase": False,
            "space": False,
            "color": "#333333",
            "fontSize": 14,
            "halo": "#f2f3f0",
            "enabled": False,
        },
        "class": "",
        "rotate": 0,
        "visible": True,
        "locked": False,
        "preset": "-",
        "visibility": {"mobile": True, "desktop": True},
        "tooltip": {"enabled": False, "text": ""},
        "connectorLine": {
            "enabled": False,
            "arrowHead": "lines",
            "type": "curveRight",
            "targetPadding": 3,
            "stroke": 1,
            "lineLength": 0,
        },
    }


def build_attack_markers(
    *,
    rows_by_actor: dict[str, list[dict[str, Any]]],
    group_objects: dict[str, dict[str, Any]],
    templates: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    built: dict[str, list[dict[str, Any]]] = {}
    for actor, group_title in ACTOR_TO_GROUP_TITLE.items():
        if group_title not in group_objects:
            raise ValueError(f"Destination chart is missing marker group {group_title!r}.")
        group_id = str(group_objects[group_title]["id"])
        template = templates.get(group_title, build_fallback_template(group_title))
        markers_for_group: list[dict[str, Any]] = []
        for index, row in enumerate(rows_by_actor[actor], start=1):
            marker = copy.deepcopy(template)
            marker["id"] = build_marker_id(actor, row, index)
            marker["groupId"] = group_id
            marker["coordinates"] = [row["Longitude"], row["Latitude"]]
            marker["data"] = {
                "Time": row["Time"],
                "Actor": row["Actor"],
                "Latitude": row["Latitude"],
                "Longitude": row["Longitude"],
                "group_size": row["group_size"],
            }
            markers_for_group.append(marker)
        built[group_title] = markers_for_group
    return built


def replace_attack_markers(
    current_data: dict[str, Any],
    attack_markers_by_group: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    markers = current_data.get("markers", [])
    new_markers: list[dict[str, Any]] = []
    inserted_groups: set[str] = set()

    for marker in markers:
        if marker.get("type") == "group":
            title = str(marker.get("title", ""))
            new_markers.append(copy.deepcopy(marker))
            if title in attack_markers_by_group:
                new_markers.extend(copy.deepcopy(item) for item in attack_markers_by_group[title])
                inserted_groups.add(title)
            continue
        if marker.get("data"):
            continue
        new_markers.append(copy.deepcopy(marker))

    missing = sorted(set(attack_markers_by_group) - inserted_groups)
    if missing:
        raise ValueError(f"Failed to insert marker groups: {', '.join(missing)}")

    payload = copy.deepcopy(current_data)
    payload["markers"] = new_markers
    return payload


def format_intro_date(date_prefix: str) -> str:
    target_date = datetime.strptime(date_prefix, "%Y_%m_%d").date() - timedelta(days=1)
    return f"{target_date.day}. {GERMAN_MONTHS[target_date.month]}"


def update_intro_text(current_intro: str, date_label: str) -> str:
    if "Stand:" in current_intro:
        return re.sub(r"(Stand:\s*)([^<]+)", rf"\g<1>{date_label}", current_intro, count=1)
    joiner = "<br> " if current_intro else ""
    return f"{current_intro}{joiner}Stand: {date_label}"


def build_metadata_patch(chart: dict[str, Any], *, date_prefix: str) -> dict[str, Any]:
    metadata = copy.deepcopy(chart.get("metadata", {}))
    describe = metadata.setdefault("describe", {})
    current_intro = str(describe.get("intro", ""))
    describe["intro"] = update_intro_text(current_intro, format_intro_date(date_prefix))
    return {"metadata": metadata}


def count_uploaded_markers(payload: dict[str, Any]) -> tuple[int, int]:
    markers = payload.get("markers", [])
    attack_count = sum(1 for marker in markers if marker.get("data"))
    manual_count = sum(1 for marker in markers if marker.get("type") != "group" and not marker.get("data"))
    return attack_count, manual_count


def run() -> int:
    args = parse_args()
    token = resolve_access_token()
    chart_id = resolve_chart_id(args.chart_id)
    date_prefix = discover_date_prefix(OUTPUT_DIR, args.date)
    rows_by_actor = load_rows_by_actor(date_prefix)

    chart = fetch_chart(chart_id, token=token)
    chart_data = fetch_chart_data(chart_id, token=token)
    group_objects, templates = select_group_objects(chart_data["markers"])
    attack_markers_by_group = build_attack_markers(
        rows_by_actor=rows_by_actor,
        group_objects=group_objects,
        templates=templates,
    )
    data_payload = replace_attack_markers(chart_data, attack_markers_by_group)
    metadata_patch = build_metadata_patch(chart, date_prefix=date_prefix)
    attack_count, manual_count = count_uploaded_markers(data_payload)
    intro_text = metadata_patch["metadata"]["describe"]["intro"]

    print(f"Chart id: {chart_id}")
    print(f"Run date: {date_prefix}")
    print(f"Attack markers: {attack_count}")
    print(f"Manual markers preserved: {manual_count}")
    print(f"Intro: {intro_text}")

    if args.dry_run:
        print("Dry run: no upload performed.")
        return 0

    put_chart_data(chart_id, data_payload, token=token)
    patch_chart_metadata(chart_id, metadata_patch, token=token)
    try:
        publish_chart(chart_id, token=token)
    except ValueError as exc:
        message = str(exc)
        if "HTTP 403" in message and f"/charts/{chart_id}/publish" in message:
            print("Upload complete. Publish skipped because the token lacks publish scope.")
            return 0
        raise
    print("Upload and publish complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
