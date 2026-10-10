"""Tests for the MQTT and EDR parsers."""

import base64
import copy
import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from aiometeogate import (
    MeteoGateParseError,
    ParameterKey,
    normalize_value,
    parse_coverage_collection,
    parse_items,
    parse_locations,
    parse_notification,
)
from aiometeogate.parsing import parse_datetime

from .conftest import FMI_STATION, MET_STATION, load_json, load_notification

AIR_TEMPERATURE = ParameterKey("air_temperature", 2.0, "point", "PT0S")
SNOW_DEPTH = ParameterKey("surface_snow_thickness", 0.0, "point", "PT0S")


def _fmi_payload() -> dict[str, Any]:
    payload: dict[str, Any] = load_json("mqtt/notification_fmi.json")["payload"]
    return payload


@pytest.mark.parametrize(
    "name", ["fmi", "met", "knmi", "meteofrance", "hungaromet", "missing_marker"]
)
def test_parse_notification(name: str, snapshot: SnapshotAssertion) -> None:
    topic, payload = load_notification(name)
    assert parse_notification(payload, topic=topic) == snapshot


def test_parse_notification_input_types(fmi_notification: tuple[str, bytes]) -> None:
    topic, payload = fmi_notification
    from_bytes = parse_notification(payload, topic=topic)
    assert parse_notification(payload.decode(), topic=topic) == from_bytes
    assert parse_notification(json.loads(payload)) == from_bytes
    assert from_bytes.key == ParameterKey("wind_speed", 10.0, "mean", "PT10M")
    assert from_bytes.station_name == "Helsinki Kaisaniemi"
    assert from_bytes.institution == "Finnish Meteorological Institute (FMI)"


def test_parse_notification_station_name_prefers_platform_name() -> None:
    # KNMI uses the title for the timeseries, not for the station.
    topic, payload = load_notification("knmi")
    assert parse_notification(payload, topic=topic).station_name == "De Bilt"


def test_parse_notification_institution_falls_back_to_creator() -> None:
    payload = _fmi_payload()
    del payload["properties"]["institution"]
    observation = parse_notification(payload)
    assert observation.institution == "Finnish Meteorological Institute"


def test_parse_notification_topic_mismatch(
    fmi_notification: tuple[str, bytes],
) -> None:
    _, payload = fmi_notification
    with pytest.raises(MeteoGateParseError, match="does not match"):
        parse_notification(payload, topic="fi/fmi/0-246-0-101004/wind_speed")


def test_parse_notification_standard_name_from_topic() -> None:
    payload = _fmi_payload()
    del payload["properties"]["content"]["standard_name"]
    observation = parse_notification(
        payload, topic=f"fi/fmi/{FMI_STATION}/air_temperature"
    )
    assert observation.key.standard_name == "air_temperature"


def test_parse_notification_base64_value() -> None:
    payload = _fmi_payload()
    content = payload["properties"]["content"]
    content["encoding"] = "base64"
    content["value"] = base64.b64encode(b"12.5").decode()
    assert parse_notification(payload).value == 12.5


@pytest.mark.parametrize(
    ("change", "match"),
    [
        (lambda p: p.pop("properties"), "Invalid notification"),
        (lambda p: p["properties"].pop("content"), "Invalid notification"),
        (lambda p: p["properties"].pop("level"), "Invalid notification"),
        (lambda p: p["properties"].update(level="high"), "Invalid notification"),
        (lambda p: p["properties"].update(datetime="soon"), "Invalid notification"),
        (lambda p: p["properties"].update(content=[]), "Invalid notification"),
        (lambda p: p["properties"].update(platform=None), "Missing station"),
        (
            lambda p: p["properties"]["content"].pop("standard_name"),
            "Missing standard name",
        ),
        (
            lambda p: p["properties"]["content"].update(value={"v": 1}),
            "Unsupported value type",
        ),
        (
            lambda p: p["properties"]["content"].update(
                encoding="base64", value="not base64!"
            ),
            "Invalid base64",
        ),
    ],
)
def test_parse_notification_invalid(change: Any, match: str) -> None:
    payload = _fmi_payload()
    change(payload)
    with pytest.raises(MeteoGateParseError, match=match):
        parse_notification(payload)


@pytest.mark.parametrize("payload", [b"{not json", b"\xff\xfe", b"[1, 2]", b"null"])
def test_parse_notification_invalid_json(payload: bytes) -> None:
    with pytest.raises(MeteoGateParseError):
        parse_notification(payload)


def test_parse_notification_without_geometry_or_pubtime() -> None:
    payload = _fmi_payload()
    del payload["geometry"]
    del payload["properties"]["pubtime"]
    observation = parse_notification(payload)
    assert observation.latitude is None
    assert observation.longitude is None
    assert observation.published_at is None


@pytest.mark.parametrize(
    ("key", "raw", "expected"),
    [
        (AIR_TEMPERATURE, "5.5", 5.5),
        (AIR_TEMPERATURE, " -3 ", -3.0),
        (AIR_TEMPERATURE, 7, 7.0),
        (AIR_TEMPERATURE, 7.25, 7.25),
        (AIR_TEMPERATURE, None, None),
        (AIR_TEMPERATURE, True, None),
        (AIR_TEMPERATURE, "", None),
        (AIR_TEMPERATURE, "abc", None),
        (AIR_TEMPERATURE, "NaN", None),
        (AIR_TEMPERATURE, "inf", None),
        (AIR_TEMPERATURE, "-32767", None),
        (AIR_TEMPERATURE, -32766.0, None),
        (AIR_TEMPERATURE, "-9999", None),
        (AIR_TEMPERATURE, "-1", -1.0),
        (SNOW_DEPTH, "-1", 0.0),
        (SNOW_DEPTH, "-5", None),
        (SNOW_DEPTH, "12", 12.0),
        (SNOW_DEPTH, "0", 0.0),
    ],
)
def test_normalize_value(
    key: ParameterKey, raw: object, expected: float | None
) -> None:
    assert normalize_value(key, raw) == expected


def test_parse_datetime() -> None:
    assert parse_datetime("2026-10-10T05:40:00.000000Z") == datetime(
        2026, 10, 10, 5, 40, tzinfo=UTC
    )
    assert parse_datetime("2026-10-10T05:40:00") == datetime(
        2026, 10, 10, 5, 40, tzinfo=UTC
    )
    converted = parse_datetime("2026-10-10T08:40:00+03:00")
    assert converted == datetime(2026, 10, 10, 5, 40, tzinfo=UTC)
    assert converted.tzinfo is UTC


@pytest.mark.parametrize(
    ("name", "station_id"), [("fmi", FMI_STATION), ("met", MET_STATION)]
)
def test_parse_coverage_collection(
    name: str, station_id: str, snapshot: SnapshotAssertion
) -> None:
    observations = parse_coverage_collection(
        load_json(f"edr/coverage_{name}.json"), station_id
    )
    assert observations == snapshot


def _coverage(
    times: list[str], values: list[Any], name: str = "air_temperature:2.0:point:PT0S"
) -> dict[str, Any]:
    return {
        "type": "Coverage",
        "domain": {
            "axes": {
                "x": {"values": [24.9]},
                "y": {"values": [60.2]},
                "t": {"values": times},
            }
        },
        "parameters": {name: {"unit": {"symbol": {"value": "°C"}}}},
        "ranges": {name: {"values": values}},
    }


def test_parse_coverage_skips_trailing_nulls() -> None:
    data = _coverage(
        ["2026-10-10T05:00:00Z", "2026-10-10T05:10:00Z", "2026-10-10T05:20:00Z"],
        [1.0, 2.0, None],
    )
    (observation,) = parse_coverage_collection(data, FMI_STATION).values()
    assert observation.value == 2.0
    assert observation.observed_at == datetime(2026, 10, 10, 5, 10, tzinfo=UTC)
    assert observation.unit == "°C"
    assert (observation.latitude, observation.longitude) == (60.2, 24.9)


def test_parse_coverage_skips_empty_ranges() -> None:
    data = _coverage(["2026-10-10T05:00:00Z"], [None])
    assert parse_coverage_collection(data, FMI_STATION) == {}


def test_parse_coverage_keeps_newest_of_duplicates() -> None:
    older = _coverage(["2026-10-10T05:00:00Z"], [1.0])
    newer = _coverage(["2026-10-10T06:00:00Z"], [2.0])
    collection = {"type": "CoverageCollection", "coverages": [newer, older]}
    (observation,) = parse_coverage_collection(collection, FMI_STATION).values()
    assert observation.value == 2.0


def test_parse_coverage_shared_parameters_and_missing_unit() -> None:
    coverage = _coverage(["2026-10-10T05:00:00Z"], [1.0])
    del coverage["parameters"]
    collection: dict[str, Any] = {
        "type": "CoverageCollection",
        "coverages": [coverage],
        "parameters": {},
    }
    (observation,) = parse_coverage_collection(collection, FMI_STATION).values()
    assert observation.unit == ""


@pytest.mark.parametrize(
    "unit",
    [None, "hPa", {"label": "hPa"}, {"symbol": {"value": 1}}],
)
def test_parse_coverage_unusual_units(unit: Any) -> None:
    coverage = _coverage(["2026-10-10T05:00:00Z"], [1.0])
    coverage["parameters"]["air_temperature:2.0:point:PT0S"] = {"unit": unit}
    (observation,) = parse_coverage_collection(coverage, FMI_STATION).values()
    assert observation.unit == ""


def test_parse_coverage_invalid() -> None:
    broken = _coverage(["2026-10-10T05:00:00Z"], [1.0])
    del broken["domain"]
    with pytest.raises(MeteoGateParseError, match="Invalid coverage"):
        parse_coverage_collection(broken, FMI_STATION)

    bad_name = _coverage(["2026-10-10T05:00:00Z"], [1.0], name="temperature")
    with pytest.raises(MeteoGateParseError, match="Invalid range"):
        parse_coverage_collection(bad_name, FMI_STATION)


def test_parse_coverage_empty_collection() -> None:
    assert parse_coverage_collection({"type": "CoverageCollection"}, FMI_STATION) == {}


def test_parse_locations() -> None:
    stations = parse_locations(load_json("edr/locations_helsinki.json"))
    assert len(stations) == 8
    kaisaniemi = next(s for s in stations if s.id == FMI_STATION)
    assert kaisaniemi.name == "Helsinki Kaisaniemi"
    assert kaisaniemi.latitude == pytest.approx(60.1752)
    assert kaisaniemi.longitude == pytest.approx(24.9446)
    assert "air_temperature:2.0:mean:PT1M" in kaisaniemi.parameter_names


def test_parse_locations_skips_malformed() -> None:
    data = {
        "features": [
            "nonsense",
            {"id": None, "geometry": {"coordinates": [1, 2]}},
            {"id": "0-1-0-1", "geometry": {"coordinates": [1]}},
            {"id": "0-1-0-2", "geometry": {"coordinates": "x"}},
            {"id": "0-1-0-3", "geometry": None},
            {"id": "0-1-0-4", "geometry": {"coordinates": {"lat": "x"}}},
            {"id": "0-1-0-5", "geometry": {"coordinates": [24.0, 60.0]}},
        ]
    }
    (station,) = parse_locations(data)
    assert station.id == "0-1-0-5"
    assert station.name == "0-1-0-5"
    assert station.parameter_names == ()


@pytest.mark.parametrize(
    ("name", "station_id"), [("fmi", FMI_STATION), ("met", MET_STATION)]
)
def test_parse_items(name: str, station_id: str, snapshot: SnapshotAssertion) -> None:
    assert parse_items(load_json(f"edr/items_{name}.json"), station_id) == snapshot


def test_parse_items_skips_bad_timeseries() -> None:
    data = copy.deepcopy(load_json("edr/items_fmi.json"))
    data["features"][0]["properties"]["parameter_name"] = "broken"
    metadata = parse_items(data, FMI_STATION)
    assert len(metadata.timeseries) == len(data["features"]) - 1


def test_parse_items_unknown_station() -> None:
    with pytest.raises(MeteoGateParseError, match="No items"):
        parse_items(load_json("edr/items_fmi.json"), MET_STATION)
    with pytest.raises(MeteoGateParseError, match="No items"):
        parse_items({"features": []}, FMI_STATION)


def test_parse_datetime_keeps_other_zones_comparable() -> None:
    eastern = datetime(2026, 1, 1, 3, tzinfo=timezone(timedelta(hours=3)))
    assert parse_datetime(eastern.isoformat()) == datetime(2026, 1, 1, tzinfo=UTC)
