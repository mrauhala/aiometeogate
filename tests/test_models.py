"""Tests for the data models."""

import pytest

from aiometeogate import ParameterKey, Station, StationMetadata, is_valid_wigos_id
from aiometeogate.models import haversine_km, parse_iso_duration


def test_parameter_key_round_trip() -> None:
    key = ParameterKey.from_parameter_name("wind_speed:10.0:mean:PT10M")
    assert key == ParameterKey("wind_speed", 10.0, "mean", "PT10M")
    assert key.parameter_name == "wind_speed:10.0:mean:PT10M"
    assert str(key) == "wind_speed:10.0:mean:PT10M"
    assert key.period_seconds == 600


@pytest.mark.parametrize(
    ("level", "expected"),
    [(2, "2.0"), (0.05, "0.05"), (-1.6, "-1.6"), (-0.0, "0.0"), (118.8, "118.8")],
)
def test_parameter_key_level_format(level: float, expected: str) -> None:
    key = ParameterKey("air_pressure", level, "point", "PT0S")
    assert key.parameter_name == f"air_pressure:{expected}:point:PT0S"


def test_parameter_key_normalises_function() -> None:
    key = ParameterKey("air_temperature", 2.0, "MEAN", "PT1M")
    assert key.function == "mean"
    assert key == ParameterKey.from_parameter_name("air_temperature:2:mean:PT1M")


@pytest.mark.parametrize(
    "name", ["air_temperature", "air_temperature:2.0:mean", "a:b:mean:PT1M", ""]
)
def test_parameter_key_invalid(name: str) -> None:
    with pytest.raises(ValueError, match="Invalid parameter name"):
        ParameterKey.from_parameter_name(name)


def test_parameter_keys_sort() -> None:
    keys = [
        ParameterKey("wind_speed", 10.0, "mean", "PT2M"),
        ParameterKey("air_temperature", 2.0, "mean", "PT1M"),
    ]
    assert min(keys).standard_name == "air_temperature"


@pytest.mark.parametrize(
    ("duration", "seconds"),
    [
        ("PT0S", 0),
        ("PT1M", 60),
        ("PT10M", 600),
        ("PT24M", 1440),
        ("PT1H", 3600),
        ("PT12H", 43200),
        ("P2D", 172800),
        ("P1W", 604800),
        ("P1DT1H", 90000),
        ("P", None),
        ("PT", None),
        ("P1DT", None),
        ("P1Y", None),
        ("PT1.5S", None),
        ("10M", None),
    ],
)
def test_parse_iso_duration(duration: str, seconds: int | None) -> None:
    assert parse_iso_duration(duration) == seconds


def test_distance() -> None:
    # Helsinki Kaisaniemi to Oslo Blindern.
    assert haversine_km(60.1752, 24.9446, 59.9423, 10.72) == pytest.approx(
        788.3, abs=0.5
    )
    station = Station(
        id="0-246-0-100971", name="Kaisaniemi", latitude=60.1752, longitude=24.9446
    )
    assert station.distance_km(60.1752, 24.9446) == 0


def test_station_metadata_name() -> None:
    assert StationMetadata(station_id="x", title="T", platform_name="P").name == "P"
    assert StationMetadata(station_id="x", title="T").name == "T"
    assert StationMetadata(station_id="x").name == "x"


@pytest.mark.parametrize(
    ("station_id", "valid"),
    [
        ("0-246-0-100971", True),
        ("0-20000-0-01492", True),
        ("0-348-1-13711", True),
        ("0-0-0-115", True),
        ("0-578-0-SN18700", True),
        ("1-246-0-100971", False),
        ("0-246-0-", False),
        ("0-246-0-+", False),
        ("0-246-0-1/2", False),
        ("0-246-0-#", False),
        ("0-246-0-12345678901234567", False),
        ("", False),
    ],
)
def test_is_valid_wigos_id(station_id: str, *, valid: bool) -> None:
    assert is_valid_wigos_id(station_id) is valid
