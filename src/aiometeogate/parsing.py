"""Parsers for MeteoGate MQTT notifications and EDR responses."""

import base64
import binascii
import json
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from .const import MISSING_VALUE_MARKERS
from .exceptions import MeteoGateParseError
from .models import (
    Observation,
    ParameterKey,
    Station,
    StationMetadata,
    TimeseriesInfo,
)

_SNOW_DEPTH = "surface_snow_thickness"
# country/institution/station/standard_name
_TOPIC_LEVELS = 4


def parse_datetime(value: str) -> datetime:
    """Parse an ISO 8601 timestamp; a timestamp without a zone is taken as UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def normalize_value(key: ParameterKey, raw: object) -> float | None:
    """Convert a published value to a float, or None if it is missing."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int | float):
        value = float(raw)
    else:
        text = str(raw).strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError:
            return None
    if not math.isfinite(value) or value in MISSING_VALUE_MARKERS:
        return None
    if key.standard_name == _SNOW_DEPTH and value < 0:
        # FMI publishes -1 for "no snow cover"; other negative depths are invalid.
        return 0.0 if value == -1 else None
    return value


def _decode_json(payload: bytes | str | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        return payload
    try:
        data = json.loads(payload)
    except (UnicodeDecodeError, ValueError) as err:
        raise MeteoGateParseError(f"Invalid JSON: {err}") from err
    if not isinstance(data, Mapping):
        raise MeteoGateParseError("Expected a JSON object")
    return data


def _coordinates(geometry: object) -> tuple[float | None, float | None]:
    """Return (latitude, longitude) from a GeoJSON-like geometry.

    MQTT notifications use an object {"lat": .., "lon": ..} while EDR responses
    use a standard [longitude, latitude] list.
    """
    if not isinstance(geometry, Mapping):
        return None, None
    coordinates = geometry.get("coordinates")
    try:
        if isinstance(coordinates, Mapping):
            return float(coordinates["lat"]), float(coordinates["lon"])
        if isinstance(coordinates, list | tuple) and len(coordinates) >= 2:  # noqa: PLR2004
            return float(coordinates[1]), float(coordinates[0])
    except (KeyError, TypeError, ValueError):
        pass
    return None, None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _station_name(properties: Mapping[str, Any]) -> str | None:
    # Some providers (e.g. KNMI) use "title" for the timeseries, not the station.
    return _optional_str(properties.get("platform_name")) or _optional_str(
        properties.get("title")
    )


def _institution(properties: Mapping[str, Any]) -> str | None:
    return _optional_str(properties.get("institution")) or _optional_str(
        properties.get("creator_name")
    )


def _content_value(content: Mapping[str, Any]) -> str | float | None:
    value = content.get("value")
    if content.get("encoding") == "base64" and isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True).decode()
        except (binascii.Error, UnicodeDecodeError) as err:
            raise MeteoGateParseError(f"Invalid base64 value: {err}") from err
    if value is None or isinstance(value, str | int | float):
        return value
    raise MeteoGateParseError(f"Unsupported value type: {type(value).__name__}")


def parse_notification(
    payload: bytes | str | Mapping[str, Any], *, topic: str | None = None
) -> Observation:
    """Parse an MQTT notification message into an observation.

    If the topic is given, the station identifier in the topic must match the
    station in the message.
    """
    data = _decode_json(payload)
    topic_parts = topic.split("/") if topic else []
    try:
        properties = data["properties"]
        content = properties["content"]
        standard_name = content.get("standard_name") or (
            topic_parts[3] if len(topic_parts) == _TOPIC_LEVELS else None
        )
        station_id = properties["platform"]
        level = float(properties["level"])
        function = str(properties["function"])
        period = str(properties["period"])
        observed_at = parse_datetime(properties["datetime"])
        pubtime = properties.get("pubtime")
        published_at = parse_datetime(pubtime) if isinstance(pubtime, str) else None
    except (KeyError, TypeError, ValueError, AttributeError) as err:
        raise MeteoGateParseError(f"Invalid notification: {err!r}") from err

    if not isinstance(standard_name, str) or not standard_name:
        raise MeteoGateParseError("Missing standard name")
    if not isinstance(station_id, str) or not station_id:
        raise MeteoGateParseError("Missing station")
    if len(topic_parts) == _TOPIC_LEVELS and topic_parts[2] != station_id:
        raise MeteoGateParseError(
            f"Topic {topic!r} does not match station {station_id!r}"
        )
    key = ParameterKey(standard_name, level, function, period)
    raw_value = _content_value(content)
    latitude, longitude = _coordinates(data.get("geometry"))
    return Observation(
        station_id=station_id,
        key=key,
        value=normalize_value(key, raw_value),
        raw_value=raw_value,
        unit=str(content.get("unit") or ""),
        observed_at=observed_at,
        published_at=published_at,
        station_name=_station_name(properties),
        institution=_institution(properties),
        naming_authority=_optional_str(properties.get("naming_authority")),
        latitude=latitude,
        longitude=longitude,
    )


def _unit_label(parameter: object) -> str:
    """Return the unit label of a CoverageJSON parameter, e.g. "Cel"."""
    if not isinstance(parameter, Mapping):
        return ""
    unit = parameter.get("unit")
    if not isinstance(unit, Mapping):
        return ""
    label = unit.get("label")
    if isinstance(label, Mapping) and isinstance(label.get("en"), str):
        return str(label["en"])
    symbol = unit.get("symbol")
    if isinstance(symbol, Mapping) and isinstance(symbol.get("value"), str):
        return str(symbol["value"])
    return ""


def _latest_in_range(
    times: list[Any], values: list[Any]
) -> tuple[datetime, float | str | None] | None:
    """Return the newest time and value that is not null."""
    for time, value in zip(reversed(times), reversed(values), strict=False):
        if value is not None:
            return parse_datetime(time), value
    return None


def parse_coverage_collection(
    data: Mapping[str, Any], station_id: str
) -> dict[ParameterKey, Observation]:
    """Return the newest observation of every parameter in a CoverageJSON response.

    The EDR API groups parameters that share a time axis into one coverage.
    """
    if data.get("type") == "Coverage":
        coverages: list[Any] = [data]
    else:
        coverages = data.get("coverages") or []
    shared_parameters = data.get("parameters") or {}

    latest: dict[ParameterKey, Observation] = {}
    for coverage in coverages:
        try:
            axes = coverage["domain"]["axes"]
            times = axes["t"]["values"]
            longitude = float(axes["x"]["values"][0])
            latitude = float(axes["y"]["values"][0])
            ranges = coverage["ranges"]
        except (KeyError, IndexError, TypeError, ValueError) as err:
            raise MeteoGateParseError(f"Invalid coverage: {err!r}") from err
        parameters = coverage.get("parameters") or shared_parameters
        for name, coverage_range in ranges.items():
            try:
                key = ParameterKey.from_parameter_name(name)
                newest = _latest_in_range(times, coverage_range["values"])
            except (KeyError, TypeError, ValueError) as err:
                raise MeteoGateParseError(f"Invalid range {name!r}: {err!r}") from err
            if newest is None:
                continue
            observed_at, raw_value = newest
            previous = latest.get(key)
            if previous is not None and previous.observed_at >= observed_at:
                continue
            latest[key] = Observation(
                station_id=station_id,
                key=key,
                value=normalize_value(key, raw_value),
                raw_value=raw_value,
                unit=_unit_label(parameters.get(name)),
                observed_at=observed_at,
                latitude=latitude,
                longitude=longitude,
            )
    return latest


def parse_locations(data: Mapping[str, Any]) -> list[Station]:
    """Parse the EDR locations response; malformed entries are skipped."""
    stations: list[Station] = []
    for feature in data.get("features") or []:
        if not isinstance(feature, Mapping):
            continue
        station_id = feature.get("id")
        latitude, longitude = _coordinates(feature.get("geometry"))
        if not isinstance(station_id, str) or latitude is None or longitude is None:
            continue
        properties = feature.get("properties") or {}
        parameter_names = properties.get("parameter-name") or []
        stations.append(
            Station(
                id=station_id,
                name=_optional_str(properties.get("name")) or station_id,
                latitude=latitude,
                longitude=longitude,
                parameter_names=tuple(p for p in parameter_names if isinstance(p, str)),
            )
        )
    return stations


def parse_items(data: Mapping[str, Any], station_id: str) -> StationMetadata:
    """Parse the EDR items response for one station."""
    features = [
        feature
        for feature in data.get("features") or []
        if isinstance(feature, Mapping)
        and isinstance(feature.get("properties"), Mapping)
        and feature["properties"].get("platform") == station_id
    ]
    if not features:
        raise MeteoGateParseError(f"No items for station {station_id}")

    timeseries: dict[ParameterKey, TimeseriesInfo] = {}
    for feature in features:
        properties = feature["properties"]
        try:
            key = ParameterKey.from_parameter_name(properties["parameter_name"])
        except (KeyError, TypeError, ValueError):
            continue
        timeseries[key] = TimeseriesInfo(
            key=key,
            unit=_optional_str(properties.get("unit")),
            timeseries_id=_optional_str(properties.get("timeseries_id")),
        )

    first = features[0]["properties"]
    latitude, longitude = _coordinates(features[0].get("geometry"))
    return StationMetadata(
        station_id=station_id,
        title=_optional_str(first.get("title")),
        platform_name=_optional_str(first.get("platform_name")),
        institution=_institution(first),
        naming_authority=_optional_str(first.get("naming_authority")),
        license=_optional_str(first.get("license")),
        latitude=latitude,
        longitude=longitude,
        timeseries=tuple(timeseries[key] for key in sorted(timeseries)),
    )
