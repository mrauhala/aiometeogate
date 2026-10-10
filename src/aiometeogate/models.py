"""Data models for MeteoGate observations and stations."""

import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Self

_EARTH_RADIUS_KM = 6371.0088

_DURATION_PATTERN = re.compile(
    r"^P(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$"
)


def parse_iso_duration(value: str) -> int | None:
    """Return the length of an ISO 8601 duration in seconds.

    Only weeks, days, hours, minutes and whole seconds are supported, since years
    and months have no fixed length. Returns None for anything else.
    """
    match = _DURATION_PATTERN.fullmatch(value)
    if match is None or value in {"P", "PT"} or value.endswith("T"):
        return None
    parts = {name: int(amount) for name, amount in match.groupdict("0").items()}
    return (
        parts["weeks"] * 604800
        + parts["days"] * 86400
        + parts["hours"] * 3600
        + parts["minutes"] * 60
        + parts["seconds"]
    )


def format_level(level: float) -> str:
    """Format a level in metres the way MeteoGate writes it, e.g. "2.0"."""
    return repr(level)


@dataclass(frozen=True, slots=True, order=True)
class ParameterKey:
    """Identifies one timeseries of a station.

    One MQTT topic can carry several variants of the same quantity, for example a
    2-minute and a 10-minute mean wind speed, so the key has all four parts that
    MeteoGate uses in its parameter names.
    """

    standard_name: str
    """CF standard name, e.g. "air_temperature"."""

    level: float
    """Height above ground in metres; negative below ground."""

    function: str
    """Aggregation method, e.g. "point", "mean", "maximum" or "sum"."""

    period: str
    """Aggregation period as an ISO 8601 duration, exactly as published."""

    def __post_init__(self) -> None:
        """Normalise the level and the function name."""
        # -0.0 and 0.0 must give the same key and parameter name.
        object.__setattr__(self, "level", float(self.level) + 0.0)
        object.__setattr__(self, "function", self.function.lower())

    @classmethod
    def from_parameter_name(cls, parameter_name: str) -> Self:
        """Create a key from a name such as "air_temperature:2.0:mean:PT1M"."""
        try:
            standard_name, level, function, period = parameter_name.split(":")
            return cls(standard_name, float(level), function, period)
        except ValueError as err:
            msg = f"Invalid parameter name: {parameter_name!r}"
            raise ValueError(msg) from err

    @property
    def parameter_name(self) -> str:
        """Return the name in the same format as the MeteoGate EDR API."""
        return (
            f"{self.standard_name}:{format_level(self.level)}"
            f":{self.function}:{self.period}"
        )

    @property
    def period_seconds(self) -> int | None:
        """Return the aggregation period in seconds, or None if it is unknown."""
        return parse_iso_duration(self.period)

    def __str__(self) -> str:
        """Return the parameter name."""
        return self.parameter_name


@dataclass(frozen=True, slots=True, kw_only=True)
class Observation:
    """A single observed value of one timeseries."""

    station_id: str
    key: ParameterKey
    value: float | None
    """The value in `unit`, or None if the provider marked it as missing."""
    raw_value: str | float | None
    unit: str
    """Unit as published, e.g. "Cel", "m/s" or "kg/m2"."""
    observed_at: datetime
    """Time of the observation (UTC)."""
    published_at: datetime | None = None
    station_name: str | None = None
    institution: str | None = None
    naming_authority: str | None = None
    latitude: float | None = None
    longitude: float | None = None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance between two points in kilometres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 2 * _EARTH_RADIUS_KM * math.asin(math.sqrt(a))


@dataclass(frozen=True, slots=True, kw_only=True)
class Station:
    """A station as listed by the EDR locations endpoint."""

    id: str
    name: str
    latitude: float
    longitude: float
    parameter_names: tuple[str, ...] = ()

    def distance_km(self, latitude: float, longitude: float) -> float:
        """Return the distance from the given point in kilometres."""
        return haversine_km(latitude, longitude, self.latitude, self.longitude)


@dataclass(frozen=True, slots=True, kw_only=True)
class TimeseriesInfo:
    """Metadata of one timeseries of a station."""

    key: ParameterKey
    unit: str | None = None
    timeseries_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class StationMetadata:
    """Station metadata from the EDR items endpoint."""

    station_id: str
    title: str | None = None
    platform_name: str | None = None
    institution: str | None = None
    naming_authority: str | None = None
    license: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    timeseries: tuple[TimeseriesInfo, ...] = field(default=())

    @property
    def name(self) -> str:
        """Return the best available display name."""
        return self.platform_name or self.title or self.station_id
