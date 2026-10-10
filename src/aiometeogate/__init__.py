"""Asynchronous client for EUMETNET MeteoGate weather observations."""

from .const import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    EDR_BASE_URL,
    Endpoint,
    is_valid_wigos_id,
)
from .edr import MeteoGateEdrClient
from .exceptions import (
    MeteoGateConnectionError,
    MeteoGateError,
    MeteoGateParseError,
    MeteoGateResponseError,
    MeteoGateStationNotFoundError,
)
from .models import (
    Observation,
    ParameterKey,
    Station,
    StationMetadata,
    TimeseriesInfo,
)
from .mqtt import MeteoGateMqttClient, MqttStats, station_topic
from .parsing import (
    normalize_value,
    parse_coverage_collection,
    parse_items,
    parse_locations,
    parse_notification,
)

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "EDR_BASE_URL",
    "Endpoint",
    "MeteoGateConnectionError",
    "MeteoGateEdrClient",
    "MeteoGateError",
    "MeteoGateMqttClient",
    "MeteoGateParseError",
    "MeteoGateResponseError",
    "MeteoGateStationNotFoundError",
    "MqttStats",
    "Observation",
    "ParameterKey",
    "Station",
    "StationMetadata",
    "TimeseriesInfo",
    "__version__",
    "is_valid_wigos_id",
    "normalize_value",
    "parse_coverage_collection",
    "parse_items",
    "parse_locations",
    "parse_notification",
    "station_topic",
]
