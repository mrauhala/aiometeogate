"""Constants for the MeteoGate client."""

import re
from enum import StrEnum
from typing import Final

DEFAULT_HOST: Final = "observations.meteogate.eu"
DEFAULT_PORT: Final = 443
EDR_BASE_URL: Final = f"https://{DEFAULT_HOST}"

# The MeteoGate broker is public; these are the documented shared credentials.
PUBLIC_USERNAME: Final = "everyone"
PUBLIC_PASSWORD: Final = "everyone"  # noqa: S105


class Endpoint(StrEnum):
    """WebSocket paths of the MeteoGate MQTT broker."""

    PUBLIC = "/mqtt/"
    """All stations that have a WIGOS station identifier."""

    WIS2 = "/mqtt-wis/"
    """Only stations that meet the WIS 2.0 requirements."""


# Series 0 WIGOS station identifier: series-issuer-issue number-local identifier.
# The pattern also guarantees the identifier is safe to use in MQTT topic filters
# and URL paths.
WIGOS_ID_PATTERN: Final = re.compile(r"^0-\d{1,5}-\d{1,5}-[A-Za-z0-9]{1,16}$")

# Values that data providers use for "no data" instead of omitting the value.
MISSING_VALUE_MARKERS: Final = frozenset(
    {-99999.0, -32768.0, -32767.0, -32766.0, -9999.0}
)


def is_valid_wigos_id(station_id: str) -> bool:
    """Return True if the string is a series 0 WIGOS station identifier."""
    return WIGOS_ID_PATTERN.fullmatch(station_id) is not None
