"""Exceptions raised by the MeteoGate client."""


class MeteoGateError(Exception):
    """Base class for all MeteoGate errors."""


class MeteoGateConnectionError(MeteoGateError):
    """The service could not be reached or the connection was lost."""


class MeteoGateResponseError(MeteoGateError):
    """The service returned an unexpected response."""


class MeteoGateStationNotFoundError(MeteoGateError):
    """The requested station is not known to the service."""


class MeteoGateParseError(MeteoGateError):
    """A message or response could not be parsed."""
