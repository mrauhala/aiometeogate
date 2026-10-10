"""Client for the MeteoGate EDR API (station list, metadata and latest values)."""

import asyncio
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import aiohttp

from .const import EDR_BASE_URL, is_valid_wigos_id
from .exceptions import (
    MeteoGateConnectionError,
    MeteoGateParseError,
    MeteoGateResponseError,
    MeteoGateStationNotFoundError,
)
from .models import Observation, ParameterKey, Station, StationMetadata
from .parsing import parse_coverage_collection, parse_items, parse_locations

_COLLECTION = "/collections/observations"
_HTTP_NOT_FOUND = 404
_HTTP_OK = 200
_HTTP_TOO_MANY_REQUESTS = 429
_HTTP_SERVER_ERROR = 500


def _check_station_id(station_id: str) -> None:
    if not is_valid_wigos_id(station_id):
        raise ValueError(f"Invalid WIGOS station identifier: {station_id!r}")


class MeteoGateEdrClient:
    """Asynchronous client for the MeteoGate EDR API.

    Pass in an aiohttp session to share it; the client only closes a session it
    created itself.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession | None = None,
        *,
        base_url: str = EDR_BASE_URL,
        request_timeout: float = 30.0,
    ) -> None:
        """Initialise the client."""
        self._session = session
        self._close_session = False
        self._base_url = base_url.rstrip("/")
        self._request_timeout = request_timeout

    async def _get_json(self, path: str, params: dict[str, str] | None = None) -> Any:  # noqa: ANN401
        if self._session is None:
            self._session = aiohttp.ClientSession()
            self._close_session = True
        url = f"{self._base_url}{path}"
        try:
            async with (
                asyncio.timeout(self._request_timeout),
                self._session.get(
                    url, params=params, headers={"Accept": "application/json"}
                ) as response,
            ):
                if response.status == _HTTP_NOT_FOUND:
                    raise MeteoGateStationNotFoundError(f"Not found: {url}")
                if (
                    response.status >= _HTTP_SERVER_ERROR
                    or response.status == _HTTP_TOO_MANY_REQUESTS
                ):
                    raise MeteoGateConnectionError(
                        f"MeteoGate returned HTTP {response.status} for {url}"
                    )
                if response.status != _HTTP_OK:
                    raise MeteoGateResponseError(
                        f"Unexpected HTTP {response.status} for {url}"
                    )
                return await response.json(content_type=None)
        except TimeoutError as err:
            raise MeteoGateConnectionError(f"Timeout fetching {url}") from err
        except aiohttp.ClientError as err:
            raise MeteoGateConnectionError(f"Error fetching {url}: {err}") from err
        except ValueError as err:
            raise MeteoGateResponseError(f"Invalid JSON from {url}") from err

    async def async_get_stations(
        self, *, bbox: tuple[float, float, float, float] | None = None
    ) -> list[Station]:
        """Return the stations, optionally limited to (west, south, east, north)."""
        params = None
        if bbox is not None:
            params = {"bbox": ",".join(str(edge) for edge in bbox)}
        try:
            data = await self._get_json(f"{_COLLECTION}/locations", params)
        except MeteoGateStationNotFoundError:
            return []
        return parse_locations(data)

    async def async_get_station_metadata(self, station_id: str) -> StationMetadata:
        """Return the metadata of one station.

        Raises MeteoGateStationNotFoundError if the station has no timeseries.
        """
        _check_station_id(station_id)
        data = await self._get_json(f"{_COLLECTION}/items", {"platform": station_id})
        try:
            return parse_items(data, station_id)
        except MeteoGateParseError as err:
            raise MeteoGateStationNotFoundError(str(err)) from err

    async def async_get_latest_observations(
        self, station_id: str, *, since: datetime | None = None
    ) -> dict[ParameterKey, Observation]:
        """Return the newest value of every timeseries of a station.

        Without `since` the service returns all stored data (up to 24 hours).
        A station with no data in the window gives an empty result.
        """
        _check_station_id(station_id)
        params = None
        if since is not None:
            start = since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            params = {"datetime": f"{start}/.."}
        try:
            data = await self._get_json(f"{_COLLECTION}/locations/{station_id}", params)
        except MeteoGateStationNotFoundError:
            return {}
        return parse_coverage_collection(data, station_id)

    async def close(self) -> None:
        """Close the session if the client created it."""
        if self._session is not None and self._close_session:
            await self._session.close()
            self._session = None
            self._close_session = False

    async def __aenter__(self) -> Self:
        """Enter the async context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the session on exit."""
        await self.close()
