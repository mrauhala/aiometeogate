"""Tests for the EDR client, run against a local aiohttp server."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from aiometeogate import (
    MeteoGateConnectionError,
    MeteoGateEdrClient,
    MeteoGateResponseError,
    MeteoGateStationNotFoundError,
    ParameterKey,
)

from .conftest import FMI_STATION, load_json

COLLECTION = "/collections/observations"
LOCATIONS = f"{COLLECTION}/locations"
ITEMS = f"{COLLECTION}/items"
STATION_DATA = f"{COLLECTION}/locations/{FMI_STATION}"


@dataclass
class FakeEdr:
    """Serves canned responses and records the requests."""

    base_url: str = ""
    responses: dict[str, web.StreamResponse | Any] = field(default_factory=dict)
    requests: list[tuple[str, dict[str, str]]] = field(default_factory=list)
    delay: float = 0

    async def handle(self, request: web.Request) -> web.StreamResponse:
        self.requests.append((request.path, dict(request.query)))
        if self.delay:
            await asyncio.sleep(self.delay)
        response = self.responses.get(request.path)
        if response is None:
            return web.json_response(
                {"detail": "Requested data not found."}, status=404
            )
        if isinstance(response, web.StreamResponse):
            return response
        return web.json_response(response)

    @property
    def params(self) -> dict[str, str]:
        ((_, params),) = self.requests
        return params


@pytest.fixture
async def edr() -> AsyncIterator[FakeEdr]:
    fake = FakeEdr()
    app = web.Application()
    app.router.add_get("/{tail:.*}", fake.handle)
    async with TestServer(app) as server:
        fake.base_url = str(server.make_url("/"))
        yield fake


@pytest.fixture
async def client(edr: FakeEdr) -> AsyncIterator[MeteoGateEdrClient]:
    async with MeteoGateEdrClient(
        base_url=edr.base_url, request_timeout=1
    ) as edr_client:
        yield edr_client


async def test_get_stations(client: MeteoGateEdrClient, edr: FakeEdr) -> None:
    edr.responses[LOCATIONS] = load_json("edr/locations_helsinki.json")
    stations = await client.async_get_stations()
    assert len(stations) == 8
    assert edr.params == {}


async def test_get_stations_bbox(client: MeteoGateEdrClient, edr: FakeEdr) -> None:
    edr.responses[LOCATIONS] = load_json("edr/locations_helsinki.json")
    await client.async_get_stations(bbox=(24.5, 60.0, 25.3, 60.4))
    assert edr.params == {"bbox": "24.5,60.0,25.3,60.4"}


async def test_get_stations_none_found(client: MeteoGateEdrClient) -> None:
    assert await client.async_get_stations(bbox=(0, 0, 1, 1)) == []


async def test_get_station_metadata(client: MeteoGateEdrClient, edr: FakeEdr) -> None:
    edr.responses[ITEMS] = load_json("edr/items_fmi.json")
    metadata = await client.async_get_station_metadata(FMI_STATION)
    assert metadata.name == "Helsinki Kaisaniemi"
    assert metadata.institution == "Finnish Meteorological Institute (FMI)"
    assert len(metadata.timeseries) == 28
    assert edr.params == {"platform": FMI_STATION}


@pytest.mark.parametrize("response", [None, {"features": []}])
async def test_get_station_metadata_not_found(
    client: MeteoGateEdrClient, edr: FakeEdr, response: Any
) -> None:
    if response is not None:
        edr.responses[ITEMS] = response
    with pytest.raises(MeteoGateStationNotFoundError):
        await client.async_get_station_metadata(FMI_STATION)


async def test_invalid_station_id(client: MeteoGateEdrClient, edr: FakeEdr) -> None:
    with pytest.raises(ValueError, match="Invalid WIGOS"):
        await client.async_get_station_metadata("../etc")
    with pytest.raises(ValueError, match="Invalid WIGOS"):
        await client.async_get_latest_observations("0-246-0-1/2")
    assert edr.requests == []


async def test_get_latest_observations(
    client: MeteoGateEdrClient, edr: FakeEdr
) -> None:
    edr.responses[STATION_DATA] = load_json("edr/coverage_fmi.json")
    since = datetime(2026, 10, 10, 6, 30, tzinfo=UTC)
    observations = await client.async_get_latest_observations(FMI_STATION, since=since)
    assert edr.params == {"datetime": "2026-10-10T06:30:00Z/.."}
    temperature = observations[ParameterKey("air_temperature", 2.0, "mean", "PT1M")]
    assert temperature.station_id == FMI_STATION
    assert temperature.unit == "Cel"
    assert temperature.value is not None


async def test_get_latest_observations_without_window(
    client: MeteoGateEdrClient, edr: FakeEdr
) -> None:
    edr.responses[STATION_DATA] = load_json("edr/coverage_fmi.json")
    assert await client.async_get_latest_observations(FMI_STATION)
    assert edr.params == {}


async def test_get_latest_observations_no_data(client: MeteoGateEdrClient) -> None:
    assert await client.async_get_latest_observations(FMI_STATION) == {}


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (500, MeteoGateConnectionError),
        (503, MeteoGateConnectionError),
        (429, MeteoGateConnectionError),
        (400, MeteoGateResponseError),
    ],
)
async def test_http_errors(
    client: MeteoGateEdrClient, edr: FakeEdr, status: int, error: type[Exception]
) -> None:
    edr.responses[LOCATIONS] = web.Response(status=status)
    with pytest.raises(error):
        await client.async_get_stations()


async def test_invalid_json(client: MeteoGateEdrClient, edr: FakeEdr) -> None:
    edr.responses[LOCATIONS] = web.Response(text="<html>", content_type="text/html")
    with pytest.raises(MeteoGateResponseError, match="Invalid JSON"):
        await client.async_get_stations()


async def test_timeout(edr: FakeEdr) -> None:
    edr.delay = 1
    async with MeteoGateEdrClient(
        base_url=edr.base_url, request_timeout=0.01
    ) as client:
        with pytest.raises(MeteoGateConnectionError, match="Timeout"):
            await client.async_get_stations()


async def test_connection_refused() -> None:
    async with MeteoGateEdrClient(base_url="http://127.0.0.1:9") as client:
        with pytest.raises(MeteoGateConnectionError, match="Error fetching"):
            await client.async_get_stations()


async def test_shared_session_is_not_closed(edr: FakeEdr) -> None:
    async with aiohttp.ClientSession() as session:
        async with MeteoGateEdrClient(session, base_url=edr.base_url) as client:
            await client.async_get_stations()
        assert not session.closed


async def test_own_session_is_closed(edr: FakeEdr) -> None:
    client = MeteoGateEdrClient(base_url=edr.base_url)
    await client.async_get_stations()
    session = client._session
    assert session is not None
    await client.close()
    assert session.closed
    assert client._session is None
    await client.close()
