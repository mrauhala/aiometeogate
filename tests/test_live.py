"""Tests against the real MeteoGate service; run with `pytest -m live`."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from aiometeogate import (
    MeteoGateEdrClient,
    MeteoGateMqttClient,
    MeteoGateStationNotFoundError,
    Observation,
)

from .conftest import FMI_STATION, KNMI_STATION

pytestmark = pytest.mark.live


async def test_stations_near_helsinki() -> None:
    async with MeteoGateEdrClient() as client:
        stations = await client.async_get_stations(bbox=(24.5, 60.0, 25.3, 60.4))
    assert FMI_STATION in {station.id for station in stations}


async def test_station_metadata_and_latest_values() -> None:
    async with MeteoGateEdrClient() as client:
        metadata = await client.async_get_station_metadata(FMI_STATION)
        observations = await client.async_get_latest_observations(
            FMI_STATION, since=datetime.now(UTC) - timedelta(hours=3)
        )
    assert metadata.institution
    assert metadata.timeseries
    assert observations
    # The live stream and EDR must use the same parameter names.
    known = {timeseries.key for timeseries in metadata.timeseries}
    assert set(observations) <= known


async def test_unknown_station() -> None:
    async with MeteoGateEdrClient() as client:
        with pytest.raises(MeteoGateStationNotFoundError):
            await client.async_get_station_metadata("0-0-0-doesnotexist")


async def test_mqtt_stream() -> None:
    # KNMI publishes every 10 minutes and repeats recent values often.
    received: asyncio.Queue[Observation] = asyncio.Queue()
    client = MeteoGateMqttClient()
    client.subscribe_station(KNMI_STATION, received.put_nowait)
    await client.connect()
    runner = asyncio.create_task(client.run())
    try:
        async with asyncio.timeout(15 * 60):
            observation = await received.get()
    finally:
        await client.disconnect()
        await runner
    assert observation.station_id == KNMI_STATION
    assert client.stats.parse_errors == 0
