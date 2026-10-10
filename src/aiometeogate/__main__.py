"""Command line tool for trying out the MeteoGate client.

python -m aiometeogate nearest 60.17 24.94
python -m aiometeogate latest 0-246-0-100971
python -m aiometeogate watch 0-246-0-100971 0-20000-0-01492 --minutes 15
"""

import argparse
import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta

from .edr import MeteoGateEdrClient
from .models import Observation
from .mqtt import MeteoGateMqttClient


def _format(observation: Observation) -> str:
    return (
        f"{observation.observed_at:%Y-%m-%d %H:%M:%SZ}  {observation.station_id}  "
        f"{observation.key.parameter_name:<45} {observation.value} {observation.unit}"
    )


async def _nearest(latitude: float, longitude: float, limit: int) -> None:
    async with MeteoGateEdrClient() as client:
        stations = await client.async_get_stations()
    stations.sort(key=lambda station: station.distance_km(latitude, longitude))
    for station in stations[:limit]:
        distance = station.distance_km(latitude, longitude)
        print(f"{distance:7.1f} km  {station.id:<20} {station.name}")


async def _latest(station_id: str, hours: float) -> None:
    since = datetime.now(UTC) - timedelta(hours=hours)
    async with MeteoGateEdrClient() as client:
        metadata = await client.async_get_station_metadata(station_id)
        observations = await client.async_get_latest_observations(
            station_id, since=since
        )
    print(f"{metadata.name} ({metadata.institution})")
    for key in sorted(observations):
        print(_format(observations[key]))


async def _watch(station_ids: list[str], minutes: float) -> None:
    client = MeteoGateMqttClient()
    for station_id in station_ids:
        client.subscribe_station(station_id, lambda obs: print(_format(obs)))
    client.add_connection_listener(
        lambda connected: print("connected" if connected else "disconnected")
    )
    await client.connect()
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(minutes * 60):
            await client.run()
    await client.disconnect()
    stats = client.stats
    print(
        f"messages={stats.messages_received} parse_errors={stats.parse_errors} "
        f"connects={stats.connects} disconnects={stats.disconnects}"
    )


def main() -> None:
    """Run the command line tool."""
    parser = argparse.ArgumentParser(prog="aiometeogate")
    parser.add_argument("--debug", action="store_true", help="enable debug logging")
    commands = parser.add_subparsers(dest="command", required=True)

    nearest = commands.add_parser("nearest", help="list stations nearest to a point")
    nearest.add_argument("latitude", type=float)
    nearest.add_argument("longitude", type=float)
    nearest.add_argument("--limit", type=int, default=10)

    latest = commands.add_parser("latest", help="show the latest values of a station")
    latest.add_argument("station_id")
    latest.add_argument("--hours", type=float, default=3)

    watch = commands.add_parser("watch", help="stream observations over MQTT")
    watch.add_argument("station_ids", nargs="+")
    watch.add_argument("--minutes", type=float, default=15)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.WARNING)
    if args.command == "nearest":
        asyncio.run(_nearest(args.latitude, args.longitude, args.limit))
    elif args.command == "latest":
        asyncio.run(_latest(args.station_id, args.hours))
    else:
        asyncio.run(_watch(args.station_ids, args.minutes))


if __name__ == "__main__":
    main()
