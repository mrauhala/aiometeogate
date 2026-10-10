"""Record test fixtures from the live MeteoGate service.

    uv run python scripts/record_fixtures.py edr
    uv run python scripts/record_fixtures.py mqtt --minutes 15

EDR responses are written to tests/fixtures/edr/ and one MQTT notification per
station to tests/fixtures/mqtt/. Review the diff before committing.
"""

import argparse
import asyncio
import contextlib
import json
import ssl
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import aiomqtt

from aiometeogate.const import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    EDR_BASE_URL,
    PUBLIC_PASSWORD,
    PUBLIC_USERNAME,
    Endpoint,
)

FIXTURES = Path(__file__).parent.parent / "tests" / "fixtures"
# FMI, MET Norway, KNMI, Météo-France and HungaroMet stations.
STATIONS = {
    "fmi": "0-246-0-100971",
    "met": "0-20000-0-01492",
    "knmi": "0-20000-0-06260",
    "meteofrance": "0-250-0-07002",
    "hungaromet": "0-348-1-13711",
}
# A small box around Helsinki keeps the locations fixture small.
HELSINKI_BBOX = "24.5,60.0,25.3,60.4"


def _write(path: Path, data: Any) -> None:  # noqa: ANN401
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {path.relative_to(FIXTURES.parent.parent)}")


async def _get(session: aiohttp.ClientSession, path: str, **params: str) -> Any:  # noqa: ANN401
    async with session.get(f"{EDR_BASE_URL}{path}", params=params) as response:
        return response.status, await response.json(content_type=None)


async def record_edr() -> None:
    """Record EDR responses."""
    since = (datetime.now(UTC) - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
    edr = FIXTURES / "edr"
    async with aiohttp.ClientSession() as session:
        _, data = await _get(
            session, "/collections/observations/locations", bbox=HELSINKI_BBOX
        )
        _write(edr / "locations_helsinki.json", data)
        for name in ("fmi", "met"):
            station_id = STATIONS[name]
            _, data = await _get(
                session, "/collections/observations/items", platform=station_id
            )
            _write(edr / f"items_{name}.json", data)
            _, data = await _get(
                session,
                f"/collections/observations/locations/{station_id}",
                datetime=f"{since}/..",
            )
            _write(edr / f"coverage_{name}.json", data)
        status, data = await _get(
            session, "/collections/observations/locations/0-0-0-doesnotexist"
        )
        _write(edr / f"error_{status}.json", data)


async def record_mqtt(minutes: float) -> None:
    """Record the first air temperature or wind message of every station."""
    wanted = {station_id: name for name, station_id in STATIONS.items()}
    tls_context = ssl.create_default_context()
    async with aiomqtt.Client(
        DEFAULT_HOST,
        DEFAULT_PORT,
        username=PUBLIC_USERNAME,
        password=PUBLIC_PASSWORD,
        transport="websockets",
        websocket_path=Endpoint.PUBLIC,
        tls_context=tls_context,
        keepalive=20,
    ) as client:
        await client.subscribe([(f"+/+/{station_id}/+", 0) for station_id in wanted])
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(minutes * 60):
                async for message in client.messages:
                    topic = message.topic.value
                    station_id = topic.split("/")[2]
                    if station_id not in wanted or not topic.endswith(
                        ("/air_temperature", "/wind_speed")
                    ):
                        continue
                    assert isinstance(message.payload, bytes)
                    _write(
                        FIXTURES
                        / "mqtt"
                        / f"notification_{wanted.pop(station_id)}.json",
                        {"topic": topic, "payload": json.loads(message.payload)},
                    )
                    if not wanted:
                        break
    if wanted:
        print(f"no message from: {', '.join(sorted(wanted.values()))}")


def main() -> None:
    """Run the recorder."""
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("edr")
    mqtt = commands.add_parser("mqtt")
    mqtt.add_argument("--minutes", type=float, default=15)
    args = parser.parse_args()
    if args.command == "edr":
        asyncio.run(record_edr())
    else:
        asyncio.run(record_mqtt(args.minutes))


if __name__ == "__main__":
    main()
