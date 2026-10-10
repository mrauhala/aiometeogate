# aiometeogate

Asynchronous Python client for [EUMETNET MeteoGate](https://github.com/EUMETNET/e-soh-documentation/wiki)
(E-SOH) near-real-time weather observations. MeteoGate publishes observations
from about 4,000 European stations (FMI, MET Norway, KNMI, Météo-France and
others).

- **Live data over MQTT** (WebSocket, TLS): `MeteoGateMqttClient` subscribes per
  station, reconnects with backoff and parses each notification into an
  `Observation`.
- **EDR API** over HTTPS: `MeteoGateEdrClient` lists stations, fetches station
  metadata and returns the newest value of every timeseries. MeteoGate keeps
  24 hours of data.

The library was written for the Home Assistant MeteoGate integration but has
no Home Assistant dependency.

## Installation

```bash
pip install aiometeogate
```

Python 3.13 or newer.

## Usage

```python
import asyncio
from datetime import UTC, datetime, timedelta

from aiometeogate import MeteoGateEdrClient, MeteoGateMqttClient

KAISANIEMI = "0-246-0-100971"  # WIGOS station identifier


async def main() -> None:
    async with MeteoGateEdrClient() as edr:
        stations = await edr.async_get_stations(bbox=(24.5, 60.0, 25.3, 60.4))
        latest = await edr.async_get_latest_observations(
            KAISANIEMI, since=datetime.now(UTC) - timedelta(hours=3)
        )
    for key, observation in sorted(latest.items()):
        print(key.parameter_name, observation.value, observation.unit)

    client = MeteoGateMqttClient()
    client.subscribe_station(KAISANIEMI, lambda obs: print(obs.key, obs.value))
    await client.connect()
    await client.run()  # runs until cancelled or client.disconnect()


asyncio.run(main())
```

One MQTT topic can carry several variants of a quantity, for example 2-minute
and 10-minute mean wind speed. Each `Observation` therefore has a
`ParameterKey` of standard name, level, aggregation function and period, written
the same way as EDR parameter names, e.g. `wind_speed:10.0:mean:PT10M`.

Values that providers mark as missing (`-32767`, `-9999`, NaN, …) become `None`.
FMI's snow depth of `-1` ("no snow cover") becomes `0`.

### Command line

```bash
python -m aiometeogate nearest 60.17 24.94
python -m aiometeogate latest 0-246-0-100971
python -m aiometeogate watch 0-246-0-100971 0-20000-0-01492 --minutes 15
```

## Notes on the service

- The broker is public: `wss://observations.meteogate.eu/mqtt/`, user and
  password `everyone`. `/mqtt-wis/` carries only WIS 2.0 stations.
- Messages are not retained. After connecting, you only get new observations;
  use the EDR client to fill in the current values.
- Providers may repeat an observation or send older ones late. Compare
  `observed_at` before replacing a value.
- The broker drops connections that use a 60 second keepalive after about two
  minutes, so the client defaults to 20 seconds.

## Data licence

The observations belong to the institutions that publish them and are
generally licensed under CC BY 4.0. Each `Observation` and `StationMetadata`
carries the `institution` to credit.

## Development

```bash
uv sync
uv run pytest            # unit tests; `-m live` runs tests against the real service
uv run ruff check . && uv run ruff format --check . && uv run mypy src tests scripts
uv run python scripts/record_fixtures.py edr   # refresh recorded fixtures
```

## License

MIT
