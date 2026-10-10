"""Tests for the MQTT client."""

import asyncio
import logging
import ssl
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any, Self
from unittest.mock import patch

import aiomqtt
import pytest

from aiometeogate import (
    Endpoint,
    MeteoGateConnectionError,
    MeteoGateMqttClient,
    Observation,
)
from aiometeogate import mqtt as mqtt_module

from .conftest import FMI_STATION, MET_STATION

FMI_TOPIC = f"+/+/{FMI_STATION}/+"
MET_TOPIC = f"+/+/{MET_STATION}/+"


@dataclass
class FakeTopic:
    value: str


@dataclass
class FakeMessage:
    topic: FakeTopic
    payload: Any


class FakeClient:
    """Stand-in for aiomqtt.Client driven by the test."""

    def __init__(
        self, broker: "FakeBroker", hostname: str, port: int, **kwargs: Any
    ) -> None:
        self.broker = broker
        self.hostname = hostname
        self.port = port
        self.kwargs = kwargs
        self.subscribed: list[list[tuple[str, int]]] = []
        self.unsubscribed: list[str] = []
        self.closed = False
        self.queue: asyncio.Queue[FakeMessage | BaseException | None] = asyncio.Queue()

    async def __aenter__(self) -> Self:
        outcome = (
            self.broker.connect_outcomes.pop(0)
            if self.broker.connect_outcomes
            else None
        )
        if outcome == "hang":
            await asyncio.Event().wait()
        if isinstance(outcome, BaseException):
            raise outcome
        return self

    async def __aexit__(self, *args: object) -> None:
        # Like aiomqtt, closing the connection ends the message iterator.
        self.closed = True
        self.queue.put_nowait(
            aiomqtt.MqttError("Disconnected during message iteration")
        )
        if self.broker.exit_hangs:
            await asyncio.Event().wait()
        if self.broker.exit_error is not None:
            raise self.broker.exit_error

    async def subscribe(self, topics: list[tuple[str, int]]) -> None:
        if self.broker.subscribe_error is not None:
            raise self.broker.subscribe_error
        self.subscribed.append(topics)

    async def unsubscribe(self, topic: str) -> None:
        if self.broker.subscribe_error is not None:
            raise self.broker.subscribe_error
        self.unsubscribed.append(topic)

    @property
    def messages(self) -> AsyncIterator[FakeMessage]:
        return self._messages()

    async def _messages(self) -> AsyncIterator[FakeMessage]:
        while True:
            item = await self.queue.get()
            if item is None:
                return
            if isinstance(item, BaseException):
                raise item
            yield item

    def deliver(self, topic: str, payload: Any) -> None:
        self.queue.put_nowait(FakeMessage(FakeTopic(topic), payload))

    def drop(self) -> None:
        self.queue.put_nowait(
            aiomqtt.MqttError("Disconnected during message iteration")
        )


class FakeBroker:
    """Creates FakeClients and records them."""

    def __init__(self) -> None:
        self.clients: list[FakeClient] = []
        self.connect_outcomes: list[Any] = []
        self.subscribe_error: Exception | None = None
        self.exit_error: Exception | None = None
        self.exit_hangs = False

    def __call__(self, hostname: str, port: int, **kwargs: Any) -> FakeClient:
        client = FakeClient(self, hostname, port, **kwargs)
        self.clients.append(client)
        return client

    @property
    def client(self) -> FakeClient:
        return self.clients[-1]


@pytest.fixture
def broker() -> Iterator[FakeBroker]:
    fake = FakeBroker()
    with patch("aiometeogate.mqtt.aiomqtt.Client", fake):
        yield fake


@pytest.fixture
def tls_context() -> ssl.SSLContext:
    return ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


@pytest.fixture
def client(tls_context: ssl.SSLContext) -> MeteoGateMqttClient:
    return MeteoGateMqttClient(
        tls_context=tls_context, reconnect_interval=(0.001, 0.004), connect_timeout=1
    )


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def _wait_for(condition: Any, limit: float = 2) -> None:
    async with asyncio.timeout(limit):
        while not condition():
            await asyncio.sleep(0.001)


async def test_connect_subscribes_registered_stations(
    broker: FakeBroker, client: MeteoGateMqttClient, tls_context: ssl.SSLContext
) -> None:
    states: list[bool] = []
    client.add_connection_listener(states.append)
    client.subscribe_station(FMI_STATION, lambda _: None)
    client.subscribe_station(MET_STATION, lambda _: None)

    await client.connect()

    assert client.connected
    assert states == [True]
    assert client.station_ids == {FMI_STATION, MET_STATION}
    fake = broker.client
    assert fake.hostname == "observations.meteogate.eu"
    assert fake.port == 443
    assert fake.kwargs["transport"] == "websockets"
    assert fake.kwargs["websocket_path"] == Endpoint.PUBLIC
    assert fake.kwargs["tls_context"] is tls_context
    assert fake.kwargs["username"] == fake.kwargs["password"] == "everyone"
    assert fake.kwargs["identifier"].startswith("aiometeogate-")
    assert fake.kwargs["keepalive"] == 20
    assert fake.subscribed == [[(FMI_TOPIC, 0), (MET_TOPIC, 0)]]
    assert client.stats.connects == 1
    assert client.stats.connected_since is not None

    await client.connect()  # already connected: no new connection
    assert len(broker.clients) == 1

    await client.disconnect()
    assert fake.closed
    assert not client.connected
    assert states == [True, False]
    assert client.stats.disconnects == 1


async def test_connect_without_stations(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    await client.connect()
    assert broker.client.subscribed == []
    await client.disconnect()


async def test_default_tls_context(broker: FakeBroker) -> None:
    client = MeteoGateMqttClient()
    await client.connect()
    assert isinstance(broker.client.kwargs["tls_context"], ssl.SSLContext)
    await client.disconnect()


@pytest.mark.parametrize(
    "outcome", [aiomqtt.MqttError("refused"), OSError("unreachable"), "hang"]
)
async def test_connect_failure(
    broker: FakeBroker, tls_context: ssl.SSLContext, outcome: Any
) -> None:
    client = MeteoGateMqttClient(tls_context=tls_context, connect_timeout=0.01)
    states: list[bool] = []
    client.add_connection_listener(states.append)
    broker.connect_outcomes = [outcome]
    with pytest.raises(MeteoGateConnectionError):
        await client.connect()
    assert not client.connected
    assert states == []
    assert client.stats.last_error


async def test_connect_subscribe_failure(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    client.subscribe_station(FMI_STATION, lambda _: None)
    broker.subscribe_error = aiomqtt.MqttError("not authorised")
    broker.exit_error = aiomqtt.MqttError("already gone")
    with pytest.raises(MeteoGateConnectionError, match="not authorised"):
        await client.connect()
    assert not client.connected
    assert broker.client.closed


async def test_messages_are_dispatched(
    broker: FakeBroker,
    client: MeteoGateMqttClient,
    fmi_notification: tuple[str, bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    received: list[Observation] = []
    other: list[Observation] = []
    client.subscribe_station(FMI_STATION, received.append)
    client.subscribe_station(MET_STATION, other.append)

    def failing(_: Observation) -> None:
        raise RuntimeError("callback bug")

    client.subscribe_station(FMI_STATION, failing)
    await client.connect()
    runner = asyncio.create_task(client.run())

    topic, payload = fmi_notification
    fake = broker.client
    fake.deliver(topic, payload)
    fake.deliver("fi/fmi/0-246-0-999999/air_temperature", payload)  # not subscribed
    fake.deliver("too/short", payload)
    fake.deliver(topic, b"{broken")
    fake.deliver(topic, 42)
    fake.deliver(topic, payload.decode())
    await _wait_for(lambda: client.stats.messages_received == 6)

    assert [o.station_id for o in received] == [FMI_STATION, FMI_STATION]
    assert received[0].key.standard_name == "wind_speed"
    assert other == []
    assert client.stats.parse_errors == 2
    assert client.stats.last_message_at is not None
    assert "Error in observation callback" in caplog.text

    await client.disconnect()
    await runner


async def test_subscribe_and_unsubscribe_while_connected(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    await client.connect()
    fake = broker.client

    remove_first = client.subscribe_station(FMI_STATION, lambda _: None)
    remove_second = client.subscribe_station(FMI_STATION, lambda _: None)
    await _settle()
    assert fake.subscribed == [[(FMI_TOPIC, 0)]]

    remove_first()
    remove_first()  # removing twice is harmless
    await _settle()
    assert fake.unsubscribed == []

    remove_second()
    await _settle()
    assert fake.unsubscribed == [FMI_TOPIC]
    assert client.station_ids == frozenset()
    await client.disconnect()


async def test_unsubscribe_while_disconnected(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    remove = client.subscribe_station(FMI_STATION, lambda _: None)
    remove()
    await client.connect()
    assert broker.client.subscribed == []
    await client.disconnect()


async def test_subscription_errors_are_ignored(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    await client.connect()
    broker.subscribe_error = aiomqtt.MqttError("gone")
    remove = client.subscribe_station(FMI_STATION, lambda _: None)
    await _settle()
    remove()
    await _settle()
    assert broker.client.subscribed == []
    assert broker.client.unsubscribed == []
    await client.disconnect()


async def test_send_after_connection_closed(client: MeteoGateMqttClient) -> None:
    await client._send_subscribe([FMI_STATION])
    await client._send_unsubscribe(FMI_STATION)
    await client._receive_messages()


def test_invalid_station(client: MeteoGateMqttClient) -> None:
    with pytest.raises(ValueError, match="Invalid WIGOS"):
        client.subscribe_station("+/#", lambda _: None)


async def test_run_reconnects_and_resubscribes(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    states: list[bool] = []
    remove_listener = client.add_connection_listener(states.append)
    client.subscribe_station(FMI_STATION, lambda _: None)
    await client.connect()
    runner = asyncio.create_task(client.run())

    first = broker.client
    broker.connect_outcomes = [aiomqtt.MqttError("refused"), None]
    first.drop()
    await _wait_for(lambda: len(broker.clients) == 3 and client.connected)

    assert first.closed
    assert broker.client.subscribed == [[(FMI_TOPIC, 0)]]
    assert states == [True, False, True]
    assert client.stats.connects == 2
    assert client.stats.disconnects == 1

    # The broker ending the stream without an error also reconnects.
    broker.client.queue.put_nowait(None)
    await _wait_for(lambda: len(broker.clients) == 4 and client.connected)

    remove_listener()
    remove_listener()
    await client.disconnect()
    await runner
    assert states == [True, False, True, False, True]


async def test_run_connects_if_needed(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    runner = asyncio.create_task(client.run())
    await _wait_for(lambda: client.connected)
    await client.disconnect()
    await runner
    assert broker.client.closed


async def test_backoff_grows_and_resets(
    broker: FakeBroker, tls_context: ssl.SSLContext
) -> None:
    client = MeteoGateMqttClient(tls_context=tls_context, reconnect_interval=(1, 4))
    delays: list[float] = []

    async def fake_wait(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    broker.connect_outcomes = [aiomqtt.MqttError("refused")] * 4
    with (
        patch.object(client, "_wait_before_retry", fake_wait),
        patch.object(mqtt_module, "_STABLE_CONNECTION_SECONDS", 0),
    ):
        runner = asyncio.create_task(client.run())
        await _wait_for(lambda: client.connected)
        assert delays == [1, 2, 4, 4]
        broker.client.drop()
        await _wait_for(lambda: len(delays) == 5 and client.connected)
        assert delays[-1] == 1  # the connection was stable, so backoff restarted
        await client.disconnect()
        await runner


async def test_wait_before_retry_ends_on_disconnect(
    client: MeteoGateMqttClient,
) -> None:
    waiter = asyncio.create_task(client._wait_before_retry(60))
    await _settle()
    await client.disconnect()
    async with asyncio.timeout(1):
        await waiter


async def test_cancelling_run_closes_connection(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    await client.connect()
    runner = asyncio.create_task(client.run())
    await _settle()
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner
    assert broker.client.closed
    assert not client.connected


@pytest.mark.usefixtures("broker")
async def test_listener_errors_are_logged(
    client: MeteoGateMqttClient, caplog: pytest.LogCaptureFixture
) -> None:
    def failing(_: bool) -> None:
        raise RuntimeError("listener bug")

    client.add_connection_listener(failing)
    with caplog.at_level(logging.ERROR):
        await client.connect()
    assert "Error in connection listener" in caplog.text
    await client.disconnect()


@pytest.mark.usefixtures("broker")
async def test_run_after_disconnect_returns(client: MeteoGateMqttClient) -> None:
    await client.disconnect()
    async with asyncio.timeout(1):
        await client.run()
    assert not client.connected


async def test_disconnect_cancels_pending_subscriptions(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    await client.connect()
    client.subscribe_station(FMI_STATION, lambda _: None)
    await client.disconnect()
    await _settle()
    assert broker.client.subscribed == []


async def test_disconnect_does_not_hang(
    broker: FakeBroker, client: MeteoGateMqttClient
) -> None:
    states: list[bool] = []
    client.add_connection_listener(states.append)
    await client.connect()
    broker.exit_hangs = True
    with patch.object(mqtt_module, "_DISCONNECT_TIMEOUT", 0.01):
        async with asyncio.timeout(1):
            await client.disconnect()
    assert not client.connected
    assert states == [True, False]
