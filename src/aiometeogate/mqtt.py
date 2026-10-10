"""MQTT client that streams MeteoGate observations for selected stations."""

import asyncio
import contextlib
import logging
import random
import secrets
import ssl
from collections.abc import Callable, Coroutine
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import aiomqtt

from .const import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    PUBLIC_PASSWORD,
    PUBLIC_USERNAME,
    Endpoint,
    is_valid_wigos_id,
)
from .exceptions import MeteoGateConnectionError, MeteoGateParseError
from .models import Observation
from .parsing import parse_notification

_LOGGER = logging.getLogger(__name__)

type ObservationCallback = Callable[[Observation], None]
type ConnectionCallback = Callable[[bool], None]

# A connection that stays up this long resets the reconnect backoff.
_STABLE_CONNECTION_SECONDS = 60.0
# Bounds memory use if the consumer falls behind.
_MAX_QUEUED_MESSAGES = 1000
# The MeteoGate broker drops connections that use a 60 s keepalive after about two
# minutes (observed October 2026); a 20 s keepalive keeps them open.
DEFAULT_KEEPALIVE = 20
_TOPIC_LEVELS = 4
_DISCONNECT_TIMEOUT = 5.0


def station_topic(station_id: str) -> str:
    """Return the topic filter that matches every message of a station."""
    return f"+/+/{station_id}/+"


@dataclass(slots=True)
class MqttStats:
    """Counters for diagnostics."""

    messages_received: int = 0
    parse_errors: int = 0
    connects: int = 0
    disconnects: int = 0
    last_message_at: datetime | None = None
    connected_since: datetime | None = None
    last_error: str | None = None


class MeteoGateMqttClient:
    """Stream observations of selected stations from the MeteoGate broker.

    Register stations with `subscribe_station`, open the connection with
    `connect`, then keep `run` running in a task. `run` reconnects with
    exponential backoff and subscribes to all registered stations again after
    every reconnect. Callbacks are called in the event loop.
    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        endpoint: str = Endpoint.PUBLIC,
        username: str = PUBLIC_USERNAME,
        password: str = PUBLIC_PASSWORD,
        tls_context: ssl.SSLContext | None = None,
        client_id: str | None = None,
        keepalive: int = DEFAULT_KEEPALIVE,
        connect_timeout: float = 30.0,
        reconnect_interval: tuple[float, float] = (1.0, 300.0),
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialise the client; nothing is connected until `connect`."""
        self._host = host
        self._port = port
        self._endpoint = endpoint
        self._username = username
        self._password = password
        self._tls_context = tls_context
        self._client_id = client_id or f"aiometeogate-{secrets.token_hex(6)}"
        self._keepalive = keepalive
        self._connect_timeout = connect_timeout
        self._min_delay, self._max_delay = reconnect_interval
        self._logger = logger or _LOGGER

        self._subscriptions: dict[str, list[ObservationCallback]] = {}
        self._connection_listeners: list[ConnectionCallback] = []
        self._client: aiomqtt.Client | None = None
        self._exit_stack: AsyncExitStack | None = None
        self._connected = False
        self._closing = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._stats = MqttStats()

    @property
    def connected(self) -> bool:
        """Return True while connected and subscribed."""
        return self._connected

    @property
    def stats(self) -> MqttStats:
        """Return counters for diagnostics."""
        return self._stats

    @property
    def station_ids(self) -> frozenset[str]:
        """Return the stations that have at least one subscriber."""
        return frozenset(self._subscriptions)

    def subscribe_station(
        self, station_id: str, callback: ObservationCallback
    ) -> Callable[[], None]:
        """Call `callback` for every observation of a station.

        Returns a function that removes the callback. If the client is connected,
        the subscription is sent to the broker at once; otherwise it is sent when
        the connection opens.
        """
        if not is_valid_wigos_id(station_id):
            raise ValueError(f"Invalid WIGOS station identifier: {station_id!r}")
        callbacks = self._subscriptions.setdefault(station_id, [])
        callbacks.append(callback)
        if len(callbacks) == 1 and self._connected:
            self._create_task(self._send_subscribe([station_id]))

        def unsubscribe() -> None:
            current = self._subscriptions.get(station_id)
            if current is None or callback not in current:
                return
            current.remove(callback)
            if current:
                return
            del self._subscriptions[station_id]
            if self._connected:
                self._create_task(self._send_unsubscribe(station_id))

        return unsubscribe

    def add_connection_listener(
        self, callback: ConnectionCallback
    ) -> Callable[[], None]:
        """Call `callback(connected)` whenever the connection state changes."""
        self._connection_listeners.append(callback)

        def remove() -> None:
            with contextlib.suppress(ValueError):
                self._connection_listeners.remove(callback)

        return remove

    async def connect(self) -> None:
        """Connect and subscribe to the registered stations.

        Raises MeteoGateConnectionError if the broker cannot be reached.
        """
        self._closing.clear()
        if self._client is None:
            await self._connect()

    async def run(self) -> None:
        """Receive messages until `disconnect` is called or the task is cancelled.

        Lost connections are re-established with exponential backoff.
        """
        loop = asyncio.get_running_loop()
        delay = self._min_delay
        try:
            while not self._closing.is_set():
                if self._client is None:
                    try:
                        await self._connect()
                    except MeteoGateConnectionError as err:
                        self._logger.debug("Connecting to MeteoGate failed: %s", err)
                        await self._wait_before_retry(delay)
                        delay = min(delay * 2, self._max_delay)
                        continue
                connected_at = loop.time()
                await self._receive_messages()
                await self._close_client()
                if self._closing.is_set():
                    break
                if loop.time() - connected_at >= _STABLE_CONNECTION_SECONDS:
                    delay = self._min_delay
                await self._wait_before_retry(delay)
                delay = min(delay * 2, self._max_delay)
        finally:
            await self._close_client()

    async def disconnect(self) -> None:
        """Close the connection and make `run` return."""
        self._closing.set()
        for task in list(self._tasks):
            task.cancel()
        await self._close_client()

    async def _connect(self) -> None:
        if self._tls_context is None:
            # Loading the CA certificates blocks, so do it outside the event loop.
            loop = asyncio.get_running_loop()
            self._tls_context = await loop.run_in_executor(
                None, ssl.create_default_context
            )
        client = aiomqtt.Client(
            self._host,
            self._port,
            username=self._username,
            password=self._password,
            identifier=self._client_id,
            logger=self._logger,
            transport="websockets",
            websocket_path=self._endpoint,
            tls_context=self._tls_context,
            clean_session=True,
            keepalive=self._keepalive,
            timeout=self._connect_timeout,
            max_queued_incoming_messages=_MAX_QUEUED_MESSAGES,
        )
        exit_stack = AsyncExitStack()
        try:
            async with asyncio.timeout(self._connect_timeout):
                await exit_stack.enter_async_context(client)
            # Take the station list and mark the client connected without awaiting
            # in between, so a station added meanwhile subscribes on its own.
            self._client = client
            self._exit_stack = exit_stack
            station_ids = list(self._subscriptions)
            self._connected = True
            if station_ids:
                await client.subscribe(
                    [(station_topic(station_id), 0) for station_id in station_ids]
                )
        except (aiomqtt.MqttError, TimeoutError, OSError) as err:
            self._stats.last_error = str(err) or type(err).__name__
            self._connected = False
            self._client = None
            self._exit_stack = None
            with contextlib.suppress(aiomqtt.MqttError, TimeoutError, OSError):
                await exit_stack.aclose()
            raise MeteoGateConnectionError(
                f"Cannot connect to MeteoGate: {self._stats.last_error}"
            ) from err

        self._stats.connects += 1
        self._stats.connected_since = datetime.now(UTC)
        self._logger.debug(
            "Connected to MeteoGate, %d station(s) subscribed", len(station_ids)
        )
        self._notify_connection(connected=True)

    async def _receive_messages(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            async for message in client.messages:
                self._handle_message(message.topic.value, message.payload)
        except aiomqtt.MqttError as err:
            self._stats.last_error = str(err)
            if not self._closing.is_set():
                self._logger.debug("Connection to MeteoGate lost: %s", err)

    async def _close_client(self) -> None:
        exit_stack, self._exit_stack = self._exit_stack, None
        self._client = None
        was_connected, self._connected = self._connected, False
        if exit_stack is not None:
            with contextlib.suppress(aiomqtt.MqttError, TimeoutError, OSError):
                # Short, so that a dead connection does not hold up shutdown.
                async with asyncio.timeout(_DISCONNECT_TIMEOUT):
                    await exit_stack.aclose()
        if was_connected:
            self._stats.disconnects += 1
            self._stats.connected_since = None
            self._notify_connection(connected=False)

    async def _wait_before_retry(self, delay: float) -> None:
        jittered = random.uniform(delay / 2, delay)  # noqa: S311
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(jittered):
                await self._closing.wait()

    def _handle_message(self, topic: str, payload: Any) -> None:  # noqa: ANN401
        self._stats.messages_received += 1
        self._stats.last_message_at = datetime.now(UTC)
        parts = topic.split("/")
        if len(parts) != _TOPIC_LEVELS:
            return
        callbacks = self._subscriptions.get(parts[2])
        if not callbacks:
            return
        if not isinstance(payload, bytes | str):
            self._stats.parse_errors += 1
            return
        try:
            observation = parse_notification(payload, topic=topic)
        except MeteoGateParseError as err:
            self._stats.parse_errors += 1
            self._logger.debug("Ignoring message on %s: %s", topic, err)
            return
        for callback in list(callbacks):
            try:
                callback(observation)
            except Exception:
                self._logger.exception("Error in observation callback")

    def _notify_connection(self, *, connected: bool) -> None:
        for listener in list(self._connection_listeners):
            try:
                listener(connected)
            except Exception:
                self._logger.exception("Error in connection listener")

    async def _send_subscribe(self, station_ids: list[str]) -> None:
        client = self._client
        if client is None:
            return
        try:
            await client.subscribe(
                [(station_topic(station_id), 0) for station_id in station_ids]
            )
        except aiomqtt.MqttError as err:
            # Every registered station is subscribed again after a reconnect.
            self._logger.debug("Subscribing failed: %s", err)

    async def _send_unsubscribe(self, station_id: str) -> None:
        client = self._client
        if client is None:
            return
        try:
            await client.unsubscribe(station_topic(station_id))
        except aiomqtt.MqttError as err:
            self._logger.debug("Unsubscribing failed: %s", err)

    def _create_task(self, coro: Coroutine[Any, Any, None]) -> None:
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
