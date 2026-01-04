"""Connection - Pure asyncio connection management for Kombu.

This module provides the Connection class for establishing connections
to Redis brokers using pure asyncio.

Example:
    async with Connection('redis://localhost') as conn:
        async with conn.Producer() as producer:
            await producer.publish({'hello': 'world'}, routing_key='my_queue')
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from .log import get_logger
from .transport.redis import Transport

if TYPE_CHECKING:
    from .entity import Queue
    from .transport.redis import Channel

__all__ = ('Connection',)

logger = get_logger(__name__)


class Connection:
    """A connection to the Redis broker.

    Pure asyncio implementation. All methods are async.

    Example:
        async with Connection('redis://localhost:6379') as conn:
            channel = await conn.channel()
            await channel.publish(b'hello', exchange='', routing_key='myqueue')

    Arguments:
        hostname: Broker URL (e.g., 'redis://localhost:6379').

    Keyword Arguments:
        transport_options: Additional options for the transport.
    """

    def __init__(
        self,
        hostname: str = 'redis://localhost:6379',
        transport_options: dict | None = None,
        **kwargs: Any,
    ):
        self._url = hostname
        self._transport_options = transport_options or {}
        self._transport: Transport | None = None
        self._default_channel: Channel | None = None
        self._closed = False

        # Parse URL for validation
        parsed = urlparse(hostname)
        if parsed.scheme not in ('redis', 'rediss'):
            raise ValueError(
                f"Unsupported transport scheme: {parsed.scheme}. "
                "This pure asyncio Kombu only supports 'redis://' and 'rediss://'."
            )

    @property
    def transport(self) -> Transport | None:
        """Get the transport."""
        return self._transport

    @property
    def is_connected(self) -> bool:
        """Check if connection is established."""
        return self._transport is not None and self._transport.is_connected

    async def connect(self) -> Connection:
        """Establish connection to the broker.

        Returns self for chaining.
        """
        if self._transport is None:
            self._transport = Transport(
                url=self._url,
                **self._transport_options,
            )
        await self._transport.connect()
        logger.debug('Connected to %s', self._url)
        return self

    async def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return
        self._closed = True

        if self._transport:
            await self._transport.close()
            self._transport = None

        self._default_channel = None

    async def channel(self) -> Channel:
        """Create a new channel.

        Returns a Channel object that can be used for messaging operations.
        """
        if not self.is_connected:
            await self.connect()
        return await self._transport.create_channel()

    async def default_channel(self) -> Channel:
        """Get or create the default channel.

        The default channel is reused for convenience operations.
        """
        if self._default_channel is None:
            self._default_channel = await self.channel()
        return self._default_channel

    def Producer(
        self,
        channel: Channel | None = None,
        **kwargs: Any,
    ) -> Producer:
        """Create a Producer for this connection.

        Args:
            channel: Optional channel to use. If not provided,
                     the default channel will be used.
            **kwargs: Additional arguments passed to Producer.

        Returns:
            A Producer instance.
        """
        from .messaging import Producer
        return Producer(self, channel=channel, **kwargs)

    def Consumer(
        self,
        queues: list[Queue],
        channel: Channel | None = None,
        **kwargs: Any,
    ) -> Consumer:
        """Create a Consumer for this connection.

        Args:
            queues: List of queues to consume from.
            channel: Optional channel to use.
            **kwargs: Additional arguments passed to Consumer.

        Returns:
            A Consumer instance.
        """
        from .messaging import Consumer
        return Consumer(self, queues=queues, channel=channel, **kwargs)

    def SimpleQueue(
        self,
        name: str,
        no_ack: bool | None = None,
        queue_opts: dict | None = None,
        exchange_opts: dict | None = None,
        channel: Channel | None = None,
        **kwargs: Any,
    ) -> SimpleQueue:
        """Create a SimpleQueue for easy point-to-point messaging.

        Args:
            name: Queue name.
            no_ack: Don't require message acknowledgment.
            queue_opts: Options passed to Queue declaration.
            exchange_opts: Options passed to Exchange declaration.
            channel: Optional channel to use.
            **kwargs: Additional arguments.

        Returns:
            A SimpleQueue instance.
        """
        from .simple import SimpleQueue
        return SimpleQueue(
            self,
            name=name,
            no_ack=no_ack,
            queue_opts=queue_opts,
            exchange_opts=exchange_opts,
            channel=channel,
            **kwargs,
        )

    async def drain_events(self, timeout: float | None = None) -> None:
        """Wait for a single event from the broker.

        This will block until a message arrives or timeout is reached.

        Args:
            timeout: Maximum time to wait in seconds.

        Raises:
            socket.timeout: If timeout is reached with no events.
        """
        import socket

        channel = await self.default_channel()
        delivered = await channel.drain_events(timeout=timeout)
        if not delivered and timeout:
            raise socket.timeout('timed out')

    async def ensure_connection(
        self,
        max_retries: int | None = None,
        interval_start: float = 2.0,
        interval_step: float = 2.0,
        interval_max: float = 30.0,
        callback: Any = None,
    ) -> Connection:
        """Ensure we have a connection to the broker.

        Will reconnect if connection is lost.

        Args:
            max_retries: Maximum number of retries (None = unlimited).
            interval_start: Initial retry interval.
            interval_step: Interval increase per retry.
            interval_max: Maximum retry interval.
            callback: Called after each retry attempt.

        Returns:
            self
        """
        import asyncio as aio

        retries = 0
        interval = interval_start

        while True:
            try:
                await self.connect()
                return self
            except Exception as exc:
                if max_retries is not None and retries >= max_retries:
                    raise

                if callback:
                    callback(exc, interval)

                logger.warning(
                    'Connection failed, retrying in %.2fs: %r',
                    interval, exc,
                )
                await aio.sleep(interval)

                retries += 1
                interval = min(interval + interval_step, interval_max)

    def clone(self, **kwargs: Any) -> Connection:
        """Create a copy of this connection with optional overrides.

        Args:
            **kwargs: Override connection parameters.

        Returns:
            A new Connection instance.
        """
        return Connection(
            hostname=kwargs.pop('hostname', self._url),
            transport_options=kwargs.pop(
                'transport_options', self._transport_options.copy()
            ),
            **kwargs,
        )

    async def release(self) -> None:
        """Release the connection (alias for close)."""
        await self.close()

    async def __aenter__(self) -> Connection:
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.close()

    def __repr__(self) -> str:
        return f'<Connection: {self._url} connected={self.is_connected}>'

    # Aliases for backwards compatibility concepts
    @property
    def connected(self) -> bool:
        """Alias for is_connected."""
        return self.is_connected
