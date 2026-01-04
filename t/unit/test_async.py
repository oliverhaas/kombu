"""Unit tests for async functionality."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from kombu.utils.functional import aretry_over_time


class test_aretry_over_time:
    """Tests for aretry_over_time async utility."""

    class Predicate(Exception):
        pass

    def setup_method(self):
        self.index = 0

    async def async_myfun(self):
        if self.index < 9:
            raise self.Predicate()
        return 42

    async def async_errback(self, exc, intervals, retries):
        interval = next(intervals)
        sleepvals = (None, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 16.0)
        self.index += 1
        assert interval == sleepvals[self.index]
        return interval

    @pytest.mark.asyncio
    async def test_simple(self):
        """Test basic async retry functionality."""
        call_count = 0

        async def success_after_3():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise self.Predicate("try again")
            return "success"

        with patch.object(asyncio, 'sleep', new_callable=AsyncMock):
            result = await aretry_over_time(
                success_after_3,
                self.Predicate,
                errback=None,
                interval_max=14
            )
            assert result == "success"
            assert call_count == 3

    @pytest.mark.asyncio
    async def test_with_errback(self):
        """Test async retry with errback."""
        call_count = 0
        errback_calls = []

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise self.Predicate("try again")
            return "done"

        async def errback(exc, intervals, retries):
            interval = next(intervals)
            errback_calls.append((exc, interval))
            return interval

        with patch.object(asyncio, 'sleep', new_callable=AsyncMock):
            result = await aretry_over_time(
                fail_then_succeed,
                self.Predicate,
                errback=errback,
                interval_max=14
            )
            assert result == "done"
            assert len(errback_calls) == 2  # Called twice before success

    @pytest.mark.asyncio
    async def test_max_retries(self):
        """Test that max_retries limits retry attempts."""
        async def always_fail():
            raise self.Predicate("always fails")

        with patch.object(asyncio, 'sleep', new_callable=AsyncMock):
            with pytest.raises(self.Predicate):
                await aretry_over_time(
                    always_fail,
                    self.Predicate,
                    max_retries=2,
                    errback=None,
                    interval_max=14
                )

    @pytest.mark.asyncio
    async def test_timeout(self):
        """Test that timeout stops retrying."""
        async def always_fail():
            raise self.Predicate("always fails")

        with pytest.raises(self.Predicate):
            await aretry_over_time(
                always_fail,
                self.Predicate,
                errback=None,
                timeout=0.1
            )

    @pytest.mark.asyncio
    async def test_with_callback(self):
        """Test callback is called during retries."""
        callback_count = 0
        call_count = 0

        async def fail_twice_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise self.Predicate("retry")
            return "ok"

        def callback():
            nonlocal callback_count
            callback_count += 1

        with patch.object(asyncio, 'sleep', new_callable=AsyncMock):
            result = await aretry_over_time(
                fail_twice_then_succeed,
                self.Predicate,
                callback=callback,
                interval_start=1,
                interval_max=1
            )
        assert result == "ok"
        # Callback is called during each retry iteration
        assert callback_count > 0

    @pytest.mark.asyncio
    async def test_zero_retries(self):
        """Test max_retries=0 means no retries."""
        call_count = 0

        async def fail():
            nonlocal call_count
            call_count += 1
            raise self.Predicate("fail")

        with pytest.raises(self.Predicate):
            await aretry_over_time(
                fail,
                self.Predicate,
                max_retries=0,
                errback=None
            )
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_with_args_kwargs(self):
        """Test passing args and kwargs to function."""
        async def add(a, b, c=0):
            return a + b + c

        result = await aretry_over_time(
            add,
            Exception,
            args=(1, 2),
            kwargs={'c': 3}
        )
        assert result == 6


class test_async_connection:
    """Tests for async Connection methods."""

    @pytest.mark.asyncio
    async def test_supports_async_property(self):
        """Test supports_async property."""
        from kombu import Connection
        from t.mocks import Transport

        conn = Connection(transport=Transport)
        # Mock transport doesn't have async support
        assert conn.supports_async is False

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager."""
        from kombu import Connection
        from t.mocks import Transport

        conn = Connection(transport=Transport)
        # First, establish sync connection (mock doesn't support async)
        conn.connect()

        async with conn:
            assert conn.connected

        # After exiting, connection should be released

    @pytest.mark.asyncio
    async def test_aconnect_not_supported(self):
        """Test aconnect raises when transport doesn't support async."""
        from kombu import Connection
        from t.mocks import Transport

        conn = Connection(transport=Transport)

        with pytest.raises(NotImplementedError):
            await conn.aconnect()


class test_async_resource:
    """Tests for async Resource pool methods."""

    @pytest.mark.asyncio
    async def test_async_queue_initialization(self):
        """Test lazy async queue initialization."""
        from kombu.resource import Resource

        class TestResource(Resource):
            def setup(self):
                pass

            def new(self):
                return Mock()

        resource = TestResource()
        # Initially no async queue
        assert resource._async_resource is None

        # Getting queue initializes it
        queue = resource._get_async_queue()
        assert queue is not None
        assert isinstance(queue, asyncio.Queue)

    @pytest.mark.asyncio
    async def test_aacquire_arelease(self):
        """Test async acquire and release cycle."""
        from kombu.resource import Resource

        created_resources = []

        class TestResource(Resource):
            def setup(self):
                pass

            def new(self):
                r = Mock()
                created_resources.append(r)
                return r

        resource = TestResource(limit=2)

        # Acquire a resource
        r1 = await resource.aacquire()
        assert r1 is not None

        # Release it back
        await resource.arelease(r1)

        # Should be able to acquire again (from pool)
        r2 = await resource.aacquire()
        assert r2 is not None


class test_async_producer:
    """Tests for async Producer methods."""

    @pytest.mark.asyncio
    async def test_apublish_calls_channel(self):
        """Test that apublish uses async channel methods if available."""
        from kombu import Connection, Exchange, Producer
        from t.mocks import Transport

        connection = Connection(transport=Transport)
        connection.connect()
        exchange = Exchange('test', 'direct')

        producer = Producer(connection.channel(), exchange)

        # Mock the channel's abasic_publish method
        producer.channel.abasic_publish = AsyncMock(return_value=None)
        producer.channel.connection.client.declared_entities = set()

        await producer.apublish({'hello': 'world'})

        # Since abasic_publish exists, it should be called
        producer.channel.abasic_publish.assert_called_once()


class test_async_consumer:
    """Tests for async Consumer methods."""

    @pytest.mark.asyncio
    async def test_acancel(self):
        """Test async cancel."""
        from kombu import Connection, Consumer, Queue
        from t.mocks import Transport

        connection = Connection(transport=Transport)
        connection.connect()
        queue = Queue('test')

        consumer = Consumer(connection, [queue])

        # acancel should work even without consume being called
        await consumer.acancel()

    @pytest.mark.asyncio
    async def test_async_context_manager_with_mock_channel(self):
        """Test Consumer async context manager with proper channel mock."""
        from kombu import Consumer, Queue

        # Create a mock channel with necessary attributes
        mock_channel = Mock()
        mock_channel.handlers = {}
        mock_channel._tag_to_queue = {}
        mock_channel.basic_consume = Mock()
        mock_channel.basic_cancel = Mock()
        mock_channel.connection = Mock()
        mock_channel.connection.client.connection_errors = (Exception,)

        queue = Queue('test')
        queue = queue.bind(mock_channel)

        consumer = Consumer(mock_channel, [queue])

        async with consumer:
            pass

        # basic_cancel should be called on exit
        mock_channel.basic_cancel.assert_called()


class test_async_entity:
    """Tests for async entity methods."""

    @pytest.mark.asyncio
    async def test_exchange_adeclare(self):
        """Test async exchange declare."""
        from kombu import Connection, Exchange
        from t.mocks import Transport

        connection = Connection(transport=Transport)
        channel = connection.channel()
        exchange = Exchange('test', 'direct')
        bound = exchange.bind(channel)

        await bound.adeclare()

        assert 'exchange_declare' in channel

    @pytest.mark.asyncio
    async def test_queue_adeclare(self):
        """Test async queue declare."""
        from kombu import Connection, Queue
        from t.mocks import Transport

        connection = Connection(transport=Transport)
        channel = connection.channel()
        queue = Queue('test')
        bound = queue.bind(channel)

        await bound.adeclare()

        assert 'queue_declare' in channel


class test_async_common:
    """Tests for async common utilities."""

    @pytest.mark.asyncio
    async def test_amaybe_declare_cached(self):
        """Test amaybe_declare caches declarations."""
        from kombu import Connection, Exchange
        from kombu.common import amaybe_declare
        from t.mocks import Transport

        connection = Connection(transport=Transport)
        connection.connect()
        channel = connection.channel()
        exchange = Exchange('test', 'direct')

        # First declaration
        result1 = await amaybe_declare(exchange, channel)
        assert result1 is True

        # Second declaration should be cached
        result2 = await amaybe_declare(exchange, channel)
        assert result2 is False

    @pytest.mark.asyncio
    async def test_aeventloop(self):
        """Test async eventloop generator."""
        from kombu.common import aeventloop

        conn = Mock()
        conn.adrain_events = AsyncMock(return_value=None)

        events = []
        async for event in aeventloop(conn, limit=3):
            events.append(event)

        assert len(events) == 3
        assert conn.adrain_events.call_count == 3


class test_async_simple:
    """Tests for async SimpleQueue methods."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test SimpleQueue async context manager."""
        from kombu import Connection
        from kombu.simple import SimpleQueue
        from t.mocks import Transport

        connection = Connection(transport=Transport)
        connection.connect()

        queue = SimpleQueue(connection, 'test')

        async with queue:
            pass

        # Consumer should be cancelled on exit


class test_async_mailbox:
    """Tests for async Mailbox methods."""

    @pytest.mark.asyncio
    async def test_acast(self):
        """Test async cast (fire-and-forget)."""
        from kombu import Connection
        from kombu.pidbox import Mailbox
        from t.mocks import Transport

        connection = Connection(transport=Transport)

        mailbox = Mailbox('test')
        mailbox.connection = connection

        # Mock the internal _abroadcast method
        mailbox._abroadcast = AsyncMock(return_value=None)

        await mailbox.acast(['worker1'], 'ping', {})

        mailbox._abroadcast.assert_called_once()
