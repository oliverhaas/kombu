"""Integration tests for async Redis transport functionality."""
from __future__ import annotations

import os
from contextlib import closing

import pytest

import kombu


def get_connection(
        hostname, port, vhost, user_name=None, password=None,
        transport_options=None):

    credentials = f'{user_name}:{password}@' if user_name else ''

    return kombu.Connection(
        f'redis://{credentials}{hostname}:{port}',
        transport_options=transport_options
    )


@pytest.fixture(params=[None, {'global_keyprefix': '_async_prefixed_'}])
def connection(request):
    return get_connection(
        hostname=os.environ.get('REDIS_HOST', 'localhost'),
        port=os.environ.get('REDIS_6379_TCP', '6379'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
        transport_options=request.param
    )


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_connect(connection):
    """Test async connection establishment."""
    # First establish sync connection (required for async)
    connection.connect()
    assert connection.connected
    assert connection.supports_async

    # Close connection
    await connection.aclose()


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_context_manager(connection):
    """Test async context manager for connection."""
    connection.connect()

    async with connection:
        assert connection.connected


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_publish_consume(connection):
    """Test async publish and consume."""
    test_queue = kombu.Queue('async_test', routing_key='async_test')

    with connection as conn:
        with conn.channel() as channel:
            producer = kombu.Producer(channel)

            # Async publish
            await producer.apublish(
                {'hello': 'async_world'},
                exchange=test_queue.exchange,
                routing_key=test_queue.routing_key,
                declare=[test_queue],
                serializer='json'
            )

            # Verify message was published (sync consume for verification)
            consumer = kombu.Consumer(
                conn, [test_queue], accept=['json']
            )

            received = []

            def callback(body, message):
                received.append(body)
                message.ack()

            consumer.register_callback(callback)
            with consumer:
                conn.drain_events(timeout=1)

            assert len(received) == 1
            assert received[0] == {'hello': 'async_world'}


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_simple_queue(connection):
    """Test async SimpleQueue operations."""
    with connection as conn:
        with closing(conn.SimpleQueue('async_simple_queue_test')) as queue:
            # Async put
            await queue.aput({'Hello': 'AsyncWorld'}, headers={'k1': 'v1'})

            # Async get
            message = await queue.aget(timeout=1)
            assert message.payload == {'Hello': 'AsyncWorld'}
            assert message.content_type == 'application/json'
            assert message.content_encoding == 'utf-8'
            assert message.headers == {'k1': 'v1'}
            message.ack()


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_drain_events(connection):
    """Test async drain_events."""
    test_queue = kombu.Queue('async_drain_test', routing_key='async_drain_test')

    with connection as conn:
        with conn.channel() as channel:
            producer = kombu.Producer(channel)
            producer.publish(
                {'msg': 'drain_test'},
                exchange=test_queue.exchange,
                routing_key=test_queue.routing_key,
                declare=[test_queue],
                serializer='json'
            )

            received = []

            def callback(body, message):
                received.append(body)
                message.ack()

            consumer = kombu.Consumer(
                conn, [test_queue], accept=['json']
            )
            consumer.register_callback(callback)

            with consumer:
                # Use async drain_events
                await conn.adrain_events(timeout=1)

            assert len(received) == 1
            assert received[0] == {'msg': 'drain_test'}


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_exchange_declare(connection):
    """Test async exchange declaration."""
    with connection as conn:
        with conn.channel() as channel:
            exchange = kombu.Exchange('async_exchange_test', type='direct')
            bound = exchange.bind(channel)

            await bound.adeclare()

            # Exchange should now exist (no error means success)


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_queue_declare(connection):
    """Test async queue declaration."""
    with connection as conn:
        with conn.channel() as channel:
            queue = kombu.Queue('async_queue_declare_test')
            bound = queue.bind(channel)

            await bound.adeclare()

            # Queue should now exist (no error means success)


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_maybe_declare(connection):
    """Test async maybe_declare utility."""
    from kombu.common import amaybe_declare

    with connection as conn:
        with conn.channel() as channel:
            exchange = kombu.Exchange('async_maybe_declare_test', type='direct')

            # First declaration
            result1 = await amaybe_declare(exchange, channel)
            assert result1 is True

            # Second declaration should be cached
            result2 = await amaybe_declare(exchange, channel)
            assert result2 is False


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_eventloop(connection):
    """Test async eventloop utility."""
    from kombu.common import aeventloop

    test_queue = kombu.Queue('async_eventloop_test', routing_key='async_eventloop_test')

    with connection as conn:
        with conn.channel() as channel:
            producer = kombu.Producer(channel)

            # Publish 3 messages
            for i in range(3):
                producer.publish(
                    {'msg': f'event_{i}'},
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='json'
                )

            received = []

            def callback(body, message):
                received.append(body)
                message.ack()

            consumer = kombu.Consumer(
                conn, [test_queue], accept=['json']
            )
            consumer.register_callback(callback)

            with consumer:
                async for _ in aeventloop(conn, limit=3, timeout=1, ignore_timeouts=True):
                    pass

            assert len(received) == 3


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_simple_buffer(connection):
    """Test async SimpleBuffer operations."""
    with connection as conn:
        with closing(conn.SimpleBuffer('async_simple_buffer_test')) as buf:
            # Async put
            await buf.aput({'Hello': 'AsyncBuffer'}, headers={'k1': 'v1'})

            # Async get
            message = await buf.aget(timeout=1)
            assert message.payload == {'Hello': 'AsyncBuffer'}
            assert message.content_type == 'application/json'
            assert message.content_encoding == 'utf-8'
            assert message.headers == {'k1': 'v1'}
            # SimpleBuffer uses no_ack=True by default


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_simple_queue_get_nowait(connection):
    """Test async SimpleQueue get_nowait."""
    with connection as conn:
        with closing(conn.SimpleQueue('async_get_nowait_test')) as queue:
            # Clear any existing messages
            queue.clear()

            # Try to get from empty queue
            with pytest.raises(queue.Empty):
                await queue.aget_nowait()

            # Put a message
            await queue.aput({'Hello': 'NoWait'})

            # Now get should succeed
            message = await queue.aget_nowait()
            assert message.payload == {'Hello': 'NoWait'}
            message.ack()


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_simple_queue_context_manager(connection):
    """Test async SimpleQueue context manager."""
    with connection as conn:
        async with conn.SimpleQueue('async_context_test') as queue:
            await queue.aput({'Hello': 'Context'})
            message = await queue.aget(timeout=1)
            assert message.payload == {'Hello': 'Context'}
            message.ack()


@pytest.mark.env('redis')
@pytest.mark.asyncio
async def test_async_channel_publish(connection):
    """Test async channel-level publish."""
    with connection as conn:
        with conn.channel() as channel:
            test_queue = kombu.Queue('async_channel_publish_test')
            test_queue = test_queue.bind(channel)
            test_queue.declare()

            # Publish directly using async method
            message = channel.prepare_message(
                body='{"test": "message"}',
                content_type='application/json',
                content_encoding='utf-8'
            )

            await channel.abasic_publish(
                message,
                exchange='',
                routing_key='async_channel_publish_test'
            )

            # Verify message was published
            msg = test_queue.get(no_ack=True, accept=['json'])
            assert msg is not None
