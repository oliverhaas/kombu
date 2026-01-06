"""Integration tests for Redis Plus transport."""
from __future__ import annotations

import os
import socket
from time import sleep

import pytest
import redis

import kombu
from kombu.transport.redis_plus import Transport

from .common import (BaseExchangeTypes, BaseMessage, BasePriority,
                     BasicFunctionality)


def get_connection(
        hostname, port, vhost, user_name=None, password=None,
        transport_options=None):

    credentials = f'{user_name}:{password}@' if user_name else ''

    return kombu.Connection(
        f'redis+plus://{credentials}{hostname}:{port}',
        transport_options=transport_options
    )


@pytest.fixture(params=[None, {'global_keyprefix': '_prefixed_plus_'}])
def connection(request):
    # this fixture yields plain connections to broker and TLS encrypted
    return get_connection(
        hostname=os.environ.get('REDIS_HOST', 'localhost'),
        port=os.environ.get('REDIS_6379_TCP', '6379'),
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
        transport_options=request.param
    )


@pytest.fixture()
def invalid_connection():
    return kombu.Connection('redis+plus://localhost:12345')


@pytest.mark.env('redis')
def test_failed_credentials():
    """Tests denied connection when wrong credentials were provided."""
    with pytest.raises(redis.exceptions.AuthenticationError):
        get_connection(
            hostname=os.environ.get('REDIS_HOST', 'localhost'),
            port=os.environ.get('REDIS_6379_TCP', '6379'),
            vhost=None,
            user_name='wrong_redis_user',
            password='wrong_redis_password'
        ).connect()


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisPlusBasicFunctionality(BasicFunctionality):
    def test_failed_connection__ConnectionError(self, invalid_connection):
        # method raises transport exception
        with pytest.raises(redis.exceptions.ConnectionError) as ex:
            invalid_connection.connection
        assert ex.type in Transport.connection_errors


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisPlusBaseExchangeTypes(BaseExchangeTypes):
    pass


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisPlusPriority(BasePriority):

    # Redis Plus uses 256 priority levels (0-255)
    # Lower numbers = higher priority
    PRIORITY_ORDER = 'desc'

    def test_publish_consume(self, connection):
        test_queue = kombu.Queue(
            'priority_test_plus', routing_key='priority_test_plus', max_priority=255
        )

        received_messages = []

        def callback(body, message):
            received_messages.append(body)
            message.ack()

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                for msg, prio in [
                    [{'msg': 'first'}, 100],   # low priority
                    [{'msg': 'second'}, 10],   # high priority
                    [{'msg': 'third'}, 100],   # low priority
                ]:
                    producer.publish(
                        msg,
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='pickle',
                        priority=prio
                    )
                # Sleep to make sure that queue sorted based on priority
                sleep(0.5)
                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['pickle']
                )
                consumer.register_callback(callback)
                with consumer:
                    # drain_events() returns just on number in
                    # Virtual transports
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                # Second message (priority 10) must be received first
                assert received_messages[0] == {'msg': 'second'}
                assert received_messages[1] == {'msg': 'first'}
                assert received_messages[2] == {'msg': 'third'}

    def test_publish_requeue_consume(self, connection):
        test_queue = kombu.Queue(
            'priority_requeue_test_plus',
            routing_key='priority_requeue_test_plus', max_priority=255
        )

        received_messages = []
        received_message_bodies = []

        def callback(body, message):
            received_messages.append(message)
            received_message_bodies.append(body)
            # don't ack the message so it can be requeued

        with connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                for msg, prio in [
                    [{'msg': 'first'}, 100],   # low priority
                    [{'msg': 'second'}, 10],   # high priority
                    [{'msg': 'third'}, 100],   # low priority
                ]:
                    producer.publish(
                        msg,
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='pickle',
                        priority=prio
                    )
                # Sleep to make sure that queue sorted based on priority
                sleep(0.5)
                consumer = kombu.Consumer(
                    conn, [test_queue], accept=['pickle']
                )
                consumer.register_callback(callback)
                with consumer:
                    # drain_events() consumes only one value unlike in py-amqp.
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)

                # requeue the messages
                for msg in received_messages:
                    msg.requeue()
                received_messages.clear()
                received_message_bodies.clear()

                # add a fourth higher priority message
                producer.publish(
                    {'msg': 'fourth'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='pickle',
                    priority=0  # highest priority
                )

                with consumer:
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)
                    conn.drain_events(timeout=1)

                # Fourth message must be received first
                assert received_message_bodies[0] == {'msg': 'fourth'}
                assert received_message_bodies[1] == {'msg': 'second'}
                assert received_message_bodies[2] == {'msg': 'first'}
                assert received_message_bodies[3] == {'msg': 'third'}


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisPlusMessage(BaseMessage):
    pass


@pytest.mark.env('redis')
def test_RedisPlusConnectTimeout(monkeypatch):
    """Test that connection timeout is properly handled."""
    # simulate a connection timeout for a new connection
    def connect_timeout(self):
        raise socket.timeout
    monkeypatch.setattr(
        redis.connection.Connection, "_connect", connect_timeout)

    # ensure the timeout raises a TimeoutError
    with pytest.raises(redis.exceptions.TimeoutError):
        kombu.Connection('redis+plus://localhost:12345').connect()


@pytest.mark.env('redis')
class test_RedisPlusNativeDelayedDelivery:
    """Tests for native delayed delivery feature."""

    @pytest.fixture
    def delayed_connection(self):
        return get_connection(
            hostname=os.environ.get('REDIS_HOST', 'localhost'),
            port=os.environ.get('REDIS_6379_TCP', '6379'),
            vhost=None,
        )

    def test_transport_declares_native_delayed_support(self, delayed_connection):
        """Test that transport declares native delayed delivery support."""
        with delayed_connection as conn:
            transport = conn.transport
            assert transport.supports_native_delayed_delivery is True

    def test_channel_has_setup_teardown_methods(self, delayed_connection):
        """Test that channel has setup/teardown methods for delayed delivery."""
        with delayed_connection as conn:
            channel = conn.default_channel
            assert hasattr(channel, 'setup_native_delayed_delivery')
            assert hasattr(channel, 'teardown_native_delayed_delivery')
            assert callable(channel.setup_native_delayed_delivery)
            assert callable(channel.teardown_native_delayed_delivery)


@pytest.mark.env('redis')
class test_RedisPlusQueueOperations:
    """Tests for Redis Plus specific queue operations using sorted sets."""

    @pytest.fixture
    def queue_connection(self):
        return get_connection(
            hostname=os.environ.get('REDIS_HOST', 'localhost'),
            port=os.environ.get('REDIS_6379_TCP', '6379'),
            vhost=None,
        )

    def test_queue_uses_sorted_set(self, queue_connection):
        """Test that queues use Redis sorted sets."""
        test_queue = kombu.Queue('sorted_set_test', routing_key='sorted_set_test')

        with queue_connection as conn:
            with conn.channel() as channel:
                producer = kombu.Producer(channel)
                producer.publish(
                    {'test': 'message'},
                    retry=True,
                    exchange=test_queue.exchange,
                    routing_key=test_queue.routing_key,
                    declare=[test_queue],
                    serializer='pickle'
                )

                # Verify message is in queue
                size = channel._size('sorted_set_test')
                assert size >= 1

                # Clean up
                channel._purge('sorted_set_test')


@pytest.mark.env('redis')
def test_RedisPlusVisibilityTimeout():
    """Test visibility timeout configuration."""
    conn = get_connection(
        hostname=os.environ.get('REDIS_HOST', 'localhost'),
        port=os.environ.get('REDIS_6379_TCP', '6379'),
        vhost=None,
        transport_options={'visibility_timeout': 600}
    )

    with conn:
        channel = conn.default_channel
        assert channel.visibility_timeout == 600
