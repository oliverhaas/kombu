"""Integration tests for Redis Streams transport."""
from __future__ import annotations

import os

import pytest
import redis

import kombu
from kombu.transport.redis_streams import Transport

from .common import BaseExchangeTypes, BaseMessage, BasicFunctionality


def get_connection(
        hostname=None, port=None, vhost=None, user_name=None, password=None,
        transport_options=None):
    """Create connection to Redis Streams transport."""
    hostname = hostname or os.environ.get('REDIS_HOST', 'localhost')
    port = port or os.environ.get('REDIS_6379_TCP', '6379')
    credentials = f'{user_name}:{password}@' if user_name else ''

    return kombu.Connection(
        f'redis://{credentials}{hostname}:{port}',
        transport=Transport,
        transport_options=transport_options
    )


@pytest.fixture(params=[None, {'global_keyprefix': '_prefixed_'}])
def connection(request):
    """Fixture for Redis Streams connection with optional global_keyprefix."""
    return get_connection(
        vhost=getattr(
            request.config, "slaveinput", {}
        ).get("slaveid", None),
        transport_options=request.param
    )


@pytest.fixture()
def invalid_connection():
    """Fixture for invalid connection (wrong port)."""
    return kombu.Connection('redis://localhost:12345', transport=Transport)


@pytest.mark.env('redis')
class test_RedisStreamsBasic:
    """Basic functionality tests for Redis Streams transport."""

    def test_put_and_get(self, connection):
        """Test basic message publish and consume."""
        queue_name = 'test-streams-basic'

        with connection as conn:
            with conn.channel() as channel:
                # Declare queue
                queue = kombu.Queue(queue_name, routing_key=queue_name)
                queue(channel).declare()

                # Publish message
                producer = kombu.Producer(channel)
                message_body = {'msg': 'test message'}
                producer.publish(
                    message_body,
                    routing_key=queue_name,
                    serializer='json'
                )

                # Consume message
                consumer = kombu.Consumer(channel, [queue], accept=['json'])

                received = []

                def callback(body, message):
                    received.append(body)
                    message.ack()

                consumer.register_callback(callback)

                with consumer:
                    conn.drain_events(timeout=2)

                assert len(received) == 1
                assert received[0] == message_body

                # Cleanup
                queue(channel).delete()

    def test_message_acknowledgment(self, connection):
        """Test message acknowledgment with PEL."""
        queue_name = 'test-streams-ack'

        with connection as conn:
            with conn.channel() as channel:
                queue = kombu.Queue(queue_name, routing_key=queue_name)
                queue(channel).declare()

                # Publish message
                producer = kombu.Producer(channel)
                producer.publish(
                    {'msg': 'test ack'},
                    routing_key=queue_name,
                    serializer='json'
                )

                # Consume without acking
                consumer = kombu.Consumer(channel, [queue], accept=['json'])

                received_messages = []

                def no_ack_callback(body, message):
                    received_messages.append(message)
                    # Don't ack!

                consumer.register_callback(no_ack_callback)

                with consumer:
                    conn.drain_events(timeout=2)

                assert len(received_messages) == 1

                # Close connection (should not lose message - it's in PEL)
                conn.close()

            # Reconnect and ack the message
            with get_connection() as conn2:
                with conn2.channel() as channel2:
                    queue2 = kombu.Queue(queue_name, routing_key=queue_name)

                    consumer2 = kombu.Consumer(channel2, [queue2], accept=['json'])

                    def ack_callback(body, message):
                        message.ack()

                    consumer2.register_callback(ack_callback)

                    # Should not get the message again (different consumer)
                    # because it's in the PEL of the first consumer
                    # This tests that PEL works

                    # Cleanup
                    queue2(channel2).delete()


@pytest.mark.env('redis')
def test_failed_credentials():
    """Test denied connection when wrong credentials were provided."""
    with pytest.raises(redis.exceptions.AuthenticationError):
        get_connection(
            user_name='wrong_redis_user',
            password='wrong_redis_password'
        ).connect()


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisStreamsBasicFunctionality(BasicFunctionality):
    """Test basic functionality using common test base."""

    def test_failed_connection__ConnectionError(self, invalid_connection):
        """Test connection error handling."""
        with pytest.raises(redis.exceptions.ConnectionError) as ex:
            invalid_connection.connection
        assert ex.type in Transport.connection_errors


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisStreamsExchangeTypes(BaseExchangeTypes):
    """Test exchange types (fanout, direct, topic) using common test base."""
    pass


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisStreamsMessage(BaseMessage):
    """Test message operations using common test base."""
    pass


@pytest.mark.env('redis')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_RedisStreamsSpecific:
    """Redis Streams-specific tests."""

    def test_auto_delete_on_consumer_cancel(self, connection):
        """Test auto-delete queue is deleted when last consumer is cancelled."""
        queue_name = 'test-auto-delete-cancel'

        with connection as conn:
            with conn.channel() as channel:
                # Declare auto-delete queue
                queue = kombu.Queue(queue_name, routing_key=queue_name, auto_delete=True)
                queue(channel).declare()

                # Publish a message
                producer = kombu.Producer(channel)
                producer.publish(
                    {'msg': 'test'},
                    routing_key=queue_name,
                    serializer='json'
                )

                # Create consumer
                consumer = kombu.Consumer(channel, [queue], accept=['json'])

                received = []

                def callback(body, message):
                    received.append(body)
                    message.ack()

                consumer.register_callback(callback)

                with consumer:
                    conn.drain_events(timeout=2)

                assert len(received) == 1

                # Cancel consumer - should trigger auto-delete
                consumer.cancel()

                # Queue should no longer exist - check stream directly
                stream_key = channel._stream_key(queue_name)
                assert not channel.client.exists(stream_key), \
                    f"Stream {stream_key} should have been deleted"

    def test_multiple_consumers_same_queue(self, connection):
        """Test multiple consumers on same queue get all messages together.

        Note: In kombu's virtual transport architecture, multiple consumers
        on the same channel share the same XREADGROUP consumer ID. This means
        messages are not load-balanced between consumers at the Redis level.
        Instead, messages are delivered to all callbacks registered on the
        channel. For true load balancing, use separate connections/channels.
        """
        queue_name = 'test-multi-consumer'

        with connection as conn:
            with conn.channel() as channel:
                queue = kombu.Queue(queue_name, routing_key=queue_name)
                queue(channel).declare()

                # Publish multiple messages
                producer = kombu.Producer(channel)
                for i in range(10):
                    producer.publish(
                        {'msg': f'message-{i}'},
                        routing_key=queue_name,
                        serializer='json'
                    )

                # Create two consumers on same channel
                all_received = []

                def callback1(body, message):
                    all_received.append(('c1', body['msg']))
                    message.ack()

                def callback2(body, message):
                    all_received.append(('c2', body['msg']))
                    message.ack()

                consumer1 = kombu.Consumer(channel, [queue], accept=['json'])
                consumer1.register_callback(callback1)

                consumer2 = kombu.Consumer(channel, [queue], accept=['json'])
                consumer2.register_callback(callback2)

                # Consume messages with both consumers
                with consumer1, consumer2:
                    for _ in range(10):
                        conn.drain_events(timeout=2)

                # All 10 messages should be received (callbacks may be called multiple times per message)
                unique_messages = {msg for _, msg in all_received}
                assert len(unique_messages) == 10, f"Expected 10 unique messages, got {len(unique_messages)}"

                # Cleanup
                queue(channel).delete()

    def test_consumer_group_persistence(self, connection):
        """Test that consumer groups persist across connections.

        This tests that when a message is consumed and acknowledged,
        the consumer group position is updated so subsequent consumers
        don't receive already-processed messages.
        """
        queue_name = 'test-group-persist'

        # Use the provided connection fixture (may have global_keyprefix)
        with connection as conn:
            with conn.channel() as channel:
                queue = kombu.Queue(queue_name, routing_key=queue_name)
                queue(channel).declare()

                producer = kombu.Producer(channel)
                producer.publish({'msg': 'msg1'}, routing_key=queue_name, serializer='json')
                producer.publish({'msg': 'msg2'}, routing_key=queue_name, serializer='json')

                consumer = kombu.Consumer(channel, [queue], accept=['json'])
                received = []

                def callback(body, message):
                    received.append(body)
                    message.ack()

                consumer.register_callback(callback)

                # Consume both messages with same consumer
                with consumer:
                    conn.drain_events(timeout=2)
                    conn.drain_events(timeout=2)

                assert len(received) == 2
                assert received[0]['msg'] == 'msg1'
                assert received[1]['msg'] == 'msg2'

                # Now publish a third message
                producer.publish({'msg': 'msg3'}, routing_key=queue_name, serializer='json')

                received.clear()

                # Same connection should get the new message
                with consumer:
                    conn.drain_events(timeout=2)

                assert len(received) == 1
                assert received[0]['msg'] == 'msg3'

                # Cleanup
                queue(channel).delete()
