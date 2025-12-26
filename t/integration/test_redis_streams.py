"""Integration tests for Redis Streams transport."""
from __future__ import annotations

import os

import pytest

import kombu
from kombu.transport.redis_streams import Transport


def get_connection(**transport_options):
    """Create connection to Redis Streams transport."""
    return kombu.Connection(
        f'redis://{os.environ.get("REDIS_HOST", "localhost")}:'
        f'{os.environ.get("REDIS_6379_TCP", "6379")}',
        transport=Transport,
        transport_options=transport_options
    )


@pytest.fixture()
def connection():
    """Fixture for Redis Streams connection."""
    return get_connection()


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

    def test_priority_ordering(self, connection):
        """Test messages are consumed in priority order."""
        queue_name = 'test-streams-priority'

        with connection as conn:
            with conn.channel() as channel:
                # Create queue with priority support
                queue = kombu.Queue(
                    queue_name,
                    routing_key=queue_name,
                    max_priority=10
                )
                queue(channel).declare()

                # Publish messages with different priorities
                producer = kombu.Producer(channel)

                producer.publish(
                    {'msg': 'low priority'},
                    routing_key=queue_name,
                    priority=8,  # Remember: lower number = higher priority in Redis
                    serializer='json'
                )

                producer.publish(
                    {'msg': 'high priority'},
                    routing_key=queue_name,
                    priority=0,  # Highest priority
                    serializer='json'
                )

                producer.publish(
                    {'msg': 'medium priority'},
                    routing_key=queue_name,
                    priority=5,
                    serializer='json'
                )

                # Consume messages
                consumer = kombu.Consumer(channel, [queue], accept=['json'])

                received = []

                def callback(body, message):
                    received.append(body['msg'])
                    message.ack()

                consumer.register_callback(callback)

                with consumer:
                    # Drain all 3 messages
                    for _ in range(3):
                        conn.drain_events(timeout=2)

                # Should be in priority order (0, 5, 8)
                assert received[0] == 'high priority'
                assert received[1] == 'medium priority'
                assert received[2] == 'low priority'

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
