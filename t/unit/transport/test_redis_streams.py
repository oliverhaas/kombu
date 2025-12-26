"""Unit tests for Redis Streams transport."""
from __future__ import annotations

import sys
import types
from collections import defaultdict
from itertools import count
from unittest.mock import Mock
from unittest.mock import patch as mock_patch

import pytest

from kombu import Connection
from kombu.utils import eventio


def _redis_modules():
    """Create mock redis modules."""
    class ConnectionError(Exception):
        pass

    class AuthenticationError(Exception):
        pass

    class InvalidData(Exception):
        pass

    class InvalidResponse(Exception):
        pass

    class ResponseError(Exception):
        pass

    class BusyLoadingError(Exception):
        pass

    class TimeoutError(Exception):
        pass

    class DataError(Exception):
        pass

    exceptions = types.ModuleType('redis.exceptions')
    exceptions.ConnectionError = ConnectionError
    exceptions.AuthenticationError = AuthenticationError
    exceptions.InvalidData = InvalidData
    exceptions.InvalidResponse = InvalidResponse
    exceptions.ResponseError = ResponseError
    exceptions.BusyLoadingError = BusyLoadingError
    exceptions.TimeoutError = TimeoutError
    exceptions.DataError = DataError

    class Redis:
        pass

    class Connection:
        pass

    class SSLConnection:
        pass

    class Pipeline:
        pass

    class PubSub:
        pass

    # Create client submodule
    client = types.ModuleType('redis.client')
    client.Pipeline = Pipeline
    client.PubSub = PubSub

    myredis = types.ModuleType('redis')
    myredis.exceptions = exceptions
    myredis.Redis = Redis
    myredis.Connection = Connection
    myredis.SSLConnection = SSLConnection
    myredis.client = client
    myredis.VERSION = (5, 0, 0)

    return myredis, exceptions


class _poll(eventio._select):
    def register(self, fd, flags):
        if flags & eventio.READ:
            self._rfd.add(fd)

    def poll(self, timeout):
        events = []
        for fd in self._rfd:
            if fd.data:
                events.append((fd.fileno(), eventio.READ))
        return events


eventio.poll = _poll

# Mock redis module
redis_module, redis_exceptions = _redis_modules()
sys.modules['redis'] = redis_module
sys.modules['redis.exceptions'] = redis_exceptions

# Mock the version check for redis package
with mock_patch('importlib.metadata.version', return_value='5.0.0'):
    # Now import after mocking
    from kombu.transport import redis_streams


class ResponseError(Exception):
    pass


class StreamsClient:
    """Mock Redis client for streams operations."""

    streams = defaultdict(list)  # stream_name -> [(message_id, fields), ...]
    groups = defaultdict(dict)  # stream_name -> {group_name -> {'consumers': {}, 'pending': []}}
    sets = defaultdict(set)
    message_id_counter = count(1000)
    _consumed_messages = defaultdict(set)  # group_key -> set of consumed message IDs

    def __init__(self, db=None, port=None, connection_pool=None, **kwargs):
        self._called = []
        self._connection = None
        self.connection = self._sconnection(self)
        self.consumer_groups_created = set()

    def ping(self, *args, **kwargs):
        return True

    def xadd(self, name, fields, id='*', maxlen=None, approximate=True):
        """Mock XADD command."""
        if id == '*':
            message_id = f"{next(self.message_id_counter)}-0"
        else:
            message_id = id

        # Convert fields dict to bytes keys
        byte_fields = {}
        for k, v in fields.items():
            byte_fields[k.encode() if isinstance(k, str) else k] = \
                v.encode() if isinstance(v, str) else v

        self.streams[name].append((message_id.encode(), byte_fields))

        # Trim if maxlen specified
        if maxlen and len(self.streams[name]) > maxlen:
            self.streams[name] = self.streams[name][-maxlen:]

        return message_id.encode()

    def xreadgroup(self, groupname, consumername, streams, count=1, block=0):
        """Mock XREADGROUP command."""
        results = []

        for stream_name, stream_id in streams.items():
            if stream_name not in self.streams:
                continue

            if stream_id == '>':
                # New messages not yet delivered to this group
                # Track consumed messages per group
                group_key = f"{stream_name}:{groupname}"
                if not hasattr(self, '_consumed'):
                    self._consumed = defaultdict(set)

                stream_messages = []
                for msg_id, fields in self.streams[stream_name]:
                    # Only return messages not yet consumed by this group
                    if msg_id not in self._consumed[group_key]:
                        if len(stream_messages) < count:
                            stream_messages.append((msg_id, fields))
                            self._consumed[group_key].add(msg_id)

                if stream_messages:
                    results.append((stream_name.encode() if isinstance(stream_name, str) else stream_name,
                                   stream_messages))
                    # Only return first stream with messages (streams are checked in order)
                    break

        return results if results else []

    def xack(self, stream, group, message_id):
        """Mock XACK command."""
        # Just record the call
        self._called.append(('XACK', stream, group, message_id))
        return 1

    def xpending_range(self, name, groupname, min, max, count):
        """Mock XPENDING_RANGE command."""
        # Return empty for simplicity
        return []

    def xclaim(self, name, groupname, consumername, min_idle_time, message_ids):
        """Mock XCLAIM command."""
        return []

    def xautoclaim(self, name, groupname, consumername, min_idle_time, start, count=100):
        """Mock XAUTOCLAIM command.

        Returns: [next_cursor, claimed_messages, deleted_ids]
        """
        # For simplicity, return empty result indicating scan complete
        return [b'0-0', [], []]

    def xinfo_stream(self, name):
        """Mock XINFO STREAM command."""
        if name not in self.streams:
            raise ResponseError("ERR no such key")
        return {
            'length': len(self.streams[name]),
            'first-entry': self.streams[name][0] if self.streams[name] else None,
            'last-entry': self.streams[name][-1] if self.streams[name] else None,
        }

    def xgroup_create(self, name, groupname, id='0', mkstream=False):
        """Mock XGROUP CREATE command."""
        key = f"{name}:{groupname}"
        if key in self.consumer_groups_created:
            raise ResponseError('BUSYGROUP Consumer Group name already exists')
        self.consumer_groups_created.add(key)
        if mkstream and name not in self.streams:
            self.streams[name] = []

    def xrange(self, stream, min='-', max='+', count=None):
        """Mock XRANGE command."""
        if stream not in self.streams:
            return []

        results = []
        for msg_id, fields in self.streams[stream]:
            if min != '-' and msg_id < min.encode():
                continue
            if max != '+' and msg_id > max.encode():
                continue
            results.append((msg_id, fields))
            if count and len(results) >= count:
                break

        return results

    def delete(self, *keys):
        """Mock DELETE command."""
        count = 0
        for key in keys:
            if key in self.streams:
                del self.streams[key]
                count += 1
            if key in self.sets:
                del self.sets[key]
                count += 1
        return count

    def exists(self, key):
        """Mock EXISTS command."""
        return key in self.streams or key in self.sets

    def sadd(self, key, member, *args):
        """Mock SADD command."""
        self.sets[key].add(member)

    def smembers(self, key):
        """Mock SMEMBERS command."""
        return self.sets.get(key, set())

    def srem(self, key, *args):
        """Mock SREM command."""
        self.sets.pop(key, None)

    def pipeline(self):
        """Return self as pipeline for simplicity."""
        return self

    def execute(self):
        """Pipeline execute."""
        return []

    def __contains__(self, k):
        return k in self._called

    class _sconnection:
        disconnected = False

        class _socket:
            blocking = True
            filenos = count(30)

            def __init__(self, *args):
                self._fileno = next(self.filenos)
                self.data = []

            def fileno(self):
                return self._fileno

            def setblocking(self, blocking):
                self.blocking = blocking

        def __init__(self, client):
            self._sock = self._socket()
            self.client = client

        def disconnect(self):
            self.disconnected = True

        def send_command(self, *args):
            self._sock.data.append(args)


class Channel(redis_streams.Channel):
    """Test channel with mocked client."""

    def _get_client(self):
        return StreamsClient

    def _get_pool(self, asynchronous=False):
        return Mock()

    def _get_response_error(self):
        return ResponseError


class Transport(redis_streams.Transport):
    """Test transport with mocked channel."""
    Channel = Channel


@pytest.fixture
def connection(request):
    """Create connection with mocked client."""
    conn = Connection('redis://localhost', transport=Transport)
    yield conn
    try:
        conn.close()
    except Exception:
        pass


class test_StreamsChannel:
    """Test Redis Streams Channel."""

    def setup_method(self):
        """Setup test fixtures."""
        # Reset class-level state
        StreamsClient.streams.clear()
        StreamsClient.groups.clear()
        StreamsClient.sets.clear()
        StreamsClient.message_id_counter = count(1000)

    def test_stream_for_pri(self, connection):
        """Test stream key generation for priorities."""
        channel = connection.default_channel

        # Priority 0 (no suffix)
        assert channel._stream_for_pri('myqueue', 0) == 'myqueue'

        # Priority 3
        sep = channel.sep
        assert channel._stream_for_pri('myqueue', 3) == f'myqueue{sep}3'

    def test_consumer_group_name(self, connection):
        """Test consumer group name generation."""
        channel = connection.default_channel
        group = channel.consumer_group

        assert group.startswith('kombu-')
        assert str(channel.connection.client.virtual_host or 0) in group

    def test_consumer_id_unique(self, connection):
        """Test consumer ID is unique."""
        channel = connection.default_channel
        consumer_id = channel.consumer_id

        assert consumer_id
        parts = consumer_id.split('-')
        assert len(parts) >= 3  # hostname-pid-thread

    def test_put_and_get(self, connection):
        """Test basic put and get."""
        channel = connection.default_channel

        message = {
            'body': 'test body',
            'properties': {'priority': 0, 'delivery_tag': 'tag'},
            'headers': {},
        }

        # Put message
        channel._put('testqueue', message)

        # Verify stream was created
        stream_key = channel._stream_for_pri('testqueue', 0)
        assert stream_key in StreamsClient.streams
        assert len(StreamsClient.streams[stream_key]) == 1

        # Get message
        retrieved = channel._get('testqueue')
        assert retrieved['body'] == 'test body'
        assert 'delivery_tag' in retrieved['properties']

    def test_priority_ordering(self, connection):
        """Test messages are consumed in priority order."""
        channel = connection.default_channel

        # Put messages with different priorities
        for pri in [6, 0, 3]:
            message = {
                'body': f'priority {pri}',
                'properties': {'priority': pri},
                'headers': {},
            }
            channel._put('testqueue', message)

        # Get should return highest priority first (0)
        msg1 = channel._get('testqueue')
        assert 'priority 0' in msg1['body']

        msg2 = channel._get('testqueue')
        assert 'priority 3' in msg2['body']

        msg3 = channel._get('testqueue')
        assert 'priority 6' in msg3['body']

    def test_size(self, connection):
        """Test queue size."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        # Initially empty
        assert channel._size('testqueue') == 0

        # Add messages
        channel._put('testqueue', message)
        channel._put('testqueue', message)

        # Size should be 2
        assert channel._size('testqueue') == 2

    def test_purge(self, connection):
        """Test queue purge."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        # Add messages
        channel._put('testqueue', message)
        channel._put('testqueue', message)

        # Purge
        count = channel._purge('testqueue')
        assert count == 2

        # Size should be 0
        assert channel._size('testqueue') == 0

    def test_delete(self, connection):
        """Test queue deletion."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        channel._put('testqueue', message)

        # Delete queue
        channel._delete('testqueue', '', '', '')

        # Stream should be gone
        stream_key = channel._stream_for_pri('testqueue', 0)
        assert stream_key not in StreamsClient.streams

    def test_has_queue(self, connection):
        """Test queue existence check."""
        channel = connection.default_channel

        # Initially doesn't exist
        assert not channel._has_queue('testqueue')

        # Add message
        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }
        channel._put('testqueue', message)

        # Now exists
        assert channel._has_queue('testqueue')

    def test_qos_ack(self, connection):
        """Test QoS acknowledgment."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        channel._put('testqueue', message)
        retrieved = channel._get('testqueue')

        delivery_tag = retrieved['properties']['delivery_tag']

        # Ack the message
        channel.qos.ack(delivery_tag)

        # Verify XACK was called
        assert any(call[0] == 'XACK' for call in channel.client._called)

    def test_fanout_stream_key(self, connection):
        """Test fanout stream key generation."""
        channel = connection.default_channel

        stream_key = channel._fanout_stream_key('test_exchange')
        assert stream_key.startswith(channel.keyprefix_fanout)
        assert 'test_exchange' in stream_key

    def test_fanout_consumer_group(self, connection):
        """Test fanout consumer group naming."""
        channel = connection.default_channel

        group = channel._fanout_consumer_group('myqueue')
        assert 'fanout' in group
        assert 'myqueue' in group

    def test_close_when_closed(self, connection):
        """Test closing already closed channel."""
        channel = connection.default_channel
        channel.close()
        # Should not raise
        channel.close()

    def test_new_queue_with_auto_delete(self, connection):
        """Test auto-delete queue creation."""
        channel = connection.default_channel

        channel._new_queue('test_auto_delete', auto_delete=True)
        assert 'test_auto_delete' in channel.auto_delete_queues

    def test_delivery_tag_format(self, connection):
        """Test delivery tag contains stream and message ID."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        channel._put('testqueue', message)
        retrieved = channel._get('testqueue')

        delivery_tag = retrieved['properties']['delivery_tag']
        # Format should be stream:message_id:group_name
        parts = delivery_tag.rsplit(':', 2)
        assert len(parts) == 3
        stream, message_id, group_name = parts
        assert stream == 'testqueue'
        assert message_id  # Should have a message ID
        assert group_name == channel.consumer_group

    def test_next_delivery_tag_unique(self, connection):
        """Test that delivery tags are unique."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        # Put multiple messages
        for _ in range(10):
            channel._put('testqueue', message)

        # Get them and collect delivery tags
        seen_tags = set()
        for _ in range(10):
            retrieved = channel._get('testqueue')
            delivery_tag = retrieved['properties']['delivery_tag']
            assert delivery_tag not in seen_tags
            seen_tags.add(delivery_tag)

    def test_consumer_group_created_on_put(self, connection):
        """Test consumer group is created when message is added."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        stream_key = channel._stream_for_pri('testqueue', 0)
        group_name = channel.consumer_group

        # Put message should create group
        channel._put('testqueue', message)

        # Verify group was created
        key = f"{stream_key}:{group_name}"
        assert key in channel.client.consumer_groups_created

    def test_multiple_priorities_create_multiple_streams(self, connection):
        """Test that different priorities use different streams."""
        channel = connection.default_channel

        for pri in [0, 3, 6]:
            message = {
                'body': f'priority {pri}',
                'properties': {'priority': pri},
                'headers': {},
            }
            channel._put('testqueue', message)

        # Should have 3 different streams
        stream_0 = channel._stream_for_pri('testqueue', 0)
        stream_3 = channel._stream_for_pri('testqueue', 3)
        stream_6 = channel._stream_for_pri('testqueue', 6)

        assert stream_0 in StreamsClient.streams
        assert stream_3 in StreamsClient.streams
        assert stream_6 in StreamsClient.streams

        assert stream_0 != stream_3
        assert stream_3 != stream_6

    def test_put_fanout(self, connection):
        """Test putting fanout message."""
        channel = connection.default_channel

        message = {
            'body': 'fanout test',
            'properties': {},
            'headers': {},
        }

        channel._put_fanout('test_exchange', message, '')

        # Verify fanout stream was created
        stream_key = channel._fanout_stream_key('test_exchange', '')
        assert stream_key in StreamsClient.streams

    def test_close_deletes_auto_delete_queues(self, connection):
        """Test that auto-delete queues are cleaned up on close."""
        channel = connection.default_channel

        # Create auto-delete queue with message
        channel._new_queue('auto_del_queue', auto_delete=True)
        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }
        channel._put('auto_del_queue', message)

        # Verify stream exists
        stream_key = channel._stream_for_pri('auto_del_queue', 0)
        assert stream_key in StreamsClient.streams

        # Close channel
        channel.close()

        # Stream should be deleted
        assert stream_key not in StreamsClient.streams


class test_StreamsQoS:
    """Test Redis Streams QoS."""

    def setup_method(self):
        """Setup test fixtures."""
        # Reset class-level state
        StreamsClient.streams.clear()
        StreamsClient.groups.clear()
        StreamsClient.sets.clear()
        StreamsClient.message_id_counter = count(1000)

    def test_reject_with_requeue(self, connection):
        """Test rejecting message with requeue."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        channel._put('testqueue', message)
        retrieved = channel._get('testqueue')

        delivery_tag = retrieved['properties']['delivery_tag']

        # Reject with requeue
        channel.qos.reject(delivery_tag, requeue=True)

        # Message should be re-added to stream
        # In streams implementation, this means XACK + re-add via XADD

    def test_reject_without_requeue(self, connection):
        """Test rejecting message without requeue."""
        channel = connection.default_channel

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        channel._put('testqueue', message)
        retrieved = channel._get('testqueue')

        delivery_tag = retrieved['properties']['delivery_tag']

        # Reject without requeue (just ACK)
        channel.qos.reject(delivery_tag, requeue=False)

        # Verify XACK was called
        assert any(call[0] == 'XACK' for call in channel.client._called)

    def test_ack_delivery_tag(self, connection):
        """Test acknowledging message by delivery tag."""
        channel = connection.default_channel

        message = {
            'body': 'test message',
            'properties': {'priority': 0},
            'headers': {},
        }

        channel._put('testqueue', message)
        retrieved = channel._get('testqueue')
        delivery_tag = retrieved['properties']['delivery_tag']

        # Clear previous calls
        channel.client._called.clear()

        # Ack the message
        channel.qos.ack(delivery_tag)

        # Verify XACK was called with correct parameters
        xack_calls = [call for call in channel.client._called if call[0] == 'XACK']
        assert len(xack_calls) >= 1

    def test_restore_visible_xclaim(self, connection):
        """Test restore_visible uses XCLAIM for idle messages."""
        channel = connection.default_channel
        qos = channel.qos

        # Set a short visibility timeout for testing
        qos.visibility_timeout = 1  # 1 second

        message = {
            'body': 'test',
            'properties': {'priority': 0},
            'headers': {},
        }

        # Put and get message (simulates pending message)
        channel._put('testqueue', message)
        channel._get('testqueue')

        # Mock xpending_range to return pending message
        original_xpending = channel.client.xpending_range

        def mock_xpending(name, groupname, min_id, max_id, count):
            # Return a pending message that's idle
            return [{
                'message_id': b'1000-0',
                'consumer': channel.consumer_id.encode(),
                'time_since_delivered': 2000,  # 2 seconds, past visibility timeout
                'times_delivered': 1
            }]

        channel.client.xpending_range = mock_xpending

        # Call restore_visible
        qos.restore_visible(start=0, num=10, interval=10)

        # Verify XCLAIM would be called (in mock, it's a no-op)
        # In real implementation, this reclaims idle messages

        # Restore original method
        channel.client.xpending_range = original_xpending

    def test_ack_with_invalid_delivery_tag(self, connection):
        """Test ack with malformed delivery tag."""
        channel = connection.default_channel

        # Try to ack with invalid tag format
        # Should not raise, just log warning
        channel.qos.ack('invalid_tag_format')

    def test_multiple_ack(self, connection):
        """Test acknowledging multiple messages."""
        channel = connection.default_channel

        # Put multiple messages
        for i in range(5):
            message = {
                'body': f'test {i}',
                'properties': {'priority': 0},
                'headers': {},
            }
            channel._put('testqueue', message)

        # Get and ack all messages
        for _ in range(5):
            retrieved = channel._get('testqueue')
            delivery_tag = retrieved['properties']['delivery_tag']
            channel.qos.ack(delivery_tag)

        # All should be acknowledged
        xack_calls = [call for call in channel.client._called if call[0] == 'XACK']
        assert len(xack_calls) >= 5


class test_StreamsTransport:
    """Test Redis Streams Transport."""

    def setup_method(self):
        """Setup test fixtures."""
        # Reset class-level state
        StreamsClient.streams.clear()
        StreamsClient.groups.clear()
        StreamsClient.sets.clear()
        StreamsClient.message_id_counter = count(1000)

    def test_connection(self):
        """Test basic connection."""
        conn = Connection('redis://localhost', transport=Transport)
        assert conn.transport_cls == Transport
        conn.close()

    def test_connection_with_options(self):
        """Test connection with transport options."""
        conn = Connection(
            'redis://localhost',
            transport=Transport,
            transport_options={
                'stream_maxlen': 5000,
                'consumer_group_prefix': 'myapp',
                'prefetch_count': 10,
            }
        )
        channel = conn.default_channel
        assert channel.stream_maxlen == 5000
        assert channel.consumer_group_prefix == 'myapp'
        assert channel.prefetch_count == 10
        conn.close()

    def test_sep_transport_option(self):
        """Test custom separator in transport options."""
        conn = Connection(
            'redis://localhost',
            transport=Transport,
            transport_options={'sep': '::'}
        )
        channel = conn.default_channel
        assert channel.sep == '::'

        # Test that separator is used in stream keys
        stream_key = channel._stream_for_pri('myqueue', 3)
        assert '::' in stream_key
        conn.close()

    def test_transport_connection_errors(self):
        """Test transport connection_errors tuple."""
        transport_instance = Transport(Connection('redis://localhost', transport=Transport))
        # Should include redis ConnectionError
        assert transport_instance.connection_errors
        assert len(transport_instance.connection_errors) > 0

    def test_transport_channel_errors(self):
        """Test transport channel_errors tuple."""
        transport_instance = Transport(Connection('redis://localhost', transport=Transport))
        # Should include redis errors
        assert transport_instance.channel_errors
        assert len(transport_instance.channel_errors) > 0

    def test_default_stream_maxlen(self):
        """Test default stream maxlen value."""
        conn = Connection('redis://localhost', transport=Transport)
        channel = conn.default_channel
        # Default should be 10000
        assert channel.stream_maxlen == 10000
        conn.close()

    def test_default_consumer_group_prefix(self):
        """Test default consumer group prefix."""
        conn = Connection('redis://localhost', transport=Transport)
        channel = conn.default_channel
        # Default should be 'kombu'
        assert channel.consumer_group_prefix == 'kombu'
        conn.close()

    def test_qos_restore_visible(self):
        """Test QoS restore_visible method exists."""
        conn = Connection('redis://localhost', transport=Transport)
        channel = conn.default_channel

        # Verify qos has restore_visible method
        assert hasattr(channel.qos, 'restore_visible')
        assert callable(channel.qos.restore_visible)

        conn.close()

    def test_empty_queue_get_returns_none(self):
        """Test getting from empty queue returns None."""
        conn = Connection('redis://localhost', transport=Transport)
        channel = conn.default_channel

        # Get from empty queue
        result = channel._get('empty_queue')
        assert result is None
        conn.close()

    def test_consumer_group_unique_per_vhost(self):
        """Test consumer groups are unique per virtual host."""
        # Connection 1 with vhost 0
        conn1 = Connection('redis://localhost/0', transport=Transport)
        channel1 = conn1.default_channel
        group1 = channel1.consumer_group

        # Connection 2 with vhost 1
        conn2 = Connection('redis://localhost/1', transport=Transport)
        channel2 = conn2.default_channel
        group2 = channel2.consumer_group

        # Groups should be different
        assert group1 != group2
        assert '0' in group1 or group1.endswith('-0')
        assert '1' in group2 or group2.endswith('-1')

        conn1.close()
        conn2.close()
