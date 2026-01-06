"""Unit tests for Redis Plus transport."""
from __future__ import annotations

import types
from collections import defaultdict
from itertools import count
from queue import Empty
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from kombu import Connection
from kombu.utils import eventio
from kombu.utils.json import dumps

if TYPE_CHECKING:
    from types import TracebackType


def _redis_modules():
    """Create mock redis modules for testing."""

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

    class TimeoutError(Exception):
        pass

    class BusyLoadingError(Exception):
        pass

    class NoScriptError(Exception):
        pass

    exceptions = types.ModuleType('redis.exceptions')
    exceptions.ConnectionError = ConnectionError
    exceptions.AuthenticationError = AuthenticationError
    exceptions.InvalidData = InvalidData
    exceptions.InvalidResponse = InvalidResponse
    exceptions.ResponseError = ResponseError
    exceptions.TimeoutError = TimeoutError
    exceptions.BusyLoadingError = BusyLoadingError
    exceptions.NoScriptError = NoScriptError

    class Redis:
        pass

    myredis = types.ModuleType('redis')
    myredis.exceptions = exceptions
    myredis.Redis = Redis

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

pytest.importorskip('redis')

from kombu.transport import redis_plus  # noqa


class ResponseError(Exception):
    pass


class Client:
    """Mock Redis client for testing."""

    queues = {}  # sorted sets for queues
    sets = defaultdict(set)
    hashes = defaultdict(dict)
    streams = defaultdict(list)
    shard_hint = None

    def __init__(self, db=None, port=None, connection_pool=None, **kwargs):
        self._called = []
        self._connection = None
        self.connection = self._sconnection(self)
        self._script_sha = 'mock_script_sha'

    def ping(self, *args, **kwargs):
        return True

    def script_load(self, script):
        return self._script_sha

    def evalsha(self, sha, numkeys, *args):
        return 0

    def delete(self, key):
        self.queues.pop(key, None)
        self.hashes.pop(key, None)

    def exists(self, key):
        return key in self.queues or key in self.sets or key in self.hashes

    def expire(self, key, seconds):
        return True

    def hset(self, key, *args, mapping=None):
        if mapping:
            self.hashes[key].update(mapping)
        elif len(args) == 2:
            k, v = args
            self.hashes[key][k] = v
        return 1

    def hget(self, key, k):
        return self.hashes[key].get(k)

    def hgetall(self, key):
        return self.hashes.get(key, {})

    def hdel(self, key, *fields):
        for f in fields:
            self.hashes[key].pop(f, None)

    def sadd(self, key, member, *args):
        self.sets[key].add(member)

    def smembers(self, key):
        return self.sets.get(key, set())

    def srem(self, key, *args):
        self.sets.pop(key, None)

    def zadd(self, key, mapping, **kwargs):
        if key not in self.queues:
            self.queues[key] = []
        for member, score in mapping.items():
            self.queues[key].append((member, score))
            # Sort by score
            self.queues[key].sort(key=lambda x: x[1])

    def zrem(self, key, *members):
        if key in self.queues:
            self.queues[key] = [
                (m, s) for m, s in self.queues[key] if m not in members
            ]

    def zcard(self, key):
        return len(self.queues.get(key, []))

    def zmpop(self, numkeys, *keys, min=False, max=False, count=1):
        """Mock ZMPOP - get and remove from sorted set."""
        for key in keys[:numkeys]:
            if key in self.queues and self.queues[key]:
                results = []
                for _ in range(min(count, len(self.queues[key]))):
                    if min:
                        member, score = self.queues[key].pop(0)
                    else:
                        member, score = self.queues[key].pop()
                    results.append((member, score))
                if results:
                    return key, results
        return None

    def xadd(self, key, fields, maxlen=None, approximate=True):
        msg_id = f'{len(self.streams[key])}-0'
        self.streams[key].append((msg_id, fields))
        return msg_id

    def xread(self, streams, count=None, block=None):
        results = []
        for stream_key, last_id in streams.items():
            if stream_key in self.streams:
                messages = self.streams[stream_key]
                # Simple: return all messages after last_id
                results.append((stream_key, messages))
        return results if results else None

    def __contains__(self, k):
        return k in self._called

    def pipeline(self):
        return Pipeline(self)

    def encode(self, value):
        return str(value)

    def _new_queue(self, key):
        self.queues[key] = []

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
            self.client = client
            self._sock = self._socket()

        def disconnect(self):
            self.disconnected = True

        def connect(self):
            pass

        def send_command(self, cmd, *args):
            self._sock.data.append((cmd, args))

    def info(self):
        return {'foo': 1}

    def parse_response(self, connection, type, **options):
        cmd, args = self.connection._sock.data.pop()
        self.connection._sock.data = []
        if type == 'BZMPOP':
            # Extract keys from args: timeout, numkeys, key1, key2, ..., MIN
            # timeout = args[0]  # not used in mock
            numkeys = args[1]
            keys = args[2:2 + numkeys]
            result = self.zmpop(numkeys, *keys, min=True, count=1)
            if result:
                return result
            raise Empty()
        return None


class Pipeline:

    def __init__(self, client):
        self.client = client
        self.stack = []

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        pass

    def __getattr__(self, key):
        if key not in self.__dict__:
            def _add(*args, **kwargs):
                self.stack.append((getattr(self.client, key), args, kwargs))
                return self
            return _add
        return self.__dict__[key]

    def execute(self):
        stack = list(self.stack)
        self.stack[:] = []
        return [fun(*args, **kwargs) for fun, args, kwargs in stack]


class Channel(redis_plus.Channel):

    def _get_client(self):
        return Client

    def _get_pool(self, asynchronous=False):
        return Mock()

    def _get_response_error(self):
        return ResponseError

    def _new_queue(self, queue, **kwargs):
        self.client._new_queue(self._queue_key(queue))

    def pipeline(self):
        return Pipeline(Client())


class Transport(redis_plus.Transport):
    Channel = Channel
    connection_errors = (KeyError,)
    channel_errors = (IndexError,)


class test_queue_score:
    """Test score calculation for sorted sets."""

    def test_priority_0_lowest_score(self):
        """Priority 0 (highest) should have lowest score."""
        score_p0 = redis_plus._queue_score(0, 1000)
        score_p255 = redis_plus._queue_score(255, 1000)
        assert score_p0 < score_p255

    def test_same_priority_earlier_time_lower_score(self):
        """Same priority, earlier time should have lower score."""
        score_early = redis_plus._queue_score(5, 1000)
        score_late = redis_plus._queue_score(5, 2000)
        assert score_early < score_late

    def test_priority_clamp(self):
        """Priority should be clamped to 0-255 range."""
        score_neg = redis_plus._queue_score(-10, 1000)
        score_zero = redis_plus._queue_score(0, 1000)
        assert score_neg == score_zero

        score_high = redis_plus._queue_score(300, 1000)
        score_255 = redis_plus._queue_score(255, 1000)
        assert score_high == score_255


class test_Channel:

    def setup_method(self):
        self.connection = self.create_connection()
        self.channel = self.connection.default_channel

    def teardown_method(self):
        # Clean up mock data
        Client.queues = {}
        Client.sets = defaultdict(set)
        Client.hashes = defaultdict(dict)
        Client.streams = defaultdict(list)

    def create_connection(self, **kwargs):
        return Connection(transport=Transport, **kwargs)

    def test_queue_key(self):
        """Test queue key generation."""
        key = self.channel._queue_key('myqueue')
        assert key == 'queue:myqueue'

    def test_message_key(self):
        """Test message key generation."""
        key = self.channel._message_key('tag123')
        assert key == 'message:tag123'

    def test_put_creates_message_hash(self):
        """Test _put creates message data in hash."""
        message = {
            'body': 'test body',
            'properties': {'priority': 5, 'delivery_tag': 'tag1'},
            'headers': {},
        }
        self.channel._put('testqueue', message)

        # Check message hash was created
        msg_key = self.channel._message_key('tag1')
        assert msg_key in Client.hashes
        assert 'payload' in Client.hashes[msg_key]
        assert 'queue' in Client.hashes[msg_key]

    def test_put_adds_to_queue(self):
        """Test _put adds message to queue sorted set."""
        message = {
            'body': 'test body',
            'properties': {'priority': 5, 'delivery_tag': 'tag1'},
            'headers': {},
        }
        self.channel._put('testqueue', message)

        queue_key = self.channel._queue_key('testqueue')
        assert queue_key in Client.queues
        assert len(Client.queues[queue_key]) == 1

    def test_put_with_eta_uses_messages_index(self):
        """Test _put with ETA adds to messages_index for delayed processing."""
        import time
        future_eta = time.time() + 3600  # 1 hour in future

        message = {
            'body': 'delayed body',
            'properties': {'priority': 5, 'delivery_tag': 'delayed_tag'},
            'headers': {'eta': future_eta},
        }
        self.channel._put('testqueue', message)

        # Check message is in messages_index (for delayed processing)
        assert 'messages_index' in Client.queues
        # The message should have native_delayed flag
        msg_key = self.channel._message_key('delayed_tag')
        assert Client.hashes[msg_key].get('native_delayed') == '1'

    def test_size_returns_queue_length(self):
        """Test _size returns correct queue length."""
        # Empty queue
        assert self.channel._size('emptyqueue') == 0

        # Add messages
        message = {
            'body': 'test',
            'properties': {'priority': 0, 'delivery_tag': 'tag1'},
            'headers': {},
        }
        self.channel._put('sizequeue', message)
        assert self.channel._size('sizequeue') == 1

    def test_purge_removes_all_messages(self):
        """Test _purge removes all messages from queue."""
        for i in range(5):
            message = {
                'body': f'test{i}',
                'properties': {'priority': 0, 'delivery_tag': f'tag{i}'},
                'headers': {},
            }
            self.channel._put('purgequeue', message)

        size = self.channel._purge('purgequeue')
        assert size == 5
        assert self.channel._size('purgequeue') == 0

    def test_priority_ordering(self):
        """Test messages are ordered by priority then time."""
        # Add lower priority message first (priority 9 = low priority)
        msg_low = {
            'body': 'low priority',
            'properties': {'priority': 9, 'delivery_tag': 'low'},
            'headers': {},
        }
        self.channel._put('prioqueue', msg_low)

        # Add higher priority message second (priority 1 = high priority)
        msg_high = {
            'body': 'high priority',
            'properties': {'priority': 1, 'delivery_tag': 'high'},
            'headers': {},
        }
        self.channel._put('prioqueue', msg_high)

        # First message out should be high priority (lower score)
        queue_key = self.channel._queue_key('prioqueue')
        members = Client.queues[queue_key]
        # First item (lowest score) should be 'high' (priority 1)
        assert members[0][0] == 'high'


class test_Transport:

    def setup_method(self):
        self.connection = Connection(transport=Transport)
        self.transport = self.connection.transport

    def test_supports_native_delayed_delivery(self):
        """Transport should declare native delayed delivery support."""
        assert self.transport.supports_native_delayed_delivery is True

    def test_driver_type(self):
        """Transport should have redis driver type."""
        assert self.transport.driver_type == 'redis'

    def test_driver_name(self):
        """Transport should have redis-plus driver name."""
        assert self.transport.driver_name == 'redis-plus'

    def test_implements_async(self):
        """Transport should implement asynchronous support."""
        assert self.transport.implements.asynchronous is True

    def test_implements_fanout(self):
        """Transport should support fanout exchange."""
        assert 'fanout' in self.transport.implements.exchange_type


class test_QoS:

    def setup_method(self):
        self.connection = Connection(transport=Transport)
        self.channel = self.connection.default_channel
        self.qos = redis_plus.QoS(self.channel)

    def teardown_method(self):
        Client.queues = {}
        Client.sets = defaultdict(set)
        Client.hashes = defaultdict(dict)

    def test_append_adds_to_messages_index(self):
        """Test append tracks message in messages_index."""
        # Create a mock message
        message = Mock()
        message.delivery_info = {
            'exchange': 'test_ex',
            'routing_key': 'test_rk',
        }
        delivery_tag = 'test_tag'

        # Setup message hash
        msg_key = self.channel._message_key(delivery_tag)
        Client.hashes[msg_key] = {
            'payload': dumps({'body': 'test'}),
            'queue': 'testqueue',
        }

        self.qos.append(message, delivery_tag)

        # Check message is tracked
        assert delivery_tag in self.qos._delivered

    def test_ack_removes_from_tracking(self):
        """Test ack removes message from tracking."""
        message = Mock()
        message.delivery_info = {
            'exchange': 'test_ex',
            'routing_key': 'test_rk',
        }
        delivery_tag = 'ack_tag'

        msg_key = self.channel._message_key(delivery_tag)
        Client.hashes[msg_key] = {
            'payload': dumps({'body': 'test'}),
            'queue': 'testqueue',
        }

        self.qos.append(message, delivery_tag)
        self.qos.ack(delivery_tag)

        # Message should be removed from delivered
        assert delivery_tag in self.qos._dirty


class test_native_delayed_delivery:
    """Test native delayed delivery interface."""

    def setup_method(self):
        self.connection = Connection(transport=Transport)
        self.channel = self.connection.default_channel

    def teardown_method(self):
        self.channel.teardown_native_delayed_delivery()
        Client.queues = {}
        Client.sets = defaultdict(set)
        Client.hashes = defaultdict(dict)

    def test_setup_starts_background_thread(self):
        """Test setup_native_delayed_delivery starts background thread."""
        self.channel.setup_native_delayed_delivery(['queue1'])

        assert self.channel._requeue_thread is not None
        assert self.channel._requeue_thread.is_alive()
        assert self.channel._requeue_stop_event is not None

    def test_teardown_stops_background_thread(self):
        """Test teardown_native_delayed_delivery stops background thread."""
        self.channel.setup_native_delayed_delivery(['queue1'])
        thread = self.channel._requeue_thread

        self.channel.teardown_native_delayed_delivery()

        assert not thread.is_alive()
        assert self.channel._requeue_thread is None

    def test_setup_idempotent(self):
        """Test calling setup twice doesn't create multiple threads."""
        self.channel.setup_native_delayed_delivery(['queue1'])
        thread1 = self.channel._requeue_thread

        self.channel.setup_native_delayed_delivery(['queue2'])
        thread2 = self.channel._requeue_thread

        assert thread1 is thread2


class test_transport_registration:
    """Test transport is properly registered."""

    def test_redis_plus_alias(self):
        """Test redis+plus transport alias works."""
        from kombu.transport import TRANSPORT_ALIASES
        assert 'redis+plus' in TRANSPORT_ALIASES

    def test_rediss_plus_alias(self):
        """Test rediss+plus transport alias works."""
        from kombu.transport import TRANSPORT_ALIASES
        assert 'rediss+plus' in TRANSPORT_ALIASES
