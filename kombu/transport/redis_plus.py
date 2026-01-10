"""Redis Plus transport module for Kombu.

A modern Redis transport using sorted sets for queues, Redis Streams for
durable fanout, and native delayed delivery support.

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes (via Redis Streams - durable)
* Supports Priority: Yes (256 levels, score-based)
* Supports Native Delayed Delivery: Yes
* Supports TTL: Yes (via message hash expiry)

Connection String
=================
Connection string has the following format:

.. code-block::

    redis+plus://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/VIRTUALHOST]
    rediss+plus://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/VIRTUALHOST]

Key Differences from Standard Redis Transport
=============================================
* Queues use sorted sets (BZMPOP) instead of lists (BRPOP)
* 256 priority levels instead of ~10 (via separate queues)
* Native delayed delivery - messages stored in broker, not worker memory
* Durable fanout via Redis Streams instead of lossy PUB/SUB
* Visibility timeout for message reliability

Transport Options
=================
* ``visibility_timeout``: (int) Seconds before unacked messages are requeued.
  Default: 300 (5 minutes).
* ``stream_maxlen``: (int) Maximum length of fanout streams. Default: 10000.
* ``global_keyprefix``: (str) Prefix for all Redis keys. Default: "".
* ``message_ttl``: (int) TTL in seconds for message hashes. Default: 259200
  (3 days).
* ``requeue_check_interval``: (int) Seconds between requeue checks.
  Default: 60.
* ``requeue_batch_limit``: (int) Max messages per requeue cycle.
  Default: 1000.
* ``socket_timeout``: (float) Socket timeout in seconds.
* ``socket_connect_timeout``: (float) Socket connect timeout in seconds.
* ``socket_keepalive``: (bool) Enable TCP keepalive.
* ``socket_keepalive_options``: (dict) TCP keepalive options.
* ``max_connections``: (int) Max connections in pool. Default: 10.
* ``health_check_interval``: (int) Health check interval. Default: 25.
* ``retry_on_timeout``: (bool) Retry on timeout.
* ``client_name``: (str) Redis client name.

Redis Key Schema
================
* ``{prefix}queue:{name}`` - Sorted Set: Queue messages (score = priority+time)
* ``{prefix}message:{tag}`` - Hash: Per-message data (payload, routing_key, etc)
* ``{prefix}messages_index`` - Sorted Set: Visibility timeout tracking
* ``{prefix}{db}.{exchange}`` - Stream: Fanout messages
* ``{prefix}_kombu.binding.{ex}`` - Set: Exchange bindings

Requires Redis 7.0+ for BZMPOP command.
"""

from __future__ import annotations

import functools
import numbers
import socket
import threading
import time as _time
from collections import namedtuple
from contextlib import contextmanager
from importlib.metadata import version
from queue import Empty
from threading import Lock

from packaging.version import Version

from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.log import get_logger
from kombu.utils import symbol_by_name
from kombu.utils.compat import register_after_fork
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ, poll
from kombu.utils.functional import accepts_argument
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.url import _parse_url

from . import virtual

try:
    import redis
    _REDIS_VERSION = Version(version("redis"))
    _REDIS_GET_CONNECTION_WITHOUT_ARGS = _REDIS_VERSION >= Version("5.3.0")
except ImportError:  # pragma: no cover
    redis = None
    _REDIS_VERSION = None
    _REDIS_GET_CONNECTION_WITHOUT_ARGS = None

try:
    from redis import sentinel
except ImportError:  # pragma: no cover
    sentinel = None

try:
    from redis import CredentialProvider
except ImportError:  # pragma: no cover
    CredentialProvider = None


logger = get_logger('kombu.transport.redis_plus')
crit, warning, debug = logger.critical, logger.warning, logger.debug


class AtomicCounter:
    """Threadsafe counter.

    Returns the value after inc/dec operations.
    """

    def __init__(self, initial=0):
        self._value = initial
        self._lock = Lock()

    def inc(self, n=1):
        with self._lock:
            self._value += n
            return self._value

    def dec(self, n=1):
        with self._lock:
            self._value -= n
            return self._value


# Default configuration
DEFAULT_PORT = 6379
DEFAULT_DB = 0
DEFAULT_VISIBILITY_TIMEOUT = 300  # 5 minutes
DEFAULT_STREAM_MAXLEN = 10000
DEFAULT_MESSAGE_TTL = 259200  # 3 days
DEFAULT_REQUEUE_CHECK_INTERVAL = 60  # 1 minute
DEFAULT_REQUEUE_BATCH_LIMIT = 1000
DEFAULT_HEALTH_CHECK_INTERVAL = 25

# Score calculation constants
# Score = (255 - priority) * PRIORITY_MULTIPLIER + timestamp_ms
# This gives us 256 priority levels with time-based ordering within each level
PRIORITY_MULTIPLIER = 10 ** 13  # Enough space for millisecond timestamps

# Key prefixes
QUEUE_KEY_PREFIX = 'queue:'
MESSAGE_KEY_PREFIX = 'message:'
MESSAGES_INDEX_KEY = 'messages_index'
BINDING_KEY_PREFIX = '_kombu.binding.'
STREAM_KEY_PREFIX = '/{db}.'

# Separator for compound keys
SEP = '\x06\x16'

error_classes_t = namedtuple('error_classes_t', (
    'connection_errors', 'channel_errors',
))


# Lua script to enqueue messages whose queue_at time has passed
# This handles both delayed messages and visibility timeout requeues
ENQUEUE_DUE_MESSAGES_SCRIPT = """
-- KEYS[1]: messages_index sorted set
-- ARGV[1]: current timestamp (ms)
-- ARGV[2]: batch limit
-- ARGV[3]: key prefix

local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local prefix = ARGV[3]

-- Get messages due for processing
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
if #due == 0 then
    return 0
end

local count = 0
for _, tag in ipairs(due) do
    local msg_key = prefix .. 'message:' .. tag
    local msg_data = redis.call('HGETALL', msg_key)

    if #msg_data > 0 then
        -- Convert flat array to table
        local msg = {}
        for i = 1, #msg_data, 2 do
            msg[msg_data[i]] = msg_data[i + 1]
        end

        local queue = msg['queue']
        local score = msg['score']
        local native_delayed = msg['native_delayed']

        if queue and score then
            local queue_key = prefix .. 'queue:' .. queue

            -- Add to queue
            redis.call('ZADD', queue_key, score, tag)

            -- Remove from messages_index
            redis.call('ZREM', KEYS[1], tag)

            -- Clear native_delayed flag and update visibility
            if native_delayed == '1' then
                redis.call('HDEL', msg_key, 'native_delayed')
            end

            count = count + 1
        end
    else
        -- Message hash doesn't exist, just remove from index
        redis.call('ZREM', KEYS[1], tag)
    end
end

return count
"""


def get_redis_error_classes():
    """Return tuple of redis error classes."""
    from redis import exceptions

    # This exception suddenly changed name between redis-py versions
    if hasattr(exceptions, 'InvalidData'):
        DataError = exceptions.InvalidData
    else:
        DataError = exceptions.DataError
    return error_classes_t(
        (virtual.Transport.connection_errors + (
            InconsistencyError,
            socket.error,
            IOError,
            OSError,
            exceptions.ConnectionError,
            exceptions.BusyLoadingError,
            exceptions.AuthenticationError,
            exceptions.TimeoutError)),
        (virtual.Transport.channel_errors + (
            DataError,
            exceptions.InvalidResponse,
            exceptions.ResponseError)),
    )


def get_redis_ConnectionError():
    """Return the redis ConnectionError exception class."""
    from redis import exceptions
    return exceptions.ConnectionError


def _current_time_ms():
    """Return current time in milliseconds."""
    return int(_time.time() * 1000)


def _queue_score(priority: int, timestamp_ms: int) -> float:
    """Calculate sorted set score for a message.

    Lower scores = higher priority + earlier time.
    Priority 0 is highest, 255 is lowest.

    Args:
        priority: Message priority (0-255, 0 is highest)
        timestamp_ms: Timestamp in milliseconds

    Returns:
        Score for ZADD command
    """
    # Clamp priority to valid range
    priority = max(0, min(255, priority))
    # Priority 0 = highest priority = lowest score = processed first
    # We use priority directly (not inverted) so priority 0 gets lower score
    return priority * PRIORITY_MULTIPLIER + timestamp_ms


def _after_fork_cleanup_channel(channel):
    channel._after_fork()


class GlobalKeyPrefixMixin:
    """Mixin to provide common logic for global key prefixing.

    Overriding all the methods used by Kombu with the same key prefixing logic
    would be cumbersome and inefficient. Hence, we override the command
    execution logic that is called by all commands.
    """

    PREFIXED_SIMPLE_COMMANDS = [
        "HDEL",
        "HGET",
        "HGETALL",
        "HLEN",
        "HSET",
        "EXPIRE",
        "LLEN",
        "LPUSH",
        "PUBLISH",
        "RPUSH",
        "RPOP",
        "SADD",
        "SREM",
        "SET",
        "SMEMBERS",
        "XADD",
        "XREAD",
        "XINFO",
        "ZADD",
        "ZCARD",
        "ZREM",
        "ZRANGEBYSCORE",
        "ZREVRANGEBYSCORE",
    ]

    PREFIXED_COMPLEX_COMMANDS = {
        "DEL": {"args_start": 0, "args_end": None},
        "BRPOP": {"args_start": 0, "args_end": -1},
        "BZMPOP": {"args_start": 1, "args_end": -2},  # timeout numkeys key [key ...] MIN|MAX [COUNT count]
        "EVALSHA": {"args_start": 2, "args_end": 3},
        "WATCH": {"args_start": 0, "args_end": None},
    }

    def _prefix_args(self, args):
        args = list(args)
        command = args.pop(0)

        if command in self.PREFIXED_SIMPLE_COMMANDS:
            args[0] = self.global_keyprefix + str(args[0])
        elif command in self.PREFIXED_COMPLEX_COMMANDS:
            args_start = self.PREFIXED_COMPLEX_COMMANDS[command]["args_start"]
            args_end = self.PREFIXED_COMPLEX_COMMANDS[command]["args_end"]

            pre_args = args[:args_start] if args_start > 0 else []
            post_args = []

            if args_end is not None:
                post_args = args[args_end:]

            args = pre_args + [
                self.global_keyprefix + str(arg)
                for arg in args[args_start:args_end]
            ] + post_args

        return [command, *args]

    def parse_response(self, connection, command_name, **options):
        """Parse a response from the Redis server.

        Method wraps ``redis.parse_response()`` to remove prefixes of keys
        returned by redis command.
        """
        ret = super().parse_response(connection, command_name, **options)
        if command_name == 'BZMPOP' and ret:
            # BZMPOP returns [key, [[member, score], ...]]
            key, members = ret
            key = key[len(self.global_keyprefix):]
            return key, members
        return ret

    def execute_command(self, *args, **kwargs):
        return super().execute_command(*self._prefix_args(args), **kwargs)

    def pipeline(self, transaction=True, shard_hint=None):
        return PrefixedRedisPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
            global_keyprefix=self.global_keyprefix,
        )


class PrefixedStrictRedis(GlobalKeyPrefixMixin, redis.Redis):
    """Returns a ``StrictRedis`` client that prefixes the keys it uses."""

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.Redis.__init__(self, *args, **kwargs)


class PrefixedRedisPipeline(GlobalKeyPrefixMixin, redis.client.Pipeline):
    """Custom Redis pipeline that takes global_keyprefix into consideration."""

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.client.Pipeline.__init__(self, *args, **kwargs)


class QoS(virtual.QoS):
    """Redis Plus QoS with visibility timeout support."""

    restore_at_shutdown = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vrestore_count = 0

    def append(self, message, delivery_tag):
        """Track message for visibility timeout."""
        delivery = message.delivery_info
        EX, RK = delivery['exchange'], delivery['routing_key']

        # Store in messages_index for visibility timeout tracking
        with self.channel.conn_or_acquire() as client:
            visibility_timeout = self.channel.visibility_timeout
            queue_at = _current_time_ms() + (visibility_timeout * 1000)

            # Update message hash with visibility info
            msg_key = self.channel._message_key(delivery_tag)
            client.hset(msg_key, mapping={
                'queue_at': str(queue_at),
                'exchange': EX,
                'routing_key': RK,
            })

            # Add to messages_index with queue_at as score
            client.zadd(
                self.channel._messages_index_key,
                {delivery_tag: queue_at}
            )

        super().append(message, delivery_tag)

    def ack(self, delivery_tag):
        """Acknowledge message and remove from tracking."""
        self._remove_from_indices(delivery_tag)
        super().ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        """Reject message, optionally requeuing it."""
        if requeue:
            self._restore_by_tag(delivery_tag)
        else:
            self._remove_from_indices(delivery_tag)
        # Call parent's ack since we've handled the message
        super().ack(delivery_tag)

    def _remove_from_indices(self, delivery_tag):
        """Remove message from tracking indices."""
        with self.channel.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                pipe.zrem(self.channel._messages_index_key, delivery_tag)
                pipe.delete(self.channel._message_key(delivery_tag))
                pipe.execute()

    def _restore_by_tag(self, delivery_tag):
        """Restore message to its queue."""
        with self.channel.conn_or_acquire() as client:
            msg_key = self.channel._message_key(delivery_tag)
            msg_data = client.hgetall(msg_key)

            if msg_data:
                queue = bytes_to_str(msg_data.get(b'queue', b''))
                score = msg_data.get(b'score')

                if queue and score:
                    # Mark as redelivered
                    payload_raw = msg_data.get(b'payload')
                    if payload_raw:
                        payload = loads(bytes_to_str(payload_raw))
                        try:
                            payload['headers']['redelivered'] = True
                            payload['properties']['delivery_info']['redelivered'] = True
                        except KeyError:
                            pass
                        client.hset(msg_key, 'payload', dumps(payload))

                    # Re-add to queue
                    queue_key = self.channel._queue_key(queue)
                    client.zadd(queue_key, {delivery_tag: float(score)})

            # Remove from messages_index
            client.zrem(self.channel._messages_index_key, delivery_tag)

    def restore_visible(self, start=0, num=10, interval=10):
        """Restore messages that have exceeded visibility timeout.

        This is called periodically during polling to handle timed-out messages.
        The actual restoration is done by the Lua script via _enqueue_due_messages.
        """
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return

        # The channel's background thread handles this via _enqueue_due_messages
        # but we can also trigger it here during polling
        try:
            self.channel._enqueue_due_messages()
        except Exception:
            # Don't let restoration errors break polling
            pass


class MultiChannelPoller:
    """Async I/O poller for Redis Plus transport.

    Uses BZMPOP for sorted set queues instead of BRPOP.
    """

    eventflags = READ | ERR

    #: Set by :meth:`get` while reading from the socket.
    _in_protected_read = False

    #: Set of one-shot callbacks to call after reading from socket.
    after_read = None

    def __init__(self):
        # active channels
        self._channels = set()
        # file descriptor -> channel map.
        self._fd_to_chan = {}
        # channel -> socket map
        self._chan_to_sock = {}
        # poll implementation (epoll/kqueue/select)
        self.poller = poll()
        # one-shot callbacks called after reading from socket.
        self.after_read = set()

    def close(self):
        for fd in self._chan_to_sock.values():
            try:
                self.poller.unregister(fd)
            except (KeyError, ValueError):
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()

    def add(self, channel):
        self._channels.add(channel)

    def discard(self, channel):
        self._channels.discard(channel)

    def _on_connection_disconnect(self, connection):
        try:
            self.poller.unregister(connection._sock)
        except (AttributeError, TypeError):
            pass

    def _register(self, channel, client, type):
        if (channel, client, type) in self._chan_to_sock:
            self._unregister(channel, client, type)
        if client.connection._sock is None:   # not connected yet.
            client.connection.connect()
        sock = client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, type)
        self._chan_to_sock[(channel, client, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, type)])

    def _client_registered(self, channel, client, cmd):
        if getattr(client, 'connection', None) is None:
            if _REDIS_GET_CONNECTION_WITHOUT_ARGS:
                client.connection = client.connection_pool.get_connection()
            else:
                client.connection = client.connection_pool.get_connection('_')
        return (client.connection._sock is not None and
                (channel, client, cmd) in self._chan_to_sock)

    def _register_BZMPOP(self, channel):
        """Enable BZMPOP mode for channel."""
        ident = channel, channel.client, 'BZMPOP'
        if not self._client_registered(channel, channel.client, 'BZMPOP'):
            channel._in_poll = False
            self._register(*ident)
        if not channel._in_poll:  # send BZMPOP
            channel._bzmpop_start()

    def _register_XREAD(self, channel):
        """Enable XREAD mode for channel (for fanout via streams)."""
        if not self._client_registered(channel, channel.stream_client, 'XREAD'):
            channel._in_stream_read = False
            self._register(channel, channel.stream_client, 'XREAD')
        if not channel._in_stream_read:
            channel._xread_start()

    def on_poll_start(self):
        for channel in self._channels:
            if channel.active_queues:           # BZMPOP mode?
                if channel.qos.can_consume():
                    self._register_BZMPOP(channel)
            if channel.active_fanout_queues:    # XREAD mode?
                self._register_XREAD(channel)

    def on_poll_init(self, poller):
        self.poller = poller
        for channel in self._channels:
            return channel.qos.restore_visible(
                num=channel.requeue_batch_limit,
            )

    def maybe_restore_messages(self):
        for channel in self._channels:
            if channel.active_queues:
                # only need to do this once, as they are not local to channel.
                return channel.qos.restore_visible(
                    num=channel.requeue_batch_limit,
                )

    def on_readable(self, fileno):
        chan, type = self._fd_to_chan[fileno]
        if chan.qos.can_consume():
            chan.handlers[type]()

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, type = self._fd_to_chan[fileno]
            chan._poll_error(type)

    def get(self, callback, timeout=None):
        self._in_protected_read = True
        try:
            for channel in self._channels:
                if channel.active_queues:           # BZMPOP mode?
                    if channel.qos.can_consume():
                        self._register_BZMPOP(channel)
                if channel.active_fanout_queues:    # XREAD mode?
                    self._register_XREAD(channel)

            events = self.poller.poll(timeout)
            if events:
                for fileno, event in events:
                    ret = self.handle_event(fileno, event)
                    if ret:
                        return
            # - no new data, so try to restore messages.
            # - reset active redis commands.
            self.maybe_restore_messages()
            raise Empty()
        finally:
            self._in_protected_read = False
            while self.after_read:
                try:
                    fun = self.after_read.pop()
                except KeyError:
                    break
                else:
                    fun()

    @property
    def fds(self):
        return self._fd_to_chan


class Channel(virtual.Channel):
    """Redis Plus Channel using sorted sets for queues."""

    QoS = QoS

    _client = None
    _stream_client = None
    _closing = False
    supports_fanout = True

    # Key patterns
    keyprefix_queue = BINDING_KEY_PREFIX + '%s'
    keyprefix_fanout = STREAM_KEY_PREFIX
    sep = SEP

    # State tracking
    _in_poll = False
    _in_stream_read = False
    _fanout_queues = {}

    # Configuration defaults
    visibility_timeout = DEFAULT_VISIBILITY_TIMEOUT
    stream_maxlen = DEFAULT_STREAM_MAXLEN
    message_ttl = DEFAULT_MESSAGE_TTL
    requeue_check_interval = DEFAULT_REQUEUE_CHECK_INTERVAL
    requeue_batch_limit = DEFAULT_REQUEUE_BATCH_LIMIT
    socket_timeout = None
    socket_connect_timeout = None
    socket_keepalive = None
    socket_keepalive_options = None
    retry_on_timeout = None
    max_connections = 10
    health_check_interval = DEFAULT_HEALTH_CHECK_INTERVAL
    client_name = None
    global_keyprefix = ''

    # Priority settings (256 levels, 0-255)
    min_priority = 0
    max_priority = 255
    default_priority = 0

    _async_pool = None
    _pool = None

    # Class-level background thread for delayed/timeout message processing
    # Shared across all channels, started when first channel created,
    # stopped when last channel closed (following GCP Pub/Sub pattern)
    _requeue_thread: threading.Thread = None
    _requeue_stop_event = threading.Event()
    _n_channels = AtomicCounter()

    # Lua script SHA
    _enqueue_script_sha = None

    from_transport_options = (
        virtual.Channel.from_transport_options +
        ('visibility_timeout',
         'stream_maxlen',
         'message_ttl',
         'requeue_check_interval',
         'requeue_batch_limit',
         'global_keyprefix',
         'socket_timeout',
         'socket_connect_timeout',
         'socket_keepalive',
         'socket_keepalive_options',
         'max_connections',
         'health_check_interval',
         'retry_on_timeout',
         'client_name')  # <-- do not add comma here!
    )

    connection_class = redis.Connection if redis else None
    connection_class_ssl = redis.SSLConnection if redis else None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._registered = False
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self.auto_delete_queues = set()
        self._fanout_to_queue = {}
        self._stream_offsets = {}  # Track stream read positions
        self.handlers = {
            'BZMPOP': self._bzmpop_read,
            'XREAD': self._xread_receive
        }
        self.bzmpop_timeout = self.connection.brpop_timeout

        # Evaluate connection.
        try:
            self.client.ping()
        except Exception:
            self._disconnect_pools()
            raise

        # Register Lua script
        self._register_scripts()

        self.connection.cycle.add(self)  # add to channel poller.
        self._registered = True

        # copy errors, in case channel closed but threads still
        # are still waiting for data.
        self.connection_errors = self.connection.connection_errors

        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_channel)

        # Start background thread for delayed/visibility timeout processing
        # when first channel is created (class-level, shared across channels)
        if Channel._n_channels.inc() == 1:
            Channel._requeue_stop_event.clear()
            Channel._requeue_thread = threading.Thread(
                target=self._requeue_loop,
                daemon=True,
                name='redis-plus-requeue',
            )
            Channel._requeue_thread.start()
            debug('Started native delayed delivery background thread')

    def _register_scripts(self):
        """Register Lua scripts with Redis."""
        with self.conn_or_acquire() as client:
            self._enqueue_script_sha = client.script_load(
                ENQUEUE_DUE_MESSAGES_SCRIPT
            )

    def _after_fork(self):
        self._disconnect_pools()

    def _disconnect_pools(self):
        pool = self._pool
        async_pool = self._async_pool

        self._async_pool = self._pool = None

        if pool is not None:
            pool.disconnect()

        if async_pool is not None:
            async_pool.disconnect()

    def _on_connection_disconnect(self, connection):
        if self._in_poll is connection:
            self._in_poll = None
        if self._in_stream_read is connection:
            self._in_stream_read = None
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    # Key generation methods

    def _queue_key(self, queue: str) -> str:
        """Get Redis key for queue sorted set."""
        return f"{QUEUE_KEY_PREFIX}{queue}"

    def _message_key(self, delivery_tag: str) -> str:
        """Get Redis key for message hash."""
        return f"{MESSAGE_KEY_PREFIX}{delivery_tag}"

    @property
    def _messages_index_key(self) -> str:
        """Get Redis key for messages index sorted set."""
        return MESSAGES_INDEX_KEY

    def _stream_key(self, exchange: str) -> str:
        """Get Redis key for fanout stream."""
        return f"{self.keyprefix_fanout}{exchange}"

    # Message operations

    def _put(self, queue, message, **kwargs):
        """Publish message to queue.

        Messages are stored in a sorted set with score based on
        priority and timestamp for proper ordering.
        """
        # Generate delivery tag if not present
        delivery_tag = message['properties'].get('delivery_tag')
        if not delivery_tag:
            delivery_tag = self._next_delivery_tag()
            message['properties']['delivery_tag'] = delivery_tag

        # Get priority and calculate score
        priority = self._get_message_priority(message, reverse=False)
        timestamp_ms = _current_time_ms()

        # Check for ETA/countdown (delayed delivery)
        eta_ms = None
        headers = message.get('headers', {})
        if headers.get('eta'):
            # ETA is an ISO timestamp string or unix timestamp
            eta = headers['eta']
            if isinstance(eta, str):
                # Parse ISO format timestamp
                import datetime
                try:
                    dt = datetime.datetime.fromisoformat(eta.replace('Z', '+00:00'))
                    eta_ms = int(dt.timestamp() * 1000)
                except (ValueError, AttributeError):
                    pass
            elif isinstance(eta, (int, float)):
                eta_ms = int(eta * 1000)

        # Calculate score - delayed messages get future timestamp in score
        if eta_ms and eta_ms > timestamp_ms:
            score = _queue_score(priority, eta_ms)
            native_delayed = True
            queue_at = eta_ms
        else:
            score = _queue_score(priority, timestamp_ms)
            native_delayed = False
            queue_at = None

        with self.conn_or_acquire() as client:
            # Store message data in hash
            msg_key = self._message_key(delivery_tag)
            msg_data = {
                'payload': dumps(message),
                'queue': queue,
                'score': str(score),
                'priority': str(priority),
                'created_at': str(timestamp_ms),
            }
            if native_delayed:
                msg_data['native_delayed'] = '1'
                msg_data['queue_at'] = str(queue_at)

            with client.pipeline() as pipe:
                pipe.hset(msg_key, mapping=msg_data)
                pipe.expire(msg_key, self.message_ttl)

                if native_delayed:
                    # Add to messages_index for delayed processing
                    pipe.zadd(self._messages_index_key, {delivery_tag: queue_at})
                else:
                    # Add directly to queue
                    queue_key = self._queue_key(queue)
                    pipe.zadd(queue_key, {delivery_tag: score})

                pipe.execute()

    def _get(self, queue, timeout=None):
        """Consume message from queue using synchronous ZMPOP."""
        with self.conn_or_acquire() as client:
            queue_key = self._queue_key(queue)

            # Use ZMPOP (non-blocking) to get lowest score message
            # ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
            result = client.zmpop(1, queue_key, min=True, count=1)

            if result:
                key, members = result
                if members:
                    delivery_tag, score = members[0]
                    delivery_tag = bytes_to_str(delivery_tag)

                    # Get message payload from hash
                    msg_key = self._message_key(delivery_tag)
                    payload_raw = client.hget(msg_key, 'payload')

                    if payload_raw:
                        return loads(bytes_to_str(payload_raw))

            raise Empty()

    def _bzmpop_start(self, timeout=None):
        """Start BZMPOP command for async polling."""
        if timeout is None:
            timeout = self.bzmpop_timeout

        queues = list(self.active_queues)
        if not queues:
            return

        keys = [self._queue_key(q) for q in queues]
        self._in_poll = self.client.connection

        # BZMPOP timeout numkeys key [key ...] MIN [COUNT count]
        command_args = ['BZMPOP', timeout or 0, len(keys)] + keys + ['MIN', 'COUNT', 1]

        if self.global_keyprefix:
            command_args = self.client._prefix_args(command_args)

        self.client.connection.send_command(*command_args)

    def _bzmpop_read(self, **options):
        """Read result from BZMPOP command."""
        try:
            try:
                result = self.client.parse_response(
                    self.client.connection,
                    'BZMPOP',
                    **options
                )
            except self.connection_errors:
                self.client.connection.disconnect()
                raise

            if result:
                key, members = result
                if members:
                    delivery_tag, score = members[0]
                    delivery_tag = bytes_to_str(delivery_tag)
                    key = bytes_to_str(key)

                    # Extract queue name from key
                    queue = key
                    if key.startswith(QUEUE_KEY_PREFIX):
                        queue = key[len(QUEUE_KEY_PREFIX):]

                    # Get message payload from hash
                    msg_key = self._message_key(delivery_tag)
                    with self.conn_or_acquire() as client:
                        payload_raw = client.hget(msg_key, 'payload')

                    if payload_raw:
                        message = loads(bytes_to_str(payload_raw))
                        self.connection._deliver(message, queue)
                        return True
            raise Empty()
        finally:
            self._in_poll = None

    def _size(self, queue):
        """Return number of messages in queue."""
        with self.conn_or_acquire() as client:
            return client.zcard(self._queue_key(queue))

    def _purge(self, queue):
        """Remove all messages from queue."""
        with self.conn_or_acquire() as client:
            queue_key = self._queue_key(queue)
            size = client.zcard(queue_key)
            client.delete(queue_key)
            return size

    # Fanout operations using Streams

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Deliver fanout message via Redis Stream."""
        with self.conn_or_acquire() as client:
            stream_key = self._stream_key(exchange)
            client.xadd(
                stream_key,
                {'message': dumps(message), 'routing_key': routing_key or ''},
                maxlen=self.stream_maxlen,
                approximate=True,
            )

    def _xread_start(self, timeout=None):
        """Start XREAD command for stream polling."""
        if timeout is None:
            timeout = self.bzmpop_timeout

        streams = {}
        for queue in self.active_fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            stream_key = self._stream_key(exchange)
            # Start from last known offset or '$' for new messages only
            streams[stream_key] = self._stream_offsets.get(stream_key, '$')

        if not streams:
            return

        self._in_stream_read = self.stream_client.connection

        # Build XREAD command
        keys = list(streams.keys())
        ids = [streams[k] for k in keys]

        command_args = ['XREAD', 'BLOCK', int((timeout or 0) * 1000), 'STREAMS'] + keys + ids

        if self.global_keyprefix:
            command_args = self.stream_client._prefix_args(command_args)

        self.stream_client.connection.send_command(*command_args)

    def _xread_receive(self, **options):
        """Read result from XREAD command."""
        try:
            try:
                result = self.stream_client.parse_response(
                    self.stream_client.connection,
                    'XREAD',
                    **options
                )
            except self.connection_errors:
                self._in_stream_read = None
                raise

            if result:
                for stream_data in result:
                    stream_key, messages = stream_data
                    stream_key = bytes_to_str(stream_key)

                    for msg_id, fields in messages:
                        msg_id = bytes_to_str(msg_id)
                        # Update offset
                        self._stream_offsets[stream_key] = msg_id

                        # Parse message
                        message_data = bytes_to_str(fields.get(b'message', b'{}'))
                        message = loads(message_data)

                        # Find queue for this stream
                        exchange = stream_key
                        if stream_key.startswith(self.keyprefix_fanout):
                            exchange = stream_key[len(self.keyprefix_fanout):]

                        queue = self._fanout_to_queue.get(exchange)
                        if queue:
                            self.connection._deliver(message, queue)

                return True
            raise Empty()
        finally:
            self._in_stream_read = None

    def _poll_error(self, type, **options):
        if type == 'XREAD':
            self.stream_client.parse_response(
                self.stream_client.connection, type
            )
        else:
            self.client.parse_response(self.client.connection, type)

    # Queue/Exchange binding

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        return super().basic_consume(queue, *args, **kwargs)

    def basic_cancel(self, consumer_tag):
        # Handle race condition during protected read
        connection = self.connection
        if connection:
            if connection.cycle._in_protected_read:
                from vine import promise
                return connection.cycle.after_read.add(
                    promise(self._basic_cancel, (consumer_tag,)),
                )
            return self._basic_cancel(consumer_tag)

    def _basic_cancel(self, consumer_tag):
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return
        try:
            self.active_fanout_queues.remove(queue)
        except KeyError:
            pass
        try:
            exchange, _ = self._fanout_queues[queue]
            self._fanout_to_queue.pop(exchange)
        except KeyError:
            pass
        return super().basic_cancel(consumer_tag)

    def _new_queue(self, queue, auto_delete=False, **kwargs):
        if auto_delete:
            self.auto_delete_queues.add(queue)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            self._fanout_queues[queue] = (
                exchange, routing_key.replace('#', '*'),
            )
        with self.conn_or_acquire() as client:
            client.sadd(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or '', pattern or '', queue or ''])
            )

    def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
        self.auto_delete_queues.discard(queue)
        with self.conn_or_acquire(client=kwargs.get('client')) as client:
            client.srem(
                self.keyprefix_queue % (exchange,),
                self.sep.join([routing_key or '', pattern or '', queue or ''])
            )
            client.delete(self._queue_key(queue))

    def _has_queue(self, queue, **kwargs):
        with self.conn_or_acquire() as client:
            return client.exists(self._queue_key(queue))

    def get_table(self, exchange):
        key = self.keyprefix_queue % exchange
        with self.conn_or_acquire() as client:
            values = client.smembers(key)
            if not values:
                return []
            return [tuple(bytes_to_str(val).split(self.sep)) for val in values]

    # Background processing for delayed delivery and visibility timeout

    def _requeue_loop(self):
        """Background loop to process delayed and timed-out messages."""
        while not Channel._requeue_stop_event.is_set():
            try:
                count = self._enqueue_due_messages()
                if count:
                    debug('Enqueued %d due messages', count)
            except Exception as exc:
                warning('Error in requeue loop: %r', exc, exc_info=True)

            # Wait for next check interval or stop event
            Channel._requeue_stop_event.wait(self.requeue_check_interval)

    def _enqueue_due_messages(self) -> int:
        """Run Lua script to enqueue messages whose time has come."""
        if not self._enqueue_script_sha:
            return 0

        with self.conn_or_acquire() as client:
            try:
                count = client.evalsha(
                    self._enqueue_script_sha,
                    1,  # numkeys
                    self._messages_index_key,  # KEYS[1]
                    _current_time_ms(),  # ARGV[1]: current time
                    self.requeue_batch_limit,  # ARGV[2]: batch limit
                    self.global_keyprefix,  # ARGV[3]: key prefix
                )
                return count or 0
            except redis.exceptions.NoScriptError:
                # Script was flushed, re-register
                self._register_scripts()
                return 0

    # Connection management

    def close(self):
        self._closing = True

        # Stop background thread when last channel closes
        if not Channel._n_channels.dec():
            Channel._requeue_stop_event.set()
            if Channel._requeue_thread:
                Channel._requeue_thread.join(timeout=5)
                Channel._requeue_thread = None
            debug('Stopped native delayed delivery background thread')

        if self._in_poll:
            try:
                self._bzmpop_read()
            except Empty:
                pass

        if not self.closed:
            self.connection.cycle.discard(self)

            client = self.__dict__.get('client')
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)

            self._disconnect_pools()
            self._close_clients()

        super().close()

    def _close_clients(self):
        for attr in 'client', 'stream_client':
            try:
                client = self.__dict__[attr]
                connection, client.connection = client.connection, None
                connection.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

    def _prepare_virtual_host(self, vhost):
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = DEFAULT_DB
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError(
                    f'Database is int between 0 and limit - 1, not {vhost}'
                )
        return vhost

    def _filter_tcp_connparams(self, socket_keepalive=None,
                               socket_keepalive_options=None, **params):
        return params

    def _process_credential_provider(self, credential_provider, connparams):
        if credential_provider:
            if isinstance(credential_provider, str):
                credential_provider_cls = symbol_by_name(credential_provider)
                credential_provider = credential_provider_cls()

            if CredentialProvider and not isinstance(credential_provider, CredentialProvider):
                raise ValueError(
                    "Credential provider is not an instance of redis.CredentialProvider"
                )

            connparams['credential_provider'] = credential_provider
            connparams.pop("username", None)
            connparams.pop("password", None)

    def _connparams(self, asynchronous=False):
        conninfo = self.connection.client
        connparams = {
            'host': conninfo.hostname or '127.0.0.1',
            'port': conninfo.port or self.connection.default_port,
            'virtual_host': conninfo.virtual_host,
            'username': conninfo.userid,
            'password': conninfo.password,
            'max_connections': self.max_connections,
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
            'socket_keepalive_options': self.socket_keepalive_options,
            'health_check_interval': self.health_check_interval,
            'retry_on_timeout': self.retry_on_timeout,
            'client_name': self.client_name,
        }

        self._process_credential_provider(
            conninfo.credential_provider, connparams
        )

        conn_class = self.connection_class

        if hasattr(conn_class, '__init__'):
            classes = [conn_class]
            if hasattr(conn_class, '__bases__'):
                classes += list(conn_class.__bases__)
            for klass in classes:
                if accepts_argument(klass.__init__, 'health_check_interval'):
                    break
            else:
                connparams.pop('health_check_interval')

        if conninfo.ssl:
            try:
                connparams.update(conninfo.ssl)
                connparams['connection_class'] = self.connection_class_ssl
            except TypeError:
                pass

        host = connparams['host']
        if '://' in host:
            scheme, _, _, username, password, path, query = _parse_url(host)
            if scheme == 'socket':
                connparams = self._filter_tcp_connparams(**connparams)
                connparams.update({
                    'connection_class': redis.UnixDomainSocketConnection,
                    'path': '/' + path}, **query)
                connparams.pop('socket_connect_timeout', None)
                connparams.pop('socket_keepalive', None)
                connparams.pop('socket_keepalive_options', None)
            connparams['username'] = username
            connparams['password'] = password

            credential_provider = query.pop("credential_provider", None)
            self._process_credential_provider(credential_provider, connparams)

            connparams.pop('host', None)
            connparams.pop('port', None)

        connparams['db'] = self._prepare_virtual_host(
            connparams.pop('virtual_host', None)
        )

        channel = self
        connection_cls = (
            connparams.get('connection_class') or
            self.connection_class
        )

        if asynchronous:
            class Connection(connection_cls):
                def disconnect(self, *args):
                    super().disconnect(*args)
                    if channel._registered:
                        channel._on_connection_disconnect(self)
            connection_cls = Connection

        connparams['connection_class'] = connection_cls

        return connparams

    def _create_client(self, asynchronous=False):
        if asynchronous:
            return self.Client(connection_pool=self.async_pool)
        return self.Client(connection_pool=self.pool)

    def _get_pool(self, asynchronous=False):
        params = self._connparams(asynchronous=asynchronous)
        self.keyprefix_fanout = self.keyprefix_fanout.format(db=params['db'])
        return redis.ConnectionPool(**params)

    def _get_client(self):
        if redis.VERSION < (3, 2, 0):
            raise VersionMismatch(
                'Redis Plus transport requires redis-py versions 3.2.0 or later. '
                f'You have {redis.__version__}'
            )

        if self.global_keyprefix:
            return functools.partial(
                PrefixedStrictRedis,
                global_keyprefix=self.global_keyprefix,
            )

        return redis.Redis

    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self._create_client()

    @property
    def pool(self):
        if self._pool is None:
            self._pool = self._get_pool()
        return self._pool

    @property
    def async_pool(self):
        if self._async_pool is None:
            self._async_pool = self._get_pool(asynchronous=True)
        return self._async_pool

    @cached_property
    def client(self):
        """Client used to publish messages, BZMPOP etc."""
        return self._create_client(asynchronous=True)

    @cached_property
    def stream_client(self):
        """Client used for XREAD on streams."""
        return self._create_client(asynchronous=True)

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    @property
    def active_queues(self):
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues
                if queue not in self.active_fanout_queues}


class Transport(virtual.Transport):
    """Redis Plus Transport using sorted sets and streams."""

    Channel = Channel

    polling_interval = None  # disable sleep between unsuccessful polls.
    brpop_timeout = 1  # Used as BZMPOP timeout
    default_port = DEFAULT_PORT
    driver_type = 'redis'
    driver_name = 'redis-plus'

    #: This transport supports native delayed delivery
    supports_native_delayed_delivery = True

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(['direct', 'topic', 'fanout'])
    )

    if redis:
        connection_errors, channel_errors = get_redis_error_classes()

    def __init__(self, *args, **kwargs):
        if redis is None:
            raise ImportError('Missing redis library (pip install redis)')
        super().__init__(*args, **kwargs)

        # All channels share the same poller.
        self.cycle = MultiChannelPoller()

        if self.polling_interval is not None:
            self.brpop_timeout = self.polling_interval

    def driver_version(self):
        return redis.__version__

    def register_with_event_loop(self, connection, loop):
        cycle = self.cycle
        cycle.on_poll_init(loop.poller)
        cycle_poll_start = cycle.on_poll_start
        add_reader = loop.add_reader
        on_readable = self.on_readable

        def _on_disconnect(connection):
            if connection._sock:
                loop.remove(connection._sock)
            if cycle.fds:
                try:
                    loop.on_tick.remove(on_poll_start)
                except KeyError:
                    pass
        cycle._on_connection_disconnect = _on_disconnect

        def on_poll_start():
            cycle_poll_start()
            [add_reader(fd, on_readable, fd) for fd in cycle.fds]
        loop.on_tick.add(on_poll_start)
        loop.call_repeatedly(10, cycle.maybe_restore_messages)
        health_check_interval = connection.client.transport_options.get(
            'health_check_interval',
            DEFAULT_HEALTH_CHECK_INTERVAL
        )
        loop.call_repeatedly(
            health_check_interval,
            lambda: None  # Health check placeholder
        )

    def on_readable(self, fileno):
        """Handle AIO event for one of our file descriptors."""
        self.cycle.on_readable(fileno)
