"""Redis Streams transport module for Kombu.

Uses Redis Streams (XADD/XREADGROUP) instead of Lists (LPUSH/BRPOP) for
message queuing, providing native message tracking via PEL (Pending Entries
List) and consumer groups for fanout instead of PUB/SUB.

Connection strings are identical to the standard redis transport.

Streams-specific transport options:
* ``consumer_group_prefix``: Prefix for consumer groups (default: 'kombu')
* ``stream_maxlen``: Maximum stream length for trimming (default: 10000)

Requires Redis 6.2.0+
"""

from __future__ import annotations

import functools
import numbers
import socket
from bisect import bisect
from collections import namedtuple
from contextlib import contextmanager
from importlib.metadata import version
from queue import Empty

from packaging.version import Version
from vine import promise

from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.log import get_logger
from kombu.utils import symbol_by_name
from kombu.utils.compat import register_after_fork
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ, poll
from kombu.utils.functional import accepts_argument
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.scheduling import cycle_by_name
from kombu.utils.url import _parse_url

from . import virtual

try:
    import redis
    _REDIS_GET_CONNECTION_WITHOUT_ARGS = Version(version("redis")) >= Version("5.3.0")
except ImportError:  # pragma: no cover
    redis = None
    _REDIS_GET_CONNECTION_WITHOUT_ARGS = None

try:
    from redis import CredentialProvider, sentinel
except ImportError:  # pragma: no cover
    sentinel = None
    CredentialProvider = None


logger = get_logger('kombu.transport.redis_streams')
crit, warning = logger.critical, logger.warning

DEFAULT_PORT = 6379
DEFAULT_DB = 0

DEFAULT_HEALTH_CHECK_INTERVAL = 25

PRIORITY_STEPS = [0, 3, 6, 9]

error_classes_t = namedtuple('error_classes_t', (
    'connection_errors', 'channel_errors',
))


# This implementation is based on redis.py but uses Redis Streams instead of
# Lists, Sorted Sets, and PUB/SUB.
#
# Key differences from redis.py:
# - XADD/XREADGROUP instead of LPUSH/BRPOP for message queuing
# - Consumer groups instead of PUB/SUB for fanout exchanges
# - Native PEL (Pending Entries List) instead of manual unacked tracking
# - XCLAIM for message recovery instead of sorted set manipulation
#
# Consuming from multiple connections using epoll enables us to emulate
# channels with different service guarantees while efficiently handling
# I/O from multiple streams simultaneously without threads.


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


class MutexHeld(Exception):
    """Raised when another party holds the lock."""


@contextmanager
def Mutex(client, name, expire):
    """Acquire redis lock in non blocking way.

    Raise MutexHeld if not successful.
    """
    lock = client.lock(name, timeout=expire)
    lock_acquired = False
    try:
        lock_acquired = lock.acquire(blocking=False)
        if lock_acquired:
            yield
        else:
            raise MutexHeld()
    finally:
        if lock_acquired:
            try:
                lock.release()
            except redis.exceptions.LockNotOwnedError:
                # when lock is expired
                pass


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
        "HLEN",
        "HSET",
        "LLEN",
        "LPUSH",
        "PUBLISH",
        "RPUSH",
        "RPOP",
        "SADD",
        "SREM",
        "SET",
        "SMEMBERS",
        "ZADD",
        "ZREM",
        "ZREVRANGEBYSCORE",
    ]

    PREFIXED_COMPLEX_COMMANDS = {
        "DEL": {"args_start": 0, "args_end": None},
        "BRPOP": {"args_start": 0, "args_end": -1},
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
        if command_name == 'BRPOP' and ret:
            key, value = ret
            key = key[len(self.global_keyprefix):]
            return key, value
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

    def pubsub(self, **kwargs):
        return PrefixedRedisPubSub(
            self.connection_pool,
            global_keyprefix=self.global_keyprefix,
            **kwargs,
        )


class PrefixedRedisPipeline(GlobalKeyPrefixMixin, redis.client.Pipeline):
    """Custom Redis pipeline that takes global_keyprefix into consideration.

    As the ``PrefixedStrictRedis`` client uses the `global_keyprefix` to prefix
    the keys it uses, the pipeline called by the client must be able to prefix
    the keys as well.
    """

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.client.Pipeline.__init__(self, *args, **kwargs)


class PrefixedRedisPubSub(redis.client.PubSub):
    """Redis pubsub client that takes global_keyprefix into consideration."""

    PUBSUB_COMMANDS = (
        "SUBSCRIBE",
        "UNSUBSCRIBE",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
    )

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        super().__init__(*args, **kwargs)

    def _prefix_args(self, args):
        args = list(args)
        command = args.pop(0)

        if command in self.PUBSUB_COMMANDS:
            args = [
                self.global_keyprefix + str(arg)
                for arg in args
            ]

        return [command, *args]

    def parse_response(self, *args, **kwargs):
        """Parse a response from the Redis server.

        Method wraps ``PubSub.parse_response()`` to remove prefixes of keys
        returned by redis command.
        """
        ret = super().parse_response(*args, **kwargs)
        if ret is None:
            return ret

        # response formats
        # SUBSCRIBE and UNSUBSCRIBE
        #  -> [message type, channel, message]
        # PSUBSCRIBE and PUNSUBSCRIBE
        #  -> [message type, pattern, channel, message]
        message_type, *channels, message = ret
        return [
            message_type,
            *[channel[len(self.global_keyprefix):] for channel in channels],
            message,
        ]

    def execute_command(self, *args, **kwargs):
        return super().execute_command(*self._prefix_args(args), **kwargs)


class QoS(virtual.QoS):
    """Redis Streams QoS using PEL (Pending Entries List).

    Unlike the standard Redis transport, this doesn't need to maintain
    a separate sorted set for unacked messages - the PEL handles this natively.
    """

    restore_at_shutdown = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vrestore_count = 0

    def ack(self, delivery_tag):
        """Acknowledge message by XACK."""
        # delivery_tag format: "stream_name:message_id:group_name" or "stream_name:message_id"
        try:
            parts = delivery_tag.rsplit(':', 2)
            if len(parts) == 3:
                stream, message_id, group_name = parts
            else:
                stream, message_id = parts
                group_name = self.channel.consumer_group

            with self.channel.conn_or_acquire() as client:
                client.xack(stream, group_name, message_id)

            super().ack(delivery_tag)
        except (ValueError, AttributeError) as e:
            # Malformed delivery tag or missing attribute
            logger.warning(f'Failed to ack {delivery_tag}: {e}')

    def reject(self, delivery_tag, requeue=False):
        """Reject message, optionally requeue."""
        try:
            parts = delivery_tag.rsplit(':', 2)
            if len(parts) == 3:
                stream, message_id, group_name = parts
            else:
                stream, message_id = parts
                group_name = self.channel.consumer_group

            if requeue:
                # For requeue, we need to re-add the message
                # First get it from PEL, then XACK and re-add
                with self.channel.conn_or_acquire() as client:
                    # Get the message from PEL
                    pending = client.xpending_range(
                        name=stream,
                        groupname=group_name,
                        min=message_id,
                        max=message_id,
                        count=1
                    )

                    if pending:
                        # Read the actual message content
                        messages = client.xrange(stream, min=message_id, max=message_id, count=1)
                        if messages:
                            msg_id, fields = messages[0]
                            # ACK the old one
                            client.xack(stream, group_name, message_id)
                            # Re-add to stream
                            client.xadd(
                                name=stream,
                                fields=fields,
                                id='*',
                                maxlen=self.channel.stream_maxlen,
                                approximate=True
                            )
            else:
                # Just ACK (removes from PEL)
                with self.channel.conn_or_acquire() as client:
                    client.xack(stream, group_name, message_id)

            super().ack(delivery_tag)
        except (ValueError, AttributeError) as e:
            logger.warning(f'Failed to reject {delivery_tag}: {e}')

    def restore_visible(self, start=0, num=10, interval=10):
        """Reclaim messages idle past visibility timeout."""
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return

        idle_time = int(self.visibility_timeout * 1000)  # milliseconds

        with self.channel.conn_or_acquire() as client:
            for stream in self.channel._get_all_streams():
                try:
                    cursor = '0-0'
                    total_claimed = 0

                    while cursor != '0-0' and total_claimed < num:
                        result = client.xautoclaim(
                            name=stream,
                            groupname=self.channel.consumer_group,
                            consumername=self.channel.consumer_id,
                            min_idle_time=idle_time,
                            start=cursor,
                            count=min(num - total_claimed, 100)
                        )

                        cursor = result[0]
                        claimed_messages = result[1] if len(result) > 1 else []
                        total_claimed += len(claimed_messages)

                        if cursor == b'0-0' or cursor == '0-0':
                            break

                except self.channel.ResponseError as e:
                    logger.warning(f'Error restoring from {stream}: {e}')
                except Exception as e:
                    logger.warning(f'Unexpected error restoring from {stream}: {e}')

    @cached_property
    def visibility_timeout(self):
        return self.channel.visibility_timeout


class MultiChannelPoller:
    """Async I/O poller for Redis transport."""

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

    def _register_XREADGROUP(self, channel):
        """Enable XREADGROUP mode for channel."""
        ident = channel, channel.client, 'XREADGROUP'
        if not self._client_registered(channel, channel.client, 'XREADGROUP'):
            channel._in_poll = False
            self._register(*ident)
        if not channel._in_poll:  # send XREADGROUP
            channel._xreadgroup_start()

    def on_poll_start(self):
        for channel in self._channels:
            # Both regular and fanout queues use XREADGROUP now
            if channel.active_queues or channel.active_fanout_queues:
                if channel.qos.can_consume():
                    self._register_XREADGROUP(channel)

    def on_poll_init(self, poller):
        self.poller = poller
        for channel in self._channels:
            return channel.qos.restore_visible(
                num=10,
            )

    def maybe_restore_messages(self):
        for channel in self._channels:
            if channel.active_queues:
                # only need to do this once, as they are not local to channel.
                return channel.qos.restore_visible(
                    num=10,
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
                # Both regular and fanout queues use XREADGROUP now
                if channel.active_queues or channel.active_fanout_queues:
                    if channel.qos.can_consume():
                        self._register_XREADGROUP(channel)

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
    """Redis Streams Channel.

    Uses Redis Streams (XADD/XREADGROUP) instead of Lists (LPUSH/BRPOP).
    """

    QoS = QoS

    _client = None
    _closing = False
    supports_fanout = True
    keyprefix_queue = '_kombu.binding.%s'
    keyprefix_fanout = '/{db}.'
    sep = '\x06\x16'
    _in_poll = False
    _fanout_queues = {}

    # Streams-specific: no longer need manual unacked tracking
    # (PEL handles this)
    visibility_timeout = 3600   # 1 hour (for XCLAIM)
    priority_steps = PRIORITY_STEPS

    # New Streams-specific options
    consumer_group_prefix = 'kombu'
    stream_maxlen = 10000
    prefetch_count = 1
    socket_timeout = None
    socket_connect_timeout = None
    socket_keepalive = None
    socket_keepalive_options = None
    retry_on_timeout = None
    max_connections = 10
    health_check_interval = DEFAULT_HEALTH_CHECK_INTERVAL
    client_name = None
    #: Transport option to disable fanout keyprefix.
    #: Can also be string, in which case it changes the default
    #: prefix ('/{db}.') into to something else.  The prefix must
    #: include a leading slash and a trailing dot.
    #:
    #: Enabled by default since Kombu 4.x.
    #: Disable for backwards compatibility with Kombu 3.x.
    fanout_prefix = True

    #: If enabled the fanout exchange will support patterns in routing
    #: and binding keys (like a topic exchange but using PUB/SUB).
    #:
    #: Enabled by default since Kombu 4.x.
    #: Disable for backwards compatibility with Kombu 3.x.
    fanout_patterns = True

    #: The global key prefix will be prepended to all keys used
    #: by Kombu, which can be useful when a redis database is shared
    #: by different users. By default, no prefix is prepended.
    global_keyprefix = ''

    #: Order in which we consume from queues.
    #:
    #: Can be either string alias, or a cycle strategy class
    #:
    #: - ``round_robin``
    #:   (:class:`~kombu.utils.scheduling.round_robin_cycle`).
    #:
    #:    Make sure each queue has an equal opportunity to be consumed from.
    #:
    #: - ``sorted``
    #:   (:class:`~kombu.utils.scheduling.sorted_cycle`).
    #:
    #:    Consume from queues in alphabetical order.
    #:    If the first queue in the sorted list always contains messages,
    #:    then the rest of the queues will never be consumed from.
    #:
    #: - ``priority``
    #:   (:class:`~kombu.utils.scheduling.priority_cycle`).
    #:
    #:    Consume from queues in original order, so that if the first
    #:    queue always contains messages, the rest of the queues
    #:    in the list will never be consumed from.
    #:
    #: The default is to consume from queues in round robin.
    queue_order_strategy = 'round_robin'

    _async_pool = None
    _pool = None

    from_transport_options = (
        virtual.Channel.from_transport_options +
        ('sep',
         'visibility_timeout',
         'fanout_prefix',
         'fanout_patterns',
         'global_keyprefix',
         'socket_timeout',
         'socket_connect_timeout',
         'socket_keepalive',
         'socket_keepalive_options',
         'queue_order_strategy',
         'max_connections',
         'health_check_interval',
         'retry_on_timeout',
         'priority_steps',
         'client_name',
         'consumer_group_prefix',
         'stream_maxlen',
         'prefetch_count')  # <-- do not add comma here!
    )

    connection_class = redis.Connection if redis else None
    connection_class_ssl = redis.SSLConnection if redis else None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._registered = False
        self._queue_cycle = cycle_by_name(self.queue_order_strategy)()
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self.auto_delete_queues = set()
        self._fanout_to_queue = {}
        # Use XREADGROUP for all streams-based consumption (regular + fanout)
        self.handlers = {'XREADGROUP': self._xreadgroup_read}
        self.brpop_timeout = self.connection.brpop_timeout

        if self.fanout_prefix:
            if isinstance(self.fanout_prefix, str):
                self.keyprefix_fanout = self.fanout_prefix
        else:
            # previous versions did not set a fanout, so cannot enable
            # by default.
            self.keyprefix_fanout = ''

        # Evaluate connection.
        try:
            self.client.ping()
        except Exception:
            self._disconnect_pools()
            raise

        self.connection.cycle.add(self)  # add to channel poller.
        # and set to true after successfully added channel to the poll.
        self._registered = True

        # copy errors, in case channel closed but threads still
        # are still waiting for data.
        self.connection_errors = self.connection.connection_errors

        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_channel)

    def _after_fork(self):
        self._disconnect_pools()

    # Streams-specific helper methods

    def _stream_for_pri(self, queue, pri):
        """Get stream key for queue at priority level."""
        pri = self.priority(pri)
        if pri:
            return f"{queue}{self.sep}{pri}"
        return queue

    @cached_property
    def consumer_group(self):
        """Consumer group name for this connection."""
        db = self.connection.client.virtual_host or 0
        return f"{self.consumer_group_prefix}-{db}"

    @cached_property
    def consumer_id(self):
        """Unique consumer identifier."""
        import os
        import threading

        hostname = socket.gethostname()
        pid = os.getpid()
        thread_id = threading.get_ident()

        return f"{hostname}-{pid}-{thread_id}"

    def _ensure_consumer_group(self, stream, group=None):
        """Ensure consumer group exists for stream."""
        if group is None:
            group = self.consumer_group

        try:
            self.client.xgroup_create(
                name=stream,
                groupname=group,
                id='0',
                mkstream=True
            )
        except self.ResponseError as e:
            # Group already exists
            if 'BUSYGROUP' not in str(e):
                raise

    def _get_all_streams(self):
        """Get list of all active streams for this channel."""
        streams = []
        for queue in self.active_queues:
            for pri in self.priority_steps:
                streams.append(self._stream_for_pri(queue, pri))
        return streams

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
        # No _in_listen anymore (removed PUB/SUB support)
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _do_restore_message(self, payload, exchange, routing_key,
                            client=None, leftmost=False):
        """Restore message to stream (simplified - no pipe needed)."""
        import uuid

        try:
            try:
                payload['headers']['redelivered'] = True
                payload['properties']['delivery_info']['redelivered'] = True
            except KeyError:
                pass

            # Find destination queues
            for queue in self._lookup(exchange, routing_key):
                pri = self._get_message_priority(payload, reverse=False)
                stream_key = self._stream_for_pri(queue, pri)

                with self.conn_or_acquire(client=client) as c:
                    # Ensure consumer group exists
                    self._ensure_consumer_group(stream_key)

                    # Re-add to stream (note: leftmost param ignored for streams)
                    c.xadd(
                        name=stream_key,
                        fields={
                            'uuid': str(uuid.uuid4()),  # New UUID
                            'payload': dumps(payload),
                        },
                        id='*',
                        maxlen=self.stream_maxlen,
                        approximate=True
                    )
        except Exception:
            crit('Could not restore message: %r', payload, exc_info=True)

    def _restore(self, message, leftmost=False):
        """Restore message (simplified for streams)."""
        try:
            # Extract payload from message
            M = message._raw
            EX = message.delivery_info.get('exchange')
            RK = message.delivery_info.get('routing_key')
            self._do_restore_message(M, EX, RK, leftmost=leftmost)
        except Exception:
            crit('Could not restore message: %r', message, exc_info=True)

    def _restore_at_beginning(self, message):
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        ret = super().basic_consume(queue, *args, **kwargs)

        # Update fair cycle between queues.
        #
        # We cycle between queues fairly to make sure that
        # each queue is equally likely to be consumed from,
        # so that a very busy queue will not block others.
        #
        # This works by using Redis's `BRPOP` command and
        # by rotating the most recently used queue to the
        # and of the list.  See Kombu github issue #166 for
        # more discussion of this method.
        self._update_queue_cycle()
        return ret

    def basic_cancel(self, consumer_tag):
        # If we are busy reading messages we may experience
        # a race condition where a message is consumed after
        # canceling, so we must delay this operation until reading
        # is complete (Issue celery/celery#1773).
        connection = self.connection
        if connection:
            if connection.cycle._in_protected_read:
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
        # No need to unsubscribe - using Streams instead of PUB/SUB
        try:
            exchange, _ = self._fanout_queues[queue]
            self._fanout_to_queue.pop(exchange)
        except KeyError:
            pass
        ret = super().basic_cancel(consumer_tag)
        self._update_queue_cycle()
        return ret

    def _get_publish_topic(self, exchange, routing_key):
        if routing_key and self.fanout_patterns:
            return ''.join([self.keyprefix_fanout, exchange, '/', routing_key])
        return ''.join([self.keyprefix_fanout, exchange])

    def _xreadgroup_start(self, timeout=None):
        """Start XREADGROUP operation for async consumption."""
        if timeout is None:
            timeout = self.brpop_timeout

        queues = self._queue_cycle.consume(len(self.active_queues))

        # Build streams dict with priorities (high priority first)
        streams = {}
        queue_to_group = {}  # Track which group to use for each stream

        # Add regular queues
        if queues:
            for pri in reversed(self.priority_steps):  # Reverse for high-to-low priority
                for queue in queues:
                    stream_key = self._stream_for_pri(queue, pri)
                    # Ensure consumer group exists
                    self._ensure_consumer_group(stream_key)
                    streams[stream_key] = '>'  # '>' means new messages
                    queue_to_group[stream_key] = self.consumer_group

        # Add fanout streams
        for queue in self.active_fanout_queues:
            if queue in self._fanout_queues:
                exchange, routing_key = self._fanout_queues[queue]
                stream_key = self._fanout_stream_key(exchange, routing_key)
                group_name = self._fanout_consumer_group(queue)

                # Ensure consumer group exists
                self._ensure_consumer_group(stream_key, group_name)
                streams[stream_key] = '>'
                queue_to_group[stream_key] = group_name

        if not streams:
            return

        self._in_poll = self.client.connection
        # Store for _xreadgroup_read
        self._pending_streams = streams
        self._pending_timeout = timeout
        self._pending_queues = queues
        self._pending_queue_to_group = queue_to_group

    def _xreadgroup_read(self, **options):
        """Read messages from XREADGROUP operation."""
        try:
            try:
                # Group streams by consumer group
                groups = {}
                for stream_key, stream_id in self._pending_streams.items():
                    group_name = self._pending_queue_to_group[stream_key]
                    if group_name not in groups:
                        groups[group_name] = {}
                    groups[group_name][stream_key] = stream_id

                # Try each consumer group until we get a message
                for group_name, group_streams in groups.items():
                    messages = self.client.xreadgroup(
                        groupname=group_name,
                        consumername=self.consumer_id,
                        streams=group_streams,
                        count=1,  # Get one message at a time
                        block=int(self._pending_timeout * 1000) if self._pending_timeout else 0
                    )

                    if messages:
                        # Process first message
                        # messages format: [(stream_name, [(message_id, {fields})])]
                        for stream, message_list in messages:
                            if message_list:
                                message_id, fields = message_list[0]

                                stream_str = bytes_to_str(stream)
                                message_id_str = bytes_to_str(message_id)

                                # Check if this is a fanout stream
                                if stream_str.startswith(self.keyprefix_fanout):
                                    # Find queue for this fanout stream
                                    for queue, (exchange, routing_key) in self._fanout_queues.items():
                                        fanout_stream = self._fanout_stream_key(exchange, routing_key)
                                        if stream_str == fanout_stream:
                                            queue_name = queue
                                            break
                                    else:
                                        # Couldn't find queue, skip
                                        continue
                                else:
                                    # Regular queue: extract queue name (remove priority suffix if present)
                                    queue_name = stream_str.rsplit(self.sep, 1)[0]

                                # Parse payload
                                payload = loads(bytes_to_str(fields[b'payload']))

                                # Set delivery tag
                                delivery_tag = f"{stream_str}:{message_id_str}:{group_name}"
                                payload['properties']['delivery_tag'] = delivery_tag

                                # Rotate queue cycle if not fanout
                                if not stream_str.startswith(self.keyprefix_fanout):
                                    self._queue_cycle.rotate(queue_name)

                                # Deliver message
                                self.connection._deliver(payload, queue_name)
                                return True

            except self.connection_errors:
                # if there's a ConnectionError, disconnect so the next
                # iteration will reconnect automatically.
                self.client.connection.disconnect()
                raise

            raise Empty()
        finally:
            self._in_poll = None
            self._pending_streams = None
            self._pending_timeout = None
            self._pending_queues = None
            self._pending_queue_to_group = None

    def _poll_error(self, type, **options):
        # Only XREADGROUP type is used now (no LISTEN/BRPOP)
        self.client.parse_response(self.client.connection, type)

    def _get(self, queue):
        """Get single message from queue (synchronous operation)."""
        with self.conn_or_acquire() as client:
            # Try each priority level
            for pri in self.priority_steps:
                stream_key = self._stream_for_pri(queue, pri)

                # Ensure consumer group
                self._ensure_consumer_group(stream_key)

                # Try to read one message (non-blocking)
                messages = client.xreadgroup(
                    groupname=self.consumer_group,
                    consumername=self.consumer_id,
                    streams={stream_key: '>'},
                    count=1,
                    block=0  # Non-blocking
                )

                if messages:
                    # messages format: [(stream_name, [(message_id, {fields})])]
                    for stream, message_list in messages:
                        for message_id, fields in message_list:
                            # Parse payload
                            payload = loads(bytes_to_str(fields[b'payload']))

                            # Create delivery tag: stream:message_id
                            stream_str = bytes_to_str(stream)
                            message_id_str = bytes_to_str(message_id)
                            delivery_tag = f"{stream_str}:{message_id_str}"

                            payload['properties']['delivery_tag'] = delivery_tag

                            return payload

            raise Empty()

    def _size(self, queue):
        """Get approximate queue size across all priorities."""
        with self.conn_or_acquire() as client:
            total = 0
            for pri in self.priority_steps:
                stream_key = self._stream_for_pri(queue, pri)
                try:
                    info = client.xinfo_stream(stream_key)
                    # Get stream length (approximate - includes acked messages)
                    total += info.get('length', 0)
                except self.ResponseError:
                    # Stream doesn't exist yet
                    pass
            return total

    def _q_for_pri(self, queue, pri):
        pri = self.priority(pri)
        if pri:
            return f"{queue}{self.sep}{pri}"
        return queue

    def priority(self, n):
        steps = self.priority_steps
        return steps[bisect(steps, n) - 1]

    def _put(self, queue, message, **kwargs):
        """Deliver message to stream."""
        import uuid

        pri = self._get_message_priority(message, reverse=False)
        stream_key = self._stream_for_pri(queue, pri)

        # Generate UUID for message tracking/deduplication
        message_uuid = str(uuid.uuid4())

        with self.conn_or_acquire() as client:
            # Ensure consumer group exists
            self._ensure_consumer_group(stream_key)

            # Add to stream
            client.xadd(
                name=stream_key,
                fields={
                    'uuid': message_uuid,
                    'payload': dumps(message),
                },
                id='*',  # Let Redis generate time-ordered ID
                maxlen=self.stream_maxlen,
                approximate=True  # More efficient trimming
            )

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Deliver fanout message to stream."""
        import uuid

        # Fanout uses one stream per exchange
        stream_key = self._fanout_stream_key(exchange, routing_key)

        # Generate UUID for message tracking
        message_uuid = str(uuid.uuid4())

        with self.conn_or_acquire() as client:
            # Add to fanout stream
            client.xadd(
                name=stream_key,
                fields={
                    'uuid': message_uuid,
                    'payload': dumps(message),
                },
                id='*',
                maxlen=self.stream_maxlen,
                approximate=True
            )

    def _fanout_stream_key(self, exchange, routing_key=''):
        """Get stream key for fanout exchange."""
        if routing_key and self.fanout_patterns:
            return f"{self.keyprefix_fanout}{exchange}/{routing_key}"
        return f"{self.keyprefix_fanout}{exchange}"

    def _fanout_consumer_group(self, queue):
        """Get consumer group name for fanout queue."""
        # Each queue gets its own consumer group for broadcast
        return f"{self.consumer_group_prefix}-fanout-{queue}"

    def _new_queue(self, queue, auto_delete=False, **kwargs):
        if auto_delete:
            self.auto_delete_queues.add(queue)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            # Mark exchange as fanout.
            self._fanout_queues[queue] = (
                exchange, routing_key.replace('#', '*'),
            )

            # Create consumer group for this queue
            stream_key = self._fanout_stream_key(exchange, routing_key)
            group_name = self._fanout_consumer_group(queue)
            self._ensure_consumer_group(stream_key, group_name)

        with self.conn_or_acquire() as client:
            client.sadd(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))

    def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
        """Delete queue and its streams."""
        self.auto_delete_queues.discard(queue)
        with self.conn_or_acquire(client=kwargs.get('client')) as client:
            # Remove from binding table
            client.srem(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))

            # Delete all priority streams
            for pri in self.priority_steps:
                stream_key = self._stream_for_pri(queue, pri)
                try:
                    client.delete(stream_key)
                except self.ResponseError:
                    pass

    def _has_queue(self, queue, **kwargs):
        """Check if any priority stream exists for queue."""
        with self.conn_or_acquire() as client:
            for pri in self.priority_steps:
                stream_key = self._stream_for_pri(queue, pri)
                if client.exists(stream_key):
                    return True
            return False

    def get_table(self, exchange):
        key = self.keyprefix_queue % exchange
        with self.conn_or_acquire() as client:
            values = client.smembers(key)
            if not values:
                # table does not exists since all queues bound to the exchange
                # were deleted. We need just return empty list.
                return []
            return [tuple(bytes_to_str(val).split(self.sep)) for val in values]

    def _purge(self, queue):
        """Purge all messages from queue by deleting and recreating streams."""
        with self.conn_or_acquire() as client:
            total = 0
            for pri in self.priority_steps:
                stream_key = self._stream_for_pri(queue, pri)
                try:
                    # Get size before deletion
                    info = client.xinfo_stream(stream_key)
                    count = info.get('length', 0)

                    # Delete stream
                    client.delete(stream_key)

                    # Recreate consumer group
                    self._ensure_consumer_group(stream_key)

                    total += count
                except self.ResponseError:
                    # Stream doesn't exist
                    pass
            return total

    def close(self):
        self._closing = True
        if self._in_poll:
            try:
                self._xreadgroup_read()
            except Empty:
                pass
        if not self.closed:
            # remove from channel poller.
            self.connection.cycle.discard(self)

            # delete fanout bindings
            client = self.__dict__.get('client')  # only if property cached
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        super().close()

    def _close_clients(self):
        # Close connections
        try:
            client = self.__dict__['client']
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
                    'Database is int between 0 and limit - 1, not {}'.format(
                        vhost,
                    ))
        return vhost

    def _filter_tcp_connparams(self, socket_keepalive=None,
                               socket_keepalive_options=None, **params):
        return params

    def _process_credential_provider(self, credential_provider, connparams):
        if credential_provider:
            if isinstance(credential_provider, str):
                credential_provider_cls = symbol_by_name(credential_provider)
                credential_provider = credential_provider_cls()

            if not isinstance(credential_provider, CredentialProvider):
                raise ValueError(
                    "Credential provider is not an instance of a redis.CredentialProvider or a subclass"
                )

            connparams['credential_provider'] = credential_provider
            # drop username and password if credential provider is configured
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

        self._process_credential_provider(conninfo.credential_provider, connparams)

        conn_class = self.connection_class

        # If the connection class does not support the `health_check_interval`
        # argument then remove it.
        if hasattr(conn_class, '__init__'):
            # check health_check_interval for the class and bases
            # classes
            classes = [conn_class]
            if hasattr(conn_class, '__bases__'):
                classes += list(conn_class.__bases__)
            for klass in classes:
                if accepts_argument(klass.__init__, 'health_check_interval'):
                    break
            else:  # no break
                connparams.pop('health_check_interval')

        if conninfo.ssl:
            # Connection(ssl={}) must be a dict containing the keys:
            # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
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

            # credential provider as query string
            credential_provider = query.pop("credential_provider", None)
            self._process_credential_provider(credential_provider, connparams)

            connparams.pop('host', None)
            connparams.pop('port', None)
        connparams['db'] = self._prepare_virtual_host(
            connparams.pop('virtual_host', None))

        channel = self
        connection_cls = (
            connparams.get('connection_class') or
            self.connection_class
        )

        if asynchronous:
            class Connection(connection_cls):
                def disconnect(self, *args):
                    super().disconnect(*args)
                    # We remove the connection from the poller
                    # only if it has been added properly.
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
                'Redis transport requires redis-py versions 3.2.0 or later. '
                'You have {0.__version__}'.format(redis))

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
        """Client used to publish messages, BRPOP etc."""
        return self._create_client(asynchronous=True)

    def _update_queue_cycle(self):
        self._queue_cycle.update(self.active_queues)

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    @property
    def active_queues(self):
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues
                if queue not in self.active_fanout_queues}


class Transport(virtual.Transport):
    """Redis Transport."""

    Channel = Channel

    polling_interval = None  # disable sleep between unsuccessful polls.
    brpop_timeout = 1
    default_port = DEFAULT_PORT
    driver_type = 'redis'
    driver_name = 'redis'

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
        # Use polling_interval to set brpop_timeout if provided, but do not modify polling_interval itself.
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

            # must have started polling or this will break reconnection
            if cycle.fds:
                # stop polling in the event loop
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
        # No subclient health check needed (removed PUB/SUB support)

    def on_readable(self, fileno):
        """Handle AIO event for one of our file descriptors."""
        self.cycle.on_readable(fileno)


if sentinel:
    class SentinelManagedSSLConnection(
            sentinel.SentinelManagedConnection,
            redis.SSLConnection):
        """Connect to a Redis server using Sentinel + TLS.

        Use Sentinel to identify which Redis server is the current master
        to connect to and when connecting to the Master server, use an
        SSL Connection.
        """

        pass


class SentinelChannel(Channel):
    """Channel with explicit Redis Sentinel knowledge.

    Broker url is supposed to look like:

    .. code-block::

        sentinel://0.0.0.0:26379;sentinel://0.0.0.0:26380/...

    where each sentinel is separated by a `;`.

    Other arguments for the sentinel should come from the transport options
    (see `transport_options` of :class:`~kombu.connection.Connection`).

    You must provide at least one option in Transport options:
     * `master_name` - name of the redis group to poll

    Example:
    -------
    .. code-block:: python

        >>> import kombu
        >>> c = kombu.Connection(
             'sentinel://sentinel1:26379;sentinel://sentinel2:26379',
             transport_options={'master_name': 'mymaster'}
        )
        >>> c.connect()
    """

    from_transport_options = Channel.from_transport_options + (
        'master_name',
        'min_other_sentinels',
        'sentinel_kwargs')

    connection_class = sentinel.SentinelManagedConnection if sentinel else None
    connection_class_ssl = SentinelManagedSSLConnection if sentinel else None

    def _sentinel_managed_pool(self, asynchronous=False):
        connparams = self._connparams(asynchronous)

        additional_params = connparams.copy()

        additional_params.pop('host', None)
        additional_params.pop('port', None)

        sentinels = []
        for url in self.connection.client.alt:
            url = _parse_url(url)
            if url.scheme == 'sentinel':
                port = url.port or self.connection.default_port
                sentinels.append((url.hostname, port))

        # Fallback for when only one sentinel is provided.
        if not sentinels:
            sentinels.append((connparams['host'], connparams['port']))

        sentinel_inst = sentinel.Sentinel(
            sentinels,
            min_other_sentinels=getattr(self, 'min_other_sentinels', 0),
            sentinel_kwargs=getattr(self, 'sentinel_kwargs', None),
            **additional_params)

        master_name = getattr(self, 'master_name', None)

        if master_name is None:
            raise ValueError(
                "'master_name' transport option must be specified."
            )

        master_kwargs = {
            k: additional_params[k]
            for k in ('username', 'password') if k in additional_params
        }

        return sentinel_inst.master_for(
            master_name,
            redis.Redis,
            **master_kwargs,
        ).connection_pool

    def _get_pool(self, asynchronous=False):
        params = self._connparams(asynchronous=asynchronous)
        self.keyprefix_fanout = self.keyprefix_fanout.format(db=params['db'])
        return self._sentinel_managed_pool(asynchronous)


class SentinelTransport(Transport):
    """Redis Sentinel Transport."""

    default_port = 26379
    Channel = SentinelChannel
