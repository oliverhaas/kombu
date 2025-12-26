# Redis Streams Implementation Plan for Kombu

## Overview
This document outlines the step-by-step plan to implement a Redis Streams-based transport for Kombu, replacing the current BRPOP/lists implementation with XREADGROUP/streams.

## Goals
1. Replace Redis Lists (`LPUSH`/`BRPOP`) with Redis Streams (`XADD`/`XREADGROUP`)
2. Replace manual unacked tracking (sorted set) with built-in PEL (Pending Entries List)
3. Maintain 100% API compatibility with existing redis transport
4. Support all existing features: priorities, fanout, multi-queue consumption
5. Improve reliability and observability

## API Contract (What We Must Maintain)

### Public API (Called by Kombu users and core)
These are inherited from `virtual.Channel` and `base.StdChannel` - **we don't implement them directly**, the base class calls our internal methods:
- `exchange_declare(exchange, type, durable, ...)`
- `queue_declare(queue, passive, ...)`
- `queue_bind(queue, exchange, routing_key, ...)`
- `basic_publish(message, exchange, routing_key, ...)`
- `basic_consume(queue, callback, ...)`
- `basic_cancel(consumer_tag)`
- `basic_ack(delivery_tag)`
- `basic_reject(delivery_tag, requeue)`
- `queue_purge(queue)`
- `close()`

### Internal Implementation (Called by virtual.Channel base class)
These are the **private** methods we implement - the contract with `virtual.Channel`:
- `_put(queue, message, **kwargs)` - Store message in queue
- `_get(queue)` - Retrieve single message (sync)
- `_purge(queue)` - Clear all messages
- `_size(queue)` - Get message count
- `_delete(queue, exchange, routing_key, pattern)` - Delete queue
- `_has_queue(queue, **kwargs)` - Check if queue exists
- `_new_queue(queue, **kwargs)` - Initialize queue
- `_queue_bind(exchange, routing_key, pattern, queue)` - Bind queue to exchange

### Redis-Specific Internal Methods
- `_put_fanout(exchange, message, routing_key)` - Fanout/broadcast publish
- `_brpop_start()` / `_brpop_read()` - Async polling (will become `_xreadgroup_*`)
- `_restore(message)` - Restore unacked messages
- `_do_restore_message()` - Restore message to queue

### QoS Methods
- `append(message, delivery_tag)` - Track unacked (will be removed - PEL handles this)
- `ack(delivery_tag)` - Acknowledge message
- `reject(delivery_tag, requeue=False)` - Reject/requeue message
- `restore_visible()` - Restore timed-out messages
- `restore_by_tag(tag)` - Restore specific message

**Note**: Tests may call internal (`_*`) methods directly to test implementation details, but user code only calls the public API.

## Architecture Comparison

### Current (Lists + BRPOP)
```
Producer:
  LPUSH queue:priority:<pri> <json_message>
  ZADD unacked_index <timestamp> <delivery_tag>  (on consume)
  HSET unacked <delivery_tag> <message_data>     (on consume)

Consumer:
  BRPOP queue:priority:0 queue:priority:3 ... <timeout>
  -> Manual unacked tracking in sorted set
  -> Manual visibility timeout checking

Acknowledge:
  ZREM unacked_index <delivery_tag>
  HDEL unacked <delivery_tag>
```

### New (Streams + XREADGROUP)
```
Producer:
  XADD queue:priority:<pri> * uuid <uuid> payload <json_message>
  -> No manual tracking needed

Consumer:
  XREADGROUP GROUP <group> <consumer> COUNT <n> BLOCK <timeout>
    STREAMS queue:priority:0 queue:priority:3 ... > > >
  -> Automatic PEL tracking by Redis
  -> Messages reserved for consumer

Acknowledge:
  XACK queue:priority:<pri> <group> <message_id>

Recovery:
  XPENDING queue:priority:<pri> <group>  (check stuck messages)
  XCLAIM queue:priority:<pri> <group> <consumer> <min_idle> <msg_id>
```

## Implementation Phases

### Phase 1: Setup and Scaffolding
**File**: `kombu/transport/redis_streams.py`

- [x] Copy redis.py to redis_streams.py
- [ ] Update module docstring
- [ ] Rename classes: `StreamsChannel`, `StreamsTransport`, `StreamsQoS`
- [ ] Add new transport options:
  - `consumer_group_prefix` - Prefix for consumer groups (default: 'kombu')
  - `consumer_id` - Consumer identifier (default: `<hostname>-<pid>-<thread_id>`)
  - `stream_maxlen` - Max stream length (default: 10000)
  - `autoclaim_interval` - How often to claim stuck messages (default: 60s)

### Phase 2: Core Streams Foundation
**Focus**: Replace list operations with stream operations

**Remove**:
- `_brpop_start()` / `_brpop_read()` methods
- `unacked_key`, `unacked_index_key`, `unacked_mutex_key` properties (use PEL instead)

**Add**:
- `_stream_for_pri(queue, pri)` - Get stream name for queue+priority
  ```python
  def _stream_for_pri(self, queue, pri):
      """Get stream key for queue at priority level."""
      pri = self.priority(pri)
      if pri:
          return f"{queue}{self.sep}{pri}"
      return queue
  ```

- `_ensure_consumer_group(stream)` - Create consumer group if needed
  ```python
  def _ensure_consumer_group(self, stream):
      """Ensure consumer group exists for stream."""
      try:
          self.client.xgroup_create(
              name=stream,
              groupname=self.consumer_group,
              id='0',
              mkstream=True
          )
      except self.ResponseError as e:
          if 'BUSYGROUP' not in str(e):
              raise
  ```

### Phase 3: Implement StreamsQoS
**File**: `kombu/transport/redis_streams.py` (class StreamsQoS)

**Changes from current QoS**:
- Remove `append()` - not needed (PEL handles this)
- Remove `_remove_from_indices()` - not needed
- Simplify `ack()` - just call XACK
- Simplify `reject()` - XACK or re-add to stream
- Rewrite `restore_visible()` - use XPENDING + XCLAIM
- Remove `restore_by_tag()` - use stream_id instead

```python
class StreamsQoS(virtual.QoS):
    """QoS for Redis Streams using PEL."""

    restore_at_shutdown = True

    def ack(self, delivery_tag):
        """Acknowledge message by stream ID."""
        # delivery_tag format: "stream_name:message_id"
        stream, message_id = delivery_tag.rsplit(':', 1)

        with self.channel.conn_or_acquire() as client:
            client.xack(stream, self.channel.consumer_group, message_id)

        super().ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        """Reject message, optionally requeue."""
        if requeue:
            # Get message from PEL and re-add to stream
            self._requeue_message(delivery_tag)
        else:
            # Just ACK (removes from PEL, but stays in stream)
            stream, message_id = delivery_tag.rsplit(':', 1)
            with self.channel.conn_or_acquire() as client:
                client.xack(stream, self.channel.consumer_group, message_id)

        super().ack(delivery_tag)

    def restore_visible(self, start=0, num=10, interval=10):
        """Reclaim messages idle past visibility timeout."""
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return

        idle_time = int(self.visibility_timeout * 1000)  # milliseconds

        with self.channel.conn_or_acquire() as client:
            # Check all active streams for stuck messages
            for stream in self.channel._get_all_streams():
                try:
                    # Get pending messages
                    pending = client.xpending_range(
                        name=stream,
                        groupname=self.channel.consumer_group,
                        min='-',
                        max='+',
                        count=num
                    )

                    for msg in pending or []:
                        if msg['time_since_delivered'] > idle_time:
                            # Claim stuck message
                            client.xclaim(
                                name=stream,
                                groupname=self.channel.consumer_group,
                                consumername=self.channel.consumer_id,
                                min_idle_time=idle_time,
                                message_ids=[msg['message_id']]
                            )
                except Exception as e:
                    logger.warning(f'Error restoring from {stream}: {e}')
```

### Phase 4: Implement Core Message Operations

**_put() - Publish to Stream**:
```python
def _put(self, queue, message, **kwargs):
    """Deliver message to stream."""
    import uuid
    from uuid_extensions import uuid7  # or fallback to uuid4

    pri = self._get_message_priority(message, reverse=False)
    stream_key = self._stream_for_pri(queue, pri)

    # Generate UUID for message tracking
    message_uuid = str(uuid7())  # Time-ordered UUID

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
```

**_get() - Synchronous Get**:
```python
def _get(self, queue):
    """Get single message from queue (for synchronous operations)."""
    with self.conn_or_acquire() as client:
        for pri in self.priority_steps:
            stream_key = self._stream_for_pri(queue, pri)

            # Ensure consumer group
            self._ensure_consumer_group(stream_key)

            # Try to read one message
            messages = client.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.consumer_id,
                streams={stream_key: '>'},
                count=1,
                block=0  # Non-blocking
            )

            if messages:
                for stream, message_list in messages:
                    for message_id, fields in message_list:
                        payload = loads(bytes_to_str(fields[b'payload']))
                        # Create delivery tag: stream:message_id
                        payload['properties']['delivery_tag'] = (
                            f"{bytes_to_str(stream)}:{bytes_to_str(message_id)}"
                        )
                        return payload

    raise Empty()
```

**_xreadgroup_start() - Async Consumption** (replaces _brpop_start):
```python
def _xreadgroup_start(self, timeout=None):
    """Start XREADGROUP operation for async consumption."""
    if timeout is None:
        timeout = self.brpop_timeout

    # Build streams dict with priorities
    streams = {}
    queues = self._queue_cycle.consume(len(self.active_queues))

    if not queues:
        return

    # Add streams in priority order (high priority first)
    for pri in reversed(self.priority_steps):  # Reverse for high-to-low
        for queue in queues:
            stream_key = self._stream_for_pri(queue, pri)
            self._ensure_consumer_group(stream_key)
            streams[stream_key] = '>'  # '>' means new messages

    # Send command
    self._in_poll = self.client.connection

    # Build XREADGROUP command
    # Note: Unlike BRPOP, we use the redis-py client method
    # Store command for later execution in _xreadgroup_read
    self._pending_streams = streams
    self._pending_timeout = timeout
```

**_xreadgroup_read() - Read Results** (replaces _brpop_read):
```python
def _xreadgroup_read(self, **options):
    """Read messages from XREADGROUP operation."""
    try:
        messages = self.client.xreadgroup(
            groupname=self.consumer_group,
            consumername=self.consumer_id,
            streams=self._pending_streams,
            count=self.prefetch_count or 1,
            block=int(self._pending_timeout * 1000) if self._pending_timeout else 0
        )

        if messages:
            # Process first message
            for stream, message_list in messages:
                for message_id, fields in message_list:
                    stream_str = bytes_to_str(stream)
                    message_id_str = bytes_to_str(message_id)

                    # Extract queue name (remove priority suffix)
                    queue = stream_str.rsplit(self.sep, 1)[0]

                    # Parse payload
                    payload = loads(bytes_to_str(fields[b'payload']))

                    # Set delivery tag
                    delivery_tag = f"{stream_str}:{message_id_str}"
                    payload['properties']['delivery_tag'] = delivery_tag

                    # Rotate queue cycle
                    self._queue_cycle.rotate(queue)

                    # Deliver message
                    self.connection._deliver(payload, queue)
                    return True

        raise Empty()

    except self.connection_errors:
        self.client.connection.disconnect()
        raise
    finally:
        self._in_poll = None
        self._pending_streams = None
        self._pending_timeout = None
```

### Phase 5: Consumer Group Management

```python
@cached_property
def consumer_group(self):
    """Consumer group name for this connection."""
    db = self.connection.client.virtual_host or 0
    return f"{self.consumer_group_prefix}-{db}"

@cached_property
def consumer_id(self):
    """Unique consumer identifier."""
    import socket
    import os
    import threading

    hostname = socket.gethostname()
    pid = os.getpid()
    thread_id = threading.get_ident()

    return f"{hostname}-{pid}-{thread_id}"

def _get_all_streams(self):
    """Get list of all active streams for this channel."""
    streams = []
    for queue in self.active_queues:
        for pri in self.priority_steps:
            streams.append(self._stream_for_pri(queue, pri))
    return streams
```

### Phase 6: Priority Support

**Same as current**: Use separate streams for each priority level.

Stream naming:
- `queue_name` (priority 0)
- `queue_name\x06\x163` (priority 3)
- `queue_name\x06\x166` (priority 6)
- `queue_name\x06\x169` (priority 9)

XREADGROUP reads in order, so list high-priority streams first.

### Phase 7: Fanout Support with Streams! 🎉

**GOOD NEWS**: We can use Redis Streams for fanout instead of PUB/SUB!

**How Fanout Works in AMQP/Kombu**:
- Fanout exchange = broadcast to ALL bound queues
- One message published → copies delivered to every queue

**How Redis Streams Supports Fanout**:
- **Consumer Groups** are independent
- Each consumer group tracks its own position in the stream
- Multiple consumer groups can read from the same stream
- Each group gets ALL messages

**Implementation Strategy**:

```python
# For fanout exchanges, use ONE stream per exchange
# Create ONE consumer group per BOUND QUEUE

# Example: Fanout exchange "logs" with 3 queues bound
exchange = Exchange('logs', type='fanout')
queue1 = Queue('logger-service')
queue2 = Queue('metrics-service')
queue3 = Queue('audit-service')

# When queues bind:
def _queue_bind(self, exchange, routing_key, pattern, queue):
    if self.typeof(exchange).type == 'fanout':
        # Mark as fanout queue
        self._fanout_queues[queue] = (exchange, routing_key)

        # Create consumer group: one per queue
        stream_key = self._fanout_stream_key(exchange)
        group_name = self._fanout_consumer_group(queue)

        self._ensure_consumer_group(stream_key, group_name)

def _put_fanout(self, exchange, message, routing_key, **kwargs):
    """Publish to fanout stream (replaces PUBLISH)."""
    stream_key = self._fanout_stream_key(exchange)

    with self.conn_or_acquire() as client:
        client.xadd(
            name=stream_key,
            fields={
                'uuid': str(uuid7()),
                'payload': dumps(message),
            },
            id='*',
            maxlen=self.stream_maxlen,
            approximate=True
        )

def _fanout_stream_key(self, exchange):
    """Get stream key for fanout exchange."""
    return f"{self.keyprefix_fanout}{exchange}"

def _fanout_consumer_group(self, queue):
    """Get consumer group name for fanout queue."""
    # Each queue gets its own consumer group
    # This allows each queue to independently consume all messages
    return f"{self.consumer_group_prefix}-fanout-{queue}"

# Consumption (replaces SUBSCRIBE):
def _consume_fanout(self, queue):
    """Consume from fanout queue."""
    exchange, routing_key = self._fanout_queues[queue]
    stream_key = self._fanout_stream_key(exchange)
    group_name = self._fanout_consumer_group(queue)

    # Each queue has its own group, so all get all messages
    messages = self.client.xreadgroup(
        groupname=group_name,  # Queue-specific group
        consumername=self.consumer_id,
        streams={stream_key: '>'},
        count=self.prefetch_count or 1,
        block=self.brpop_timeout * 1000
    )

    # Process messages...
```

**Key Points**:
1. ✅ **No PUB/SUB needed** - Streams handle fanout natively
2. ✅ **Reliable** - Messages persist, not lost if consumer offline
3. ✅ **Acknowledged** - Can track which messages each queue has seen
4. ✅ **Simpler** - No separate subclient, LISTEN, PSUBSCRIBE complexity
5. ✅ **Unified** - Same code path for all exchange types

**Migration**:
- Remove: `subclient`, `_subscribe()`, `_unsubscribe_from()`, `_receive()`, `_receive_one()`, `_handle_message()`
- Remove: `active_fanout_queues`, `_fanout_to_queue` (simplify tracking)
- Remove: PUB/SUB connection management, LISTEN mode in poller
- Add: Fanout-specific consumer group management
- Modify: `_xreadgroup_start()` to include fanout streams with their groups

**Benefits Over PUB/SUB**:
| Feature | PUB/SUB (Old) | Streams (New) |
|---------|---------------|---------------|
| Message persistence | ❌ Lost if no subscriber | ✅ Persisted in stream |
| Acknowledgment | ❌ Fire-and-forget | ✅ Tracked per group |
| Offline consumers | ❌ Miss messages | ✅ Catch up when back |
| Observability | ❌ No history | ✅ XPENDING, XINFO |
| Code complexity | Separate paths | Unified |

**Consumer Group Naming**:
- Direct/Topic queues: `kombu-{db}` (shared, work queue)
- Fanout queues: `kombu-fanout-{queue_name}` (per-queue, broadcast)

### Phase 8: Message Restoration

**_do_restore_message()** - Simplified:
```python
def _do_restore_message(self, payload, exchange, routing_key,
                        client, leftmost=False):
    """Restore message to stream (simplified - no pipe needed)."""
    try:
        # Mark as redelivered
        try:
            payload['headers']['redelivered'] = True
            payload['properties']['delivery_info']['redelivered'] = True
        except KeyError:
            pass

        # Find destination queues
        for queue in self._lookup(exchange, routing_key):
            pri = self._get_message_priority(payload, reverse=False)
            stream_key = self._stream_for_pri(queue, pri)

            # Re-add to stream
            # Note: Using XADD means it goes to end (can't do leftmost easily)
            client.xadd(
                name=stream_key,
                fields={
                    'uuid': str(uuid7()),  # New UUID
                    'payload': dumps(payload),
                },
                id='*',
                maxlen=self.stream_maxlen,
                approximate=True
            )
    except Exception:
        crit('Could not restore message: %r', payload, exc_info=True)
```

### Phase 9: Queue Management

**_size()** - Get approximate stream size:
```python
def _size(self, queue):
    """Get approximate queue size across all priorities."""
    with self.conn_or_acquire() as client:
        total = 0
        for pri in self.priority_steps:
            stream_key = self._stream_for_pri(queue, pri)
            try:
                info = client.xinfo_stream(stream_key)
                # Use 'length' minus acked messages (approximate)
                total += info.get('length', 0)
            except self.ResponseError:
                # Stream doesn't exist
                pass
        return total
```

**_purge()** - Clear stream:
```python
def _purge(self, queue):
    """Purge all messages from queue."""
    with self.conn_or_acquire() as client:
        total = 0
        for pri in self.priority_steps:
            stream_key = self._stream_for_pri(queue, pri)
            try:
                # Get size before deletion
                info = client.xinfo_stream(stream_key)
                count = info.get('length', 0)

                # Delete and recreate stream
                client.delete(stream_key)

                # Recreate consumer group
                self._ensure_consumer_group(stream_key)

                total += count
            except self.ResponseError:
                pass
        return total
```

**_delete()** - Delete stream:
```python
def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
    """Delete queue and its streams."""
    self.auto_delete_queues.discard(queue)
    with self.conn_or_acquire(client=kwargs.get('client')) as client:
        # Remove from binding table
        client.srem(
            self.keyprefix_queue % (exchange,),
            self.sep.join([routing_key or '', pattern or '', queue or ''])
        )

        # Delete all priority streams
        for pri in self.priority_steps:
            stream_key = self._stream_for_pri(queue, pri)
            try:
                client.delete(stream_key)
            except self.ResponseError:
                pass
```

**_has_queue()** - Check if queue exists:
```python
def _has_queue(self, queue, **kwargs):
    """Check if any priority stream exists for queue."""
    with self.conn_or_acquire() as client:
        for pri in self.priority_steps:
            stream_key = self._stream_for_pri(queue, pri)
            if client.exists(stream_key):
                return True
        return False
```

### Phase 10: Stream Maintenance

**Add stream trimming**:
```python
def _trim_streams(self):
    """Trim streams to prevent unbounded growth."""
    with self.conn_or_acquire() as client:
        for stream in self._get_all_streams():
            try:
                client.xtrim(
                    name=stream,
                    maxlen=self.stream_maxlen,
                    approximate=True
                )
            except self.ResponseError:
                pass
```

**Add to Transport.register_with_event_loop**:
```python
# In register_with_event_loop, add periodic trimming
loop.call_repeatedly(
    self.stream_trim_interval or 300,  # Every 5 minutes
    cycle.maybe_trim_streams
)
```

### Phase 11: Update MultiChannelPoller

Minimal changes needed:
- `_register_BRPOP` → `_register_XREADGROUP`
- Update handler mapping: `{'XREADGROUP': self._xreadgroup_read, 'LISTEN': self._receive}`

### Phase 12: Transport Options

Add new options:
```python
from_transport_options = (
    virtual.Channel.from_transport_options +
    ('sep',
     'ack_emulation',  # Keep for compatibility, but always True for streams
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
     'consumer_group_prefix',  # NEW
     'stream_maxlen',          # NEW
     'autoclaim_interval',     # NEW
     'prefetch_count')         # NEW
)
```

### Phase 13: Testing Strategy

**Unit Tests** (`t/unit/transport/test_redis_streams.py`):
1. Copy test_redis.py structure
2. Mock redis streams commands (XADD, XREADGROUP, XACK, etc.)
3. Test each method in isolation
4. Test priority ordering
5. Test consumer group creation
6. Test message restoration
7. Test global key prefix

**Integration Tests** (`t/integration/test_redis_streams.py`):
1. Test publish/consume workflow
2. Test priority consumption order
3. Test multiple consumers (consumer groups)
4. Test message requeue
5. Test visibility timeout and recovery
6. Test fanout exchanges
7. Test connection failures and recovery
8. Performance comparison with lists transport

## Migration Path

### For Users

1. **Update connection string** (optional new transport name):
   ```python
   # Old
   conn = Connection('redis://localhost')

   # New (explicit)
   conn = Connection('redis+streams://localhost')
   ```

2. **Or use transport option**:
   ```python
   conn = Connection(
       'redis://localhost',
       transport='kombu.transport.redis_streams:Transport'
   )
   ```

3. **Configure streams options**:
   ```python
   conn = Connection(
       'redis://localhost',
       transport='kombu.transport.redis_streams:Transport',
       transport_options={
           'stream_maxlen': 50000,  # Max messages per stream
           'consumer_group_prefix': 'myapp',
           'visibility_timeout': 7200,  # 2 hours
       }
   )
   ```

### Breaking Changes

**None!** API compatibility is maintained.

**Behavioral differences**:
1. Message IDs are stream IDs (e.g., `1703612345678-0`) instead of UUIDs
   - This is internal; users shouldn't rely on delivery tag format
2. Requeue adds to end of stream (can't prepend like LPUSH)
   - This may affect requeue priority behavior
3. Stream messages persist after ACK (until trimmed)
   - Lists remove immediately on RPOP
   - Streams keep history for observability

### Data Migration

**None required!** Streams and lists can coexist:
- Old messages in lists will be consumed by old workers
- New messages in streams will be consumed by new workers
- Gradual migration is safe

## Success Criteria

- [ ] All existing unit tests pass (with mocked streams)
- [ ] All integration tests pass (with real Redis)
- [ ] Performance within 10% of lists transport
- [ ] Message delivery reliability ≥ lists transport
- [ ] Documentation complete
- [ ] Migration guide written

## Timeline Estimate

- Phase 1-2: 4 hours
- Phase 3-4: 8 hours
- Phase 5-6: 4 hours
- Phase 7-8: 4 hours
- Phase 9-10: 4 hours
- Phase 11: 6 hours (poller changes)
- Phase 12: 8 hours (unit tests)
- Phase 13: 8 hours (integration tests)
- Documentation: 4 hours

**Total: ~50 hours** of focused development

## Risk Mitigation

1. **Redis version requirement**: Streams require Redis 5.0+
   - Add version check in `__init__`
   - Clear error message for old Redis

2. **Performance regression**:
   - Benchmark before/after
   - Optimize XREADGROUP batch sizes
   - Use pipelining where possible

3. **Consumer group conflicts**:
   - Use unique consumer IDs
   - Document group naming conventions
   - Provide cleanup utilities

4. **Stream growth**:
   - Implement automatic trimming
   - Monitor stream sizes
   - Document trim strategies

## Future Enhancements

1. **XCLAIM automation**: Auto-claim stuck messages in background
2. **Stream compaction**: Periodic cleanup of acked messages
3. **Metrics export**: Expose PEL depth, consumer lag, etc.
4. **Dead letter queue**: Move failed messages after N retries
5. **Time-based retention**: TTL for messages instead of count-based

## References

- [Redis Streams Introduction](https://redis.io/docs/data-types/streams/)
- [Redis Streams Tutorial](https://redis.io/docs/data-types/streams-tutorial/)
- [XREADGROUP Documentation](https://redis.io/commands/xreadgroup/)
- [Kombu Virtual Transport](https://github.com/celery/kombu/blob/main/kombu/transport/virtual/base.py)
