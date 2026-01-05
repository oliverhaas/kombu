# kombu-asyncio

**Pure asyncio messaging library for Python - Redis transport only**

> **EXPERIMENTAL**: This is an experimental asyncio-native rewrite of Kombu.
> It is a **completely breaking change** from the original Kombu library.
> The goal is to enable asyncio-native Celery while keeping the Celery migration
> path as smooth as possible.

## Overview

`kombu-asyncio` is a ground-up rewrite of [Kombu](https://github.com/celery/kombu)
designed for Python's asyncio. Instead of adding async wrappers to synchronous code,
this library is built asyncio-first.

**Key differences from Kombu:**

- All operations are `async`/`await` (no sync API)
- Only Redis transport supported (via `redis.asyncio`)
- No Hub/Timer - uses asyncio event loop directly
- Simplified, minimal codebase
- Python 3.10+ required

## Installation

```bash
pip install kombu-asyncio
```

## Quick Start

```python
import asyncio
from kombu import Connection, Queue

async def main():
    async with Connection('redis://localhost:6379') as conn:
        # Simple queue API
        async with conn.SimpleQueue('my_queue') as queue:
            # Publish
            await queue.put({'hello': 'world'})

            # Consume
            message = await queue.get(timeout=5)
            print(message.payload)  # {'hello': 'world'}
            await message.ack()

asyncio.run(main())
```

## Producer/Consumer Pattern

```python
import asyncio
from kombu import Connection, Exchange, Queue

async def main():
    async with Connection('redis://localhost:6379') as conn:
        # Declare exchange and queue
        exchange = Exchange('media', type='direct')
        queue = Queue('video', exchange=exchange, routing_key='video')

        # Publish messages
        async with conn.Producer(exchange=exchange) as producer:
            await producer.publish(
                {'filename': 'video.mp4', 'size': 1024},
                routing_key='video',
            )

        # Consume messages
        def process_video(body, message):
            print(f"Processing: {body}")
            # message.ack() is called automatically if no_ack=True

        async with conn.Consumer([queue], callbacks=[process_video]) as consumer:
            await conn.drain_events(timeout=5)

asyncio.run(main())
```

## Async Callbacks

Callbacks can be async functions:

```python
async def process_video(body, message):
    await some_async_operation(body)
    await message.ack()

async with conn.Consumer([queue], callbacks=[process_video], no_ack=False):
    await conn.drain_events(timeout=5)
```

## Exchange Types

Supported exchange types (emulated on Redis):

- **direct**: Routes to queues with matching routing key
- **fanout**: Routes to all bound queues (uses Redis Pub/Sub)
- **topic**: Pattern-based routing (`*` matches one word, `#` matches zero or more)

## Breaking Changes from Kombu

This is **not** a drop-in replacement for Kombu. Key differences:

| Kombu | kombu-asyncio |
|-------|---------------|
| `with Connection() as conn:` | `async with Connection() as conn:` |
| `conn.drain_events()` | `await conn.drain_events()` |
| `producer.publish(...)` | `await producer.publish(...)` |
| `message.ack()` | `await message.ack()` |
| Multiple transports | Redis only |
| Sync + async API | Async only |

## Celery Integration

This library is designed to power an asyncio-native version of Celery
(`celery-asyncio`). The Celery layer will provide backward compatibility
for existing Celery users through `asyncio.to_thread()` for sync tasks.

## Requirements

- Python 3.10+
- redis-py 7.1+

## License

BSD-3-Clause (same as Kombu)

## Links

- [Kombu Documentation](https://kombu.readthedocs.io/) (original sync version)
- [Source Code](https://github.com/celery/kombu/tree/main-asyncio)
- [Celery Project](https://github.com/celery/celery)
