.. _guide-transports:

============
 Transports
============

.. contents::
    :local:
    :depth: 2

.. _transports-overview:

Overview
========

Transports are the underlying mechanism that Kombu uses to communicate with
message brokers. Each transport provides a consistent interface while handling
the specifics of the underlying protocol.

.. _transports-capabilities:

Transport Capabilities
======================

Transports can declare various capabilities that affect how Kombu and
higher-level libraries like Celery interact with them.

.. _transports-native-delayed-delivery:

Native Delayed Delivery
-----------------------

Some transports support handling delayed messages (messages with ``eta`` or
``countdown``) natively within the broker, rather than requiring workers to
hold messages in memory until their scheduled time.

**Checking for Support**

You can check if a transport supports native delayed delivery:

.. code-block:: python

    from kombu import Connection

    with Connection('redis+plus://localhost') as conn:
        transport = conn.transport
        if transport.supports_native_delayed_delivery:
            print("This transport handles delays natively!")
        else:
            print("Delays are handled by workers.")

**Benefits of Native Delayed Delivery**

When a transport supports native delayed delivery:

- **Reduced memory pressure**: Workers don't need to hold delayed messages
  in memory.
- **Message persistence**: Delayed messages survive worker restarts since
  they're stored in the broker.
- **Better load distribution**: The broker can deliver messages to any
  available worker when the delay expires.

**For Transport Authors**

To implement native delayed delivery in a custom transport:

1. Set the capability flag on your ``Transport`` class:

   .. code-block:: python

       from kombu.transport.base import Transport

       class MyTransport(Transport):
           supports_native_delayed_delivery = True

2. Implement the channel methods in your ``Channel`` class:

   .. code-block:: python

       from kombu.transport.base import StdChannel

       class MyChannel(StdChannel):

           def setup_native_delayed_delivery(self, queues):
               """Initialize delayed delivery for the given queues.

               Called when consumers start. Use this to set up any
               necessary broker structures (e.g., delay queues,
               sorted sets, etc.).
               """
               # Your implementation here
               pass

           def teardown_native_delayed_delivery(self):
               """Clean up delayed delivery handling.

               Called when consumers stop. Use this to clean up
               any resources created during setup.
               """
               # Your implementation here
               pass

**Transport Support**

Currently, the following transports support native delayed delivery:

- ``redis+plus://`` - Redis Plus transport (uses sorted sets for delayed delivery)

Third-party transports may implement this interface. Check the transport
documentation for details.

.. _transports-redis-plus:

Redis Plus Transport
====================

The Redis Plus transport (``redis+plus://``) is a modern Redis transport that
uses sorted sets for queues, Redis Streams for durable fanout, and implements
native delayed delivery support.

Connection String
-----------------

.. code-block::

    redis+plus://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/VIRTUALHOST]
    rediss+plus://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/VIRTUALHOST]

Key Differences from Standard Redis Transport
---------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Feature
     - Standard Redis
     - Redis Plus
   * - Queue structure
     - Lists (BRPOP)
     - Sorted sets (BZMPOP)
   * - Priority levels
     - ~10 (separate queues)
     - 256 (score-based)
   * - Delayed delivery
     - Worker memory
     - Native (sorted set scores)
   * - Reliability
     - BRPOP + restore
     - BZMPOP + visibility timeout
   * - Fanout
     - PUB/SUB (lossy)
     - Streams (durable)

Requirements
------------

- Redis 7.0+ (for BZMPOP command)
- redis-py 3.2.0+

Transport Options
-----------------

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Option
     - Default
     - Description
   * - ``visibility_timeout``
     - 300
     - Seconds before unacked messages are requeued
   * - ``stream_maxlen``
     - 10000
     - Maximum length of fanout streams
   * - ``global_keyprefix``
     - ""
     - Prefix for all Redis keys
   * - ``message_ttl``
     - 259200
     - TTL in seconds for message hashes (3 days)
   * - ``requeue_check_interval``
     - 60
     - Seconds between delayed/timeout message checks
   * - ``requeue_batch_limit``
     - 1000
     - Max messages per requeue cycle
   * - ``socket_timeout``
     - None
     - Socket timeout in seconds
   * - ``socket_connect_timeout``
     - None
     - Socket connect timeout in seconds
   * - ``max_connections``
     - 10
     - Max connections in pool
   * - ``health_check_interval``
     - 25
     - Health check interval in seconds

Example Usage
-------------

Basic connection:

.. code-block:: python

    from kombu import Connection

    with Connection('redis+plus://localhost:6379/0') as conn:
        # Use the connection
        pass

With transport options:

.. code-block:: python

    from kombu import Connection

    conn = Connection(
        'redis+plus://localhost:6379/0',
        transport_options={
            'visibility_timeout': 600,
            'global_keyprefix': 'myapp:',
        }
    )

Redis Key Schema
----------------

The Redis Plus transport uses the following key schema:

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Key Pattern
     - Type
     - Purpose
   * - ``{prefix}queue:{name}``
     - Sorted Set
     - Queue messages (score = priority + timestamp)
   * - ``{prefix}message:{tag}``
     - Hash
     - Per-message data (payload, routing_key, etc.)
   * - ``{prefix}messages_index``
     - Sorted Set
     - Visibility timeout and delayed message tracking
   * - ``{prefix}/{db}.{exchange}``
     - Stream
     - Fanout messages (durable)
   * - ``{prefix}_kombu.binding.{ex}``
     - Set
     - Exchange bindings

Migration from Standard Redis Transport
---------------------------------------

Users can migrate from the standard Redis transport to Redis Plus:

1. Update your connection string from ``redis://`` to ``redis+plus://``
2. Ensure Redis 7.0+ is being used
3. If using Celery, use the built-in migrate command:

   .. code-block:: bash

       celery -A myapp migrate redis://localhost/0 redis+plus://localhost/0

Note: The ``queue:`` prefix ensures no collision with existing list-based queues
from the standard Redis transport.
