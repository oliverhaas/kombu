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

Some transports support handling delayed messages natively within the broker,
rather than requiring workers to hold messages in memory until their scheduled
time.

**Checking for Support**

You can check if a transport supports native delayed delivery:

.. code-block:: python

    from kombu import Connection

    with Connection('redis://localhost') as conn:
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

**Message Property: eta**

The ``eta`` (estimated time of arrival) property specifies when a message
should become available for delivery. It is stored in the message properties
alongside ``priority``:

.. code-block:: python

    message = {
        'body': ...,
        'properties': {
            'priority': 0,
            'eta': 1704067200.0,  # Unix timestamp (float, seconds)
            'delivery_tag': ...,
        },
        'headers': {...}
    }

The ``eta`` value is an **absolute Unix timestamp** (seconds since epoch) as
a float. This is the same format used by Celery's ``eta`` parameter.

When publishing with Celery, the ``eta`` or ``countdown`` task options are
converted to this property. For direct Kombu usage:

.. code-block:: python

    import time
    from kombu import Connection, Producer, Queue

    queue = Queue('tasks')

    with Connection('redis+plus://localhost') as conn:
        producer = Producer(conn)

        # Deliver in 60 seconds
        eta = time.time() + 60

        producer.publish(
            {'task': 'example'},
            exchange=queue.exchange,
            routing_key=queue.routing_key,
            declare=[queue],
            eta=eta,  # Producer converts this to properties['eta']
        )

**For Transport Authors**

To implement native delayed delivery in a custom transport:

1. Set the capability flag on your ``Transport`` class:

   .. code-block:: python

       from kombu.transport.base import Transport

       class MyTransport(Transport):
           supports_native_delayed_delivery = True

2. In your ``Channel._put()`` method, check for ``message['properties'].get('eta')``.
   If present and in the future, store the message for delayed delivery instead
   of making it immediately available.

3. Implement a mechanism to make delayed messages available when their ``eta``
   arrives. This could be:

   - A background thread/task that periodically checks for due messages
   - Integration with broker-native scheduling features
   - Use of the event loop via ``loop.call_repeatedly()``

**Transport Support**

Currently, the following transports support native delayed delivery:

- None in core Kombu (interface only)

Third-party transports may implement this interface. Check the transport
documentation for details.
