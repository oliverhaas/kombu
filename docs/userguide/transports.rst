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

- None in core Kombu (interface only)

Third-party transports may implement this interface. Check the transport
documentation for details.
