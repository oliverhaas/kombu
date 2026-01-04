"""Built-in transports - Pure asyncio version.

This version of Kombu only supports Redis transport.
"""

from __future__ import annotations

from .redis import Transport

TRANSPORT_ALIASES = {
    'redis': 'kombu.transport.redis:Transport',
    'rediss': 'kombu.transport.redis:Transport',
}

__all__ = ('Transport', 'TRANSPORT_ALIASES')
