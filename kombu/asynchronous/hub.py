"""Event loop implementation."""

from __future__ import annotations

import asyncio
import errno
import threading
from contextlib import contextmanager
from copy import copy
from queue import Empty
from time import sleep
from types import GeneratorType as generator

from vine import Thenable, promise

from kombu.log import get_logger
from kombu.utils.compat import fileno
from kombu.utils.eventio import ERR, READ, WRITE, poll
from kombu.utils.objects import cached_property

from .timer import Timer

__all__ = ('Hub', 'AsyncioHub', 'get_event_loop', 'set_event_loop')
logger = get_logger(__name__)

_current_loop: Hub | None = None

W_UNKNOWN_EVENT = """\
Received unknown event %r for fd %r, please contact support!\
"""


class Stop(BaseException):
    """Stops the event loop."""


def _raise_stop_error():
    raise Stop()


@contextmanager
def _dummy_context(*args, **kwargs):
    yield


def get_event_loop() -> Hub | None:
    """Get current event loop object."""
    return _current_loop


def set_event_loop(loop: Hub | None) -> Hub | None:
    """Set the current event loop object."""
    global _current_loop
    _current_loop = loop
    return loop


class Hub:
    """Event loop object.

    Arguments:
    ---------
        timer (kombu.asynchronous.Timer): Specify custom timer instance.
    """

    #: Flag set if reading from an fd will not block.
    READ = READ

    #: Flag set if writing to an fd will not block.
    WRITE = WRITE

    #: Flag set on error, and the fd should be read from asap.
    ERR = ERR

    #: List of callbacks to be called when the loop is exiting,
    #: applied with the hub instance as sole argument.
    on_close = None

    def __init__(self, timer=None):
        self.timer = timer if timer is not None else Timer()

        self.readers = {}
        self.writers = {}
        self.on_tick = set()
        self.on_close = set()
        self._ready = set()
        self._ready_lock = threading.Lock()

        self._running = False
        self._loop = None

        # The eventloop (in celery.worker.loops)
        # will merge fds in this set and then instead of calling
        # the callback for each ready fd it will call the
        # :attr:`consolidate_callback` with the list of ready_fds
        # as an argument.  This API is internal and is only
        # used by the multiprocessing pool to find inqueues
        # that are ready to write.
        self.consolidate = set()
        self.consolidate_callback = None

        self.propagate_errors = ()

        self._create_poller()

    @property
    def poller(self):
        if not self._poller:
            self._create_poller()
        return self._poller

    @poller.setter
    def poller(self, value):
        self._poller = value

    def reset(self):
        self.close()
        self._create_poller()

    def _create_poller(self):
        self._poller = poll()
        self._register_fd = self._poller.register
        self._unregister_fd = self._poller.unregister

    def _close_poller(self):
        if self._poller is not None:
            self._poller.close()
            self._poller = None
            self._register_fd = None
            self._unregister_fd = None

    def stop(self):
        self.call_soon(_raise_stop_error)

    def __repr__(self):
        return '<Hub@{:#x}: R:{} W:{}>'.format(
            id(self), len(self.readers), len(self.writers),
        )

    def fire_timers(self, min_delay=1, max_delay=10, max_timers=10,
                    propagate=()):
        timer = self.timer
        delay = None
        if timer and timer._queue:
            for i in range(max_timers):
                delay, entry = next(self.scheduler)
                if entry is None:
                    break
                try:
                    entry()
                except propagate:
                    raise
                except (MemoryError, AssertionError):
                    raise
                except OSError as exc:
                    if exc.errno == errno.ENOMEM:
                        raise
                    logger.error('Error in timer: %r', exc, exc_info=1)
                except Exception as exc:
                    logger.error('Error in timer: %r', exc, exc_info=1)
        return min(delay or min_delay, max_delay)

    def _remove_from_loop(self, fd):
        try:
            self._unregister(fd)
        finally:
            self._discard(fd)

    def add(self, fd, callback, flags, args=(), consolidate=False):
        fd = fileno(fd)
        try:
            self.poller.register(fd, flags)
        except ValueError:
            self._remove_from_loop(fd)
            raise
        else:
            dest = self.readers if flags & READ else self.writers
            if consolidate:
                self.consolidate.add(fd)
                dest[fd] = None
            else:
                dest[fd] = callback, args

    def remove(self, fd):
        fd = fileno(fd)
        self._remove_from_loop(fd)

    def run_forever(self):
        self._running = True
        try:
            while 1:
                try:
                    self.run_once()
                except Stop:
                    break
        finally:
            self._running = False

    def run_once(self):
        try:
            next(self.loop)
        except StopIteration:
            self._loop = None

    def call_soon(self, callback, *args):
        if not isinstance(callback, Thenable):
            callback = promise(callback, args)
        with self._ready_lock:
            self._ready.add(callback)
        return callback

    def call_later(self, delay, callback, *args):
        return self.timer.call_after(delay, callback, args)

    def call_at(self, when, callback, *args):
        return self.timer.call_at(when, callback, args)

    def call_repeatedly(self, delay, callback, *args):
        return self.timer.call_repeatedly(delay, callback, args)

    def add_reader(self, fds, callback, *args):
        return self.add(fds, callback, READ | ERR, args)

    def add_writer(self, fds, callback, *args):
        return self.add(fds, callback, WRITE, args)

    def remove_reader(self, fd):
        writable = fd in self.writers
        on_write = self.writers.get(fd)
        try:
            self._remove_from_loop(fd)
        finally:
            if writable:
                cb, args = on_write
                self.add(fd, cb, WRITE, args)

    def remove_writer(self, fd):
        readable = fd in self.readers
        on_read = self.readers.get(fd)
        try:
            self._remove_from_loop(fd)
        finally:
            if readable:
                cb, args = on_read
                self.add(fd, cb, READ | ERR, args)

    def _unregister(self, fd):
        try:
            self.poller.unregister(fd)
        except (AttributeError, KeyError, OSError):
            pass

    def _pop_ready(self):
        with self._ready_lock:
            ready = self._ready
            self._ready = set()
            return ready

    def close(self, *args):
        [self._unregister(fd) for fd in self.readers]
        self.readers.clear()
        [self._unregister(fd) for fd in self.writers]
        self.writers.clear()
        self.consolidate.clear()
        self._close_poller()
        for callback in self.on_close:
            callback(self)

        # Complete remaining todo before Hub close
        # Eg: Acknowledge message
        # To avoid infinite loop where one of the callables adds items
        # to self._ready (via call_soon or otherwise).
        # we create new list with current self._ready
        todos = self._pop_ready()
        for item in todos:
            item()

        # Clear global event loop variable if this hub is the current loop
        if _current_loop is self:
            set_event_loop(None)

    def _discard(self, fd):
        fd = fileno(fd)
        self.readers.pop(fd, None)
        self.writers.pop(fd, None)
        self.consolidate.discard(fd)

    def on_callback_error(self, callback, exc):
        logger.error(
            'Callback %r raised exception: %r', callback, exc, exc_info=1,
        )

    def create_loop(self,
                    generator=generator, sleep=sleep, min=min, next=next,
                    Empty=Empty, StopIteration=StopIteration,
                    KeyError=KeyError, READ=READ, WRITE=WRITE, ERR=ERR):
        readers, writers = self.readers, self.writers
        poll = self.poller.poll
        fire_timers = self.fire_timers
        hub_remove = self.remove
        scheduled = self.timer._queue
        consolidate = self.consolidate
        consolidate_callback = self.consolidate_callback
        propagate = self.propagate_errors

        while 1:
            todo = self._pop_ready()

            for item in todo:
                if item:
                    item()

            poll_timeout = fire_timers(propagate=propagate) if scheduled else 1

            for tick_callback in copy(self.on_tick):
                tick_callback()

            #  print('[[[HUB]]]: %s' % (self.repr_active(),))
            if readers or writers:
                to_consolidate = []
                try:
                    events = poll(poll_timeout)
                    #  print('[EVENTS]: %s' % (self.repr_events(events),))
                except ValueError:  # Issue celery/#882
                    return

                for fd, event in events or ():
                    general_error = False
                    if fd in consolidate and \
                            writers.get(fd) is None:
                        to_consolidate.append(fd)
                        continue
                    cb = cbargs = None

                    if event & READ:
                        try:
                            cb, cbargs = readers[fd]
                        except KeyError:
                            self.remove_reader(fd)
                            continue
                    elif event & WRITE:
                        try:
                            cb, cbargs = writers[fd]
                        except KeyError:
                            self.remove_writer(fd)
                            continue
                    elif event & ERR:
                        general_error = True
                    else:
                        logger.info(W_UNKNOWN_EVENT, event, fd)
                        general_error = True

                    if general_error:
                        try:
                            cb, cbargs = (readers.get(fd) or
                                          writers.get(fd))
                        except TypeError:
                            pass

                    if cb is None:
                        self.remove(fd)
                        continue

                    if isinstance(cb, generator):
                        try:
                            next(cb)
                        except OSError as exc:
                            if exc.errno != errno.EBADF:
                                raise
                            hub_remove(fd)
                        except StopIteration:
                            pass
                        except Exception:
                            hub_remove(fd)
                            raise
                    else:
                        try:
                            cb(*cbargs)
                        except Empty:
                            pass
                if to_consolidate:
                    consolidate_callback(to_consolidate)
            else:
                # no sockets yet, startup is probably not done.
                sleep(min(poll_timeout, 0.1))
            yield

    def repr_active(self):
        from .debug import repr_active
        return repr_active(self)

    def repr_events(self, events):
        from .debug import repr_events
        return repr_events(self, events or [])

    @cached_property
    def scheduler(self):
        return iter(self.timer)

    @property
    def loop(self):
        if self._loop is None:
            self._loop = self.create_loop()
        return self._loop


class AsyncioHub(Hub):
    """Hub implementation that delegates to asyncio event loop.

    This subclass integrates with asyncio's event loop, allowing Kombu
    to operate in async contexts while maintaining backward compatibility
    with sync code.

    When running in async mode (detected via asyncio.get_running_loop()),
    fd registration and callback scheduling are delegated to the asyncio
    event loop instead of using manual polling.
    """

    def __init__(self, timer=None):
        super().__init__(timer=timer)
        self._asyncio_loop = None
        self._async_mode = False
        self._asyncio_handles = {}  # Track asyncio handles for cleanup

    def _get_asyncio_loop(self):
        """Get the asyncio event loop, creating one if needed."""
        if self._asyncio_loop is None:
            try:
                self._asyncio_loop = asyncio.get_running_loop()
                self._async_mode = True
            except RuntimeError:
                # Not in async context, don't create a new loop
                pass
        return self._asyncio_loop

    def _ensure_async_mode(self):
        """Check if we're in async mode and update state."""
        try:
            loop = asyncio.get_running_loop()
            if loop != self._asyncio_loop:
                self._asyncio_loop = loop
            self._async_mode = True
            return True
        except RuntimeError:
            self._async_mode = False
            return False

    @property
    def in_async_context(self):
        """Return True if currently running in an async context."""
        return self._ensure_async_mode()

    def add_reader(self, fd, callback, *args):
        """Register fd for reading.

        In async mode, delegates to asyncio loop.add_reader().
        """
        if self._async_mode and self._asyncio_loop:
            fd_num = fileno(fd)
            self._asyncio_loop.add_reader(fd_num, callback, *args)
            self.readers[fd_num] = callback, args
        else:
            super().add_reader(fd, callback, *args)

    def add_writer(self, fd, callback, *args):
        """Register fd for writing.

        In async mode, delegates to asyncio loop.add_writer().
        """
        if self._async_mode and self._asyncio_loop:
            fd_num = fileno(fd)
            self._asyncio_loop.add_writer(fd_num, callback, *args)
            self.writers[fd_num] = callback, args
        else:
            super().add_writer(fd, callback, *args)

    def remove_reader(self, fd):
        """Remove fd from reading.

        In async mode, delegates to asyncio loop.remove_reader().
        """
        if self._async_mode and self._asyncio_loop:
            fd_num = fileno(fd)
            try:
                self._asyncio_loop.remove_reader(fd_num)
            except (ValueError, OSError):
                pass
            self.readers.pop(fd_num, None)
        else:
            super().remove_reader(fd)

    def remove_writer(self, fd):
        """Remove fd from writing.

        In async mode, delegates to asyncio loop.remove_writer().
        """
        if self._async_mode and self._asyncio_loop:
            fd_num = fileno(fd)
            try:
                self._asyncio_loop.remove_writer(fd_num)
            except (ValueError, OSError):
                pass
            self.writers.pop(fd_num, None)
        else:
            super().remove_writer(fd)

    def call_soon(self, callback, *args):
        """Schedule callback to be called soon.

        In async mode, delegates to asyncio loop.call_soon().
        """
        if self._async_mode and self._asyncio_loop:
            if not isinstance(callback, Thenable):
                callback = promise(callback, args)
            self._asyncio_loop.call_soon(callback)
            return callback
        return super().call_soon(callback, *args)

    def call_later(self, delay, callback, *args):
        """Schedule callback to be called after delay seconds.

        In async mode, delegates to asyncio loop.call_later().
        """
        if self._async_mode and self._asyncio_loop:
            handle = self._asyncio_loop.call_later(delay, callback, *args)
            return handle
        return super().call_later(delay, callback, *args)

    def call_at(self, when, callback, *args):
        """Schedule callback to be called at specific time.

        In async mode, delegates to asyncio loop.call_at().
        """
        if self._async_mode and self._asyncio_loop:
            handle = self._asyncio_loop.call_at(when, callback, *args)
            return handle
        return super().call_at(when, callback, *args)

    async def arun_once(self):
        """Async version of run_once - yields control to asyncio."""
        # Ensure we're in async mode
        self._ensure_async_mode()

        # Process ready callbacks
        ready = self._pop_ready()
        for item in ready:
            if item:
                if asyncio.iscoroutinefunction(item):
                    await item()
                else:
                    item()

        # Fire timers
        if self.timer and self.timer._queue:
            self.fire_timers()

        # Yield control to asyncio event loop
        await asyncio.sleep(0)

    async def arun_forever(self):
        """Async event loop - runs until stop() is called."""
        self._running = True
        self._ensure_async_mode()
        try:
            while self._running:
                try:
                    await self.arun_once()
                except Stop:
                    break
        finally:
            self._running = False

    async def afire_timers(self, min_delay=1, max_delay=10, max_timers=10):
        """Async version of fire_timers that can await coroutines."""
        timer = self.timer
        delay = None
        if timer and timer._queue:
            for _ in range(max_timers):
                delay, entry = next(self.scheduler)
                if entry is None:
                    break
                try:
                    if asyncio.iscoroutinefunction(entry.fun):
                        await entry.fun(*entry.args, **entry.kwargs)
                    else:
                        entry()
                except (MemoryError, AssertionError):
                    raise
                except OSError as exc:
                    if exc.errno == errno.ENOMEM:
                        raise
                    logger.error('Error in timer: %r', exc, exc_info=1)
                except Exception as exc:
                    logger.error('Error in timer: %r', exc, exc_info=1)
        return min(delay or min_delay, max_delay)

    def close(self, *args):
        """Close the hub, cleaning up asyncio resources."""
        # Clean up asyncio readers/writers if in async mode
        if self._async_mode and self._asyncio_loop:
            for fd in list(self.readers.keys()):
                try:
                    self._asyncio_loop.remove_reader(fd)
                except (ValueError, OSError):
                    pass
            for fd in list(self.writers.keys()):
                try:
                    self._asyncio_loop.remove_writer(fd)
                except (ValueError, OSError):
                    pass

        # Reset async state
        self._asyncio_loop = None
        self._async_mode = False

        # Call parent close
        super().close(*args)

    def __repr__(self):
        mode = 'async' if self._async_mode else 'sync'
        return '<AsyncioHub@{:#x}: R:{} W:{} mode:{}>'.format(
            id(self), len(self.readers), len(self.writers), mode,
        )
