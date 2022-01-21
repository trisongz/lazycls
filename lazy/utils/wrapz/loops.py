
"""
Taken from 
https://stackoverflow.com/questions/52232177/runtimeerror-timeout-context-manager-should-be-used-inside-a-task/69514930#69514930

"""

import asyncio
import threading
from typing import Awaitable, TypeVar

T = TypeVar("T")

def _start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


_LOOP: asyncio.AbstractEventLoop = None
_LOOP_THREAD: threading.Thread = None

_LOOP_ACTIVE = lambda: bool(_LOOP is not None)

def _ensure_loop_started():
    """
    Used to Start loop when needed.
    """
    global _LOOP, _LOOP_THREAD
    if _LOOP is not None: return
    _LOOP = asyncio.new_event_loop()
    _LOOP_THREAD = threading.Thread(target=_start_background_loop, args=(_LOOP,), daemon=True)
    _LOOP_THREAD.start()

def get_loop():
    """
    Returns the created EventLoop.
    Starts it if it isnt running.
    """
    _ensure_loop_started()
    return _LOOP


def asyncio_run(coro: Awaitable[T], timeout=30) -> T:
    """
    Runs the coroutine in an event loop running on a background thread,
    and blocks the current thread until it returns a result.
    This plays well with gevent, since it can yield on the Future result call.

    :param coro: A coroutine, typically an async method
    :param timeout: How many seconds we should wait for a result before raising an error
    """
    _ensure_loop_started()
    return asyncio.run_coroutine_threadsafe(coro, _LOOP).result(timeout=timeout)


def asyncio_gather(*futures, return_exceptions=False):
    """
    A version of asyncio.gather that runs on the internal event loop
    """
    _ensure_loop_started()
    return asyncio.gather(*futures, loop=_LOOP, return_exceptions=return_exceptions)


__all__ = (
    'get_loop',
    'asyncio_run',
    'asyncio_gather',
    '_LOOP_ACTIVE'
)
