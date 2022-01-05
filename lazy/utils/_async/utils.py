import math

import sniffio
import threading
from importlib import import_module
from typing import Any, Callable, Coroutine, Dict, Generator, Optional, Tuple, Type, TypeVar

from lazy.libz import Lib

BACKENDS = 'asyncio', 'trio'

T_Retval = TypeVar('T_Retval')
threadlocals = threading.local()

DefaultAsyncBackend: str = 'trio' if Lib.is_avail_trio else 'asyncio'


"""
Borrowing from funcs and utils from anyio
"""

def run_async(func: Callable[..., Coroutine[Any, Any, T_Retval]], *args: object, backend: str = DefaultAsyncBackend, backend_options: Optional[Dict[str, Any]] = None) -> T_Retval:
    """
    Run the given coroutine function in an asynchronous event loop.
    The current thread must not be already running an event loop.
    :param func: a coroutine function
    :param args: positional arguments to ``func``
    :param backend: name of the asynchronous event loop implementation – currently either
        ``asyncio`` or ``trio``
    :param backend_options: keyword arguments to call the backend ``run()`` implementation with
        (documented :ref:`here <backend options>`)
    :return: the return value of the coroutine function
    :raises RuntimeError: if an asynchronous event loop is already running in this thread
    :raises LookupError: if the named backend is not found
    """
    try: asynclib_name = sniffio.current_async_library()
    except sniffio.AsyncLibraryNotFoundError: pass
    else: raise RuntimeError(f'Already running {asynclib_name} in this thread')

    try: asynclib = import_module(f'anyio._backends._{backend}', package=__name__)
    except ImportError as exc: raise LookupError(f'No such backend: {backend}') from exc

    token = None
    if sniffio.current_async_library_cvar.get(None) is None: token = sniffio.current_async_library_cvar.set(backend)

    try:
        backend_options = backend_options or {}
        return asynclib.run(func, *args, **backend_options)  # type: ignore
    finally:
        if token: sniffio.current_async_library_cvar.reset(token)

