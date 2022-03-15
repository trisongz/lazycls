import anyio
import inspect
from collections import deque
from datetime import datetime, timedelta
from anyio.to_thread import run_sync as _run_sync
from functools import wraps, partial, lru_cache
from typing import Callable, Dict, List, Any, Union, Iterable, Generator, Awaitable, Optional, Coroutine

try:
    from tqdm.auto import tqdm, trange
    from tqdm.asyncio import trange as async_trange
    from tqdm.asyncio import tqdm as async_tqdm

    _tqdm_enabled = True
except ImportError:
    tqdm: object = None
    trange: object = None
    async_trange: object = None
    async_tqdm: object = None
    _tqdm_enabled = False

try:
    import trio
    _async_backend = 'trio'

#except ImportError:
except: _async_backend = 'asyncio'

from .loops import *


CoroutineResult = Awaitable[Any]
CoroutineFunction = Callable[..., CoroutineResult]
CoroutineMethod = Callable[..., CoroutineResult]


def iscoroutinefunction(obj):
    """
    This is probably in the library elsewhere but returns bool
    based on if the function is a coro
    """
    if inspect.iscoroutinefunction(obj): return True
    if hasattr(obj, '__call__') and inspect.iscoroutinefunction(obj.__call__): return True
    return False


async def to_thread(func: Callable, *args, **kwargs) -> Any:
    # anyio's run_sync() doesn't support passing kwargs
    func_kwargs = partial(func, **kwargs)
    return await _run_sync(func_kwargs, *args)


async def run_sync(func: Callable, *args, **kwargs) -> Any:
    # anyio's run_sync() doesn't support passing kwargs
    func_kwargs = partial(func, **kwargs)
    return await _run_sync(func_kwargs, *args)



def run_async_asyncio(func, *args, **kwargs):
    """
    Runs an Async Function in a Sync Call using asyncio
    in sync calls
    """
    if not iscoroutinefunction(func):
        func = AsyncFunction(func)
    coro = func(*args, **kwargs)
    return asyncio_run(coro)


def run_async_anyio(func, *args, **kwargs):
    """
    Runs an Async Function in a Sync Call using anyio
    Returns the Results
    """
    current_async_module = getattr(anyio._core._eventloop.threadlocals, "current_async_module", None)
    partial_func = partial(func, *args, **kwargs)
    try:
        if current_async_module is None:
            return anyio.run(partial_func)
            #return anyio.run(partial_func, backend = _async_backend)
        return anyio.from_thread.run(partial_func)
    except:
        return run_async_asyncio(func, *args, **kwargs)


def run_async(func: Coroutine, *args, **kwargs):
    """
    Returns the result of the coroutine using asyncio/anyio
    in sync calls
    """
    if _LOOP_ACTIVE(): 
        return run_async_asyncio(func, *args, **kwargs)
    return run_async_anyio(func, *args, **kwargs)

"""
Utilities
"""

def count_iterable(iterable):
    if hasattr(iterable, '__len__'):
        return len(iterable)
    d = deque(enumerate(iterable, 1), maxlen=1)
    return d[0][0] if d else 0

async def async_count_iterable(iterable):
    if hasattr(iterable, '__len__'):
        return len(iterable)
    #return sum(1 for _ in await iterable())
    d = deque(enumerate(await iterable(), 1), maxlen=1)
    return d[0][0] if d else 0

def wrap_tqdm_func(func: Callable, desc: str = None, **config):
    desc = desc + f' {func.__name__}' if desc else func.__name__
    @wraps(func)
    def wrapped_tqdm_func(*args, **kwargs) -> 'trange':
        func_iter = partial(func, *args, **kwargs)
        return trange(count_iterable(func_iter()), desc = desc,  **config)
    return wrapped_tqdm_func
    

def wrap_tqdm_iterable(func: Callable, desc: str = None, leave: bool = False, **config) -> trange:
    """
    Assumes that the func is already wrapped.
    May have some overhead.
    """
    #_func = sync_to_async_func(func) if not iscoroutinefunction(func) else func
    #_func_length = count_iterable(anyio.run_async_from_thread(func)) if iscoroutinefunction(func) else count_iterable(get_async_run_func(func))
    #return trange(_func_length, desc = desc, leave = leave, **config)
    return trange(count_iterable(func()), desc = desc, leave = leave, **config)

"""
Wraps
"""


def timed_cache(seconds: int, maxsize: int = 128):
    def wrapper_cache(func: Callable):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.utcnow() + func.lifetime
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            if datetime.utcnow() >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.utcnow() + func.lifetime
            return func(*args, **kwargs)
        return wrapped_func
    return wrapper_cache


def AsyncFunction(func: Callable) -> CoroutineFunction:
    """
    Converts a Sync Function to Async Function
    """
    @wraps(func)
    async def new_func(*args, **kwargs) -> Any:
        return await run_sync(func, *args, **kwargs)
    return new_func

def AsyncClassFunction(func: Callable) -> CoroutineFunction:
    """
    Converts a Sync Class Function to Async Class Function
    """
    @wraps(func)
    async def new_func(self, *args, **kwargs) -> Any:
        return await run_sync(func, *args, **kwargs)
    return new_func


def AsyncMethod(func: Callable) -> CoroutineMethod:
    """
    Converts a Sync Class Method to Async Class Function
    """
    @wraps(func)
    async def method(self, *args, **kwargs) -> Any:
        return await run_sync(func, *args, **kwargs)
    return method


def AsyncCoroMethod(coro: CoroutineFunction) -> CoroutineMethod:
    """
    Converts an Async Coroutine to Async Class Function
    """
    @wraps(coro)
    async def method(self, *args, **kwargs) -> Any:
        return await coro(*args, **kwargs)
    return method


def SyncFunction(func: CoroutineFunction):
    """
    Converts an Async Function to a Sync Function
    """
    @wraps(func)
    def wrapped_func(*args, **kwargs) -> Any:
        if _LOOP_ACTIVE(): 
            return run_async_asyncio(func, *args, **kwargs)
        return run_async_anyio(func, *args, **kwargs)
    return wrapped_func




__all__ = (
    'run_sync',
    'to_thread',
    'run_async',
    'run_async_anyio',
    'run_async_asyncio',
    'asyncio_run',
    'asyncio_gather',
    'SyncFunction',
    'AsyncFunction',
    'AsyncClassFunction',
    'AsyncMethod',

    'AsyncCoroMethod',
    'CoroutineResult',
    'CoroutineFunction',
    'CoroutineMethod',
    'tqdm',
    'trange',
    '_tqdm_enabled',
    '_async_backend',
    'iscoroutinefunction',
    'wrap_tqdm_iterable',
    
)
