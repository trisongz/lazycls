import anyio
import inspect
from collections import deque
from anyio.to_thread import run_sync
from functools import wraps, partial
from pydantic import BaseModel
from typing import Callable, Dict, List, Any, Union, Iterable, Generator, Awaitable, Optional, Coroutine

from .loops import *

try:
    from tqdm.auto import tqdm, trange
    from tqdm.asyncio import trange as async_trange
    from tqdm.asyncio import tqdm as async_tqdm

    _tqdm_enabled = True
except ImportError:
    tqdm, trange, async_trange, async_tqdm = object, object, object, object
    _tqdm_enabled = False



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
    return await run_sync(func_kwargs, *args)


async def to_thread_cls(func: Callable, cls, *args, **kwargs) -> Any:
    """
    Specific Async wrapper for Classes
    // post really need this.
    """
    func_kwargs = partial(func, cls, **kwargs)
    return await run_sync(func_kwargs, *args)


def sync_to_async_wrap(func: Callable) -> CoroutineFunction:
    @wraps(func)
    async def new_func(*args, **kwargs) -> Any:
        return await to_thread(func, *args, **kwargs)
    return new_func


def get_sync_run_func_anyio(async_func, *args, **kwargs):
    """
    Returns the result of the coroutine using anyio
    in sync calls
    """
    current_async_module = getattr(anyio._core._eventloop.threadlocals, "current_async_module", None)
    partial_func = partial(async_func, *args, **kwargs)
    if current_async_module is None:
        return anyio.run(partial_func)
    return anyio.from_thread.run(partial_func)

def get_sync_run_func_asyncio(async_func, *args, **kwargs):
    """
    Returns the result of the coroutine using asyncio
    in sync calls
    """
    coro = async_func(*args, **kwargs)
    return async_run(coro)

def async_to_sync_wrap(async_func: Coroutine):
    """
    Transforms an async function to sync
    """
    @wraps(async_func)
    def wrapped_func(*args, **kwargs) -> Any:
        if _LOOP_ACTIVE(): return get_sync_run_func_asyncio(async_func, *args, **kwargs)
        return get_sync_run_func_anyio(async_func, *args, **kwargs)
    return wrapped_func


def async_to_sync_func(async_func: Coroutine, *args, **kwargs):
    """
    Returns the result of the coroutine using asyncio
    in sync calls
    """
    if _LOOP_ACTIVE(): return get_sync_run_func_asyncio(async_func, *args, **kwargs)
    return get_sync_run_func_anyio(async_func, *args, **kwargs)

    
def func_as_method_coro(func: Callable) -> CoroutineMethod:
    @wraps(func)
    async def method(self, *args, **kwargs) -> Any:
        return await to_thread(func, *args, **kwargs)
    return method

def coro_as_method_coro(coro: CoroutineFunction) -> CoroutineMethod:
    @wraps(coro)
    async def method(self, *args, **kwargs) -> Any:
        return await coro(*args, **kwargs)
    return method

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
    

async def async_wrap_tqdm_iterable(async_func: Coroutine, desc: str = None, leave: bool = False, **config) -> trange:
    """
    Assumes that the func is already wrapped.
    May have some overhead.
    """
    #_func = sync_to_async_func(func) if not iscoroutinefunction(func) else func
    #_func_length = count_iterable(anyio.run_async_from_thread(func)) if iscoroutinefunction(func) else count_iterable(get_async_run_func(func))
    #return trange(_func_length, desc = desc, leave = leave, **config)
    iter_length = await async_count_iterable(async_func)
    return trange(iter_length, desc = desc, leave = leave, **config)

## This will be what is prefixed to the class
ASYNC_WRAPZ_PREFIX = 'async_'

def set_async_wrapz_prefix(prefix: str = ASYNC_WRAPZ_PREFIX):
    global ASYNC_WRAPZ_PREFIX
    ASYNC_WRAPZ_PREFIX = prefix


def async_class_wrap(func):
    """
    Wrapz that uses the default async prefix of 'async_'

    @wrapz.AsyncClassFunc
    def class_func(self, *args, **kwargs):
        ...
    
    will create a new the global classfunction

    async def async_class_func(self, *args, **kwargs):
        ...
    
    
    """
    @wraps(func)
    def wrapper_func(self, *args, **kwargs):
        async_func_name = ASYNC_WRAPZ_PREFIX + func.__name__
        cls = globals()[self.__class__.__name__]

        if not hasattr(cls, async_func_name):
            async def async_cls_func(*args, **kwargs):
                cls_func = getattr(cls, func.__name__)
                return await to_thread(cls_func.__call__, *args, **kwargs)
            
            if issubclass(cls, BaseModel):
                cls.__private_attributes__[async_func_name] = None
            setattr(cls, async_func_name, async_cls_func)
            
        if not hasattr(self, async_func_name):
            async def async_self_func(*args, **kwargs):
                return await to_thread(func.__call__, self, *args, **kwargs)
            
            setattr(cls, async_func_name, async_self_func)        
        return func.__call__(self, *args, **kwargs)    
    return wrapper_func


def async_prefix_class_wrap(prefix: str = ASYNC_WRAPZ_PREFIX):
    """
    Wrapz that allows specifying the async prefix, which defaults to 'async_'
    Must call this method when setting the decorator

    @wrapz.AsyncClassFuncPrefix(prefix = '_async_')
    def class_func(self, *args, **kwargs):
        ...
    
    will create a new the global classfunction
    
    async def _async_class_func(self, *args, **kwargs):
        ...
    """
    def wrapped_inner(func):
        @wraps(func)
        def wrapper_func(self, *args, **kwargs):
            async_func_name = prefix + func.__name__
            cls = globals()[self.__class__.__name__]

            if not hasattr(cls, async_func_name):
                async def async_cls_func(*args, **kwargs):
                    cls_func = getattr(cls, func.__name__)
                    return await to_thread(cls_func.__call__, *args, **kwargs)
                
                if issubclass(cls, BaseModel):
                    cls.__private_attributes__[async_func_name] = None
                setattr(cls, async_func_name, async_cls_func)
                
            if not hasattr(self, async_func_name):
                async def async_self_func(*args, **kwargs):
                    return await to_thread(func.__call__, self, *args, **kwargs)
                
                setattr(cls, async_func_name, async_self_func)        
            return func.__call__(self, *args, **kwargs)    
        return wrapper_func
    return wrapped_inner



def sync_class_wrap(func):
    """
    Wrapz that creates the sync name assuming the default async prefix of 'async_'

    @wrapz.SyncClassFunc
    async def async_class_func(self, *args, **kwargs):
        ...
    
    will create a new the global classfunction

    def class_func(self, *args, **kwargs):
        ...
    
    """
    @wraps(func)
    def wrapper_func(self, *args, **kwargs):
        if func.__name__.startswith(ASYNC_WRAPZ_PREFIX):
            sync_func_name = func.__name__.replace(ASYNC_WRAPZ_PREFIX, '')

        elif func.__name__.startswith("_"):
            sync_func_name = func.__name__.replace('_', '')
        
        else:
            sync_func_name = 'sync_' + func.__name__

        cls = globals()[self.__class__.__name__]

        if not hasattr(cls, sync_func_name):
            def sync_cls_func(*args, **kwargs):
                cls_func = getattr(cls, func.__name__)
                return async_to_sync_func(cls_func.__call__, *args, **kwargs)
            
            if issubclass(cls, BaseModel):
                cls.__private_attributes__[sync_func_name] = None
            setattr(cls, sync_func_name, sync_cls_func)
            
        if not hasattr(self, sync_func_name):
            def sync_self_func(*args, **kwargs):
                return async_to_sync_func(func.__call__, self, *args, **kwargs)
            
            setattr(cls, sync_func_name, sync_self_func)        
        return func.__call__(self, *args, **kwargs)    
    return wrapper_func

def sync_prefix_class_wrap(prefix: str = ASYNC_WRAPZ_PREFIX, replacement: str = ''):
    def sync_wrapped_func(func):
        """
        Wrapz that creates the sync name assuming splitting on the prefix
        and replacing with replacement: str

        the default async prefix of 'async_'

        @wrapz.AsyncClassFuncPrefix(prefix = '_async_', replace='sync_')
        async def _async_class_func(self, *args, **kwargs):
            ...
        
        will create a new the global classfunction

        def sync_class_func(self, *args, **kwargs):
            ...
        
        """
        @wraps(func)
        def wrapper_func(self, *args, **kwargs):
            if func.__name__.startswith(prefix):
                sync_func_name = func.__name__.replace(prefix, replacement)
            
            else:
                sync_func_name = replacement + func.__name__

            cls = globals()[self.__class__.__name__]

            if not hasattr(cls, sync_func_name):
                def sync_cls_func(*args, **kwargs):
                    cls_func = getattr(cls, func.__name__)
                    return async_to_sync_func(cls_func.__call__, *args, **kwargs)
                
                if issubclass(cls, BaseModel):
                    cls.__private_attributes__[sync_func_name] = None
                setattr(cls, sync_func_name, sync_cls_func)
                
            if not hasattr(self, sync_func_name):
                def sync_self_func(*args, **kwargs):
                    return async_to_sync_func(func.__call__, self, *args, **kwargs)
                
                setattr(cls, sync_func_name, sync_self_func)        
            return func.__call__(self, *args, **kwargs)    
        return wrapper_func
    return sync_wrapped_func



## Runs the function itself as sync <-> async
run_async_from_sync = async_to_sync_func
run_sync_from_async = to_thread

## Wraps the target function from/to async <-> sync
SyncWrap = async_to_sync_wrap
AsyncWrap = sync_to_async_wrap

#sync_to_async_func = sync_to_async_wrap

## Converts the Class method/function from/to async <-> sync
AsyncClassFunc = async_class_wrap
AsyncClassFuncPrefix = async_prefix_class_wrap
SyncClassFunc = sync_class_wrap
SyncClassFuncPrefix = sync_prefix_class_wrap




__all__ = (
    'anyio',
    'wraps',
    'partial',
    'run_sync',
    'CoroutineResult',
    'CoroutineFunction',
    'CoroutineMethod',
    'tqdm',
    'trange',
    '_tqdm_enabled',
    'iscoroutinefunction',
    'to_thread',
    'async_to_sync_wrap',
    #'sync_to_async_func',
    'sync_to_async_wrap',
    'async_to_sync_func',
    'func_as_method_coro',
    'coro_as_method_coro',
    'count_iterable',
    'wrap_tqdm_iterable',
    'SyncClassFunc',
    'SyncClassFuncPrefix',
    'AsyncClassFunc',
    'AsyncClassFuncPrefix',
    'run_async_from_sync',
    'run_sync_from_async',
    'SyncWrap',
    'AsyncWrap',
)
