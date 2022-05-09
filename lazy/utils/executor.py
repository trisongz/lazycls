import os
import anyio
import asyncio
import functools
import subprocess

from lazy.types import *

from concurrent import futures
from anyio._core._eventloop import get_asynclib, threadlocals

from .helpers import is_coro_func

DEFAULT_SHELL = os.getenv('DEFAULT_SHELL', "/bin/bash")
MAX_WORKERS = int(os.getenv('EXECUTOR_MAX_WORKERS', '8'))


class Executor:
    pool: futures.ThreadPoolExecutor = None

    @staticmethod
    def is_coro(func: Union[Callable, Coroutine, Any], func_name: str = None) -> bool:
        return is_coro_func(func, func_name)

    @classmethod
    def init_pool(cls):
        if cls.pool: return
        cls.pool = futures.ThreadPoolExecutor(max_workers = MAX_WORKERS)
    
    @classmethod
    def get_pool(cls) -> futures.ThreadPoolExecutor:
        cls.init_pool()
        return cls.pool

    @classmethod
    def get_async_module(cls):
        return getattr(threadlocals, "current_async_module", None)

    @classmethod
    async def run_as_async(cls, sync_func: Callable, *args, **kwargs):
        """
        Turns a Sync Function into an Async Function        
        """
        blocking = functools.partial(sync_func, *args, **kwargs)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(cls.get_pool(), blocking)
    
    @classmethod
    def run_as_sync(cls, async_func: Coroutine, *args, **kwargs):
        """
        Turns an Async Function into a Sync Function
        """
        current_async_module = cls.get_async_module()
        partial_f = functools.partial(async_func, *args, **kwargs)
        if current_async_module is None:
            return anyio.run(partial_f)
        return anyio.from_thread.run(partial_f)

    @classmethod
    def wrap_as_async(cls, sync_func: Callable):
        """
        Turns an Async Function into a Sync Function
        """
        @functools.wraps(sync_func)
        async def wrapper(*args, **kwargs) -> Any:
            return await cls.run_as_async(sync_func, *args, **kwargs)
        return wrapper

    @classmethod
    def wrap_as_sync(cls, async_func: Coroutine):
        """
        Turns an Async Function into a Sync Function
        """
        @functools.wraps(async_func)
        def wrapper(*args, **kwargs) -> Any:
            current_async_module = cls.get_async_module()
            partial_f = functools.partial(async_func, *args, **kwargs)
            if current_async_module is None:
                return anyio.run(partial_f)
            return anyio.from_thread.run(partial_f)
        return wrapper

    @classmethod
    def subproc(cls, cmd: Union[List[str], str], shell: bool = True, default_shell: str = DEFAULT_SHELL, *args, **kwargs) -> subprocess.CompletedProcess:
        #if isinstance(cmd, str): cmd = shlex.split(cmd)
        cmd = [default_shell, "-c"] + cmd
        return subprocess.run(cmd, shell=shell, *args, **kwargs)

    


