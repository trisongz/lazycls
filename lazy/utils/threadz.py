
################################################################
###           Background Workers using Threads               ###
################################################################

import os
import sys
import signal
import threading
import anyio

from functools import wraps, partial
from threading import Thread, Lock, RLock
from typing import Callable, Dict, List, Any, Union, Iterable, Generator, Awaitable, Optional, Coroutine
from .wrapz import iscoroutinefunction, sync_to_async_wrap, async_run
from .helpers import get_logger

try:
    import trio
    _async_backend = 'trio'

except ImportError:
    _async_backend = 'asyncio'

logger = get_logger('lazy:threadz')


class Threadz(Thread):
    def __init__(self, func, *args, callback: Optional[Callable] = None, threadz_id: int = None, daemon: bool = None, **kwargs):
        self.func = func
        self.callback = callback
        self.is_daemon = daemon
        self.threadz_id = threadz_id
        self.args = args
        self.kwargs = kwargs
        self.threadz_name =  f'{self.func.__name__}_{threadz_id}' if threadz_id is not None else self.func.__name__
        self.result = None
        super().__init__(daemon = daemon, name = self.threadz_name)

    @property
    def function_name(self): 
        return self.func.__name__

    def run(self):
        if not iscoroutinefunction(self.func):
            self.result = self.func(*self.args, **self.kwargs)
        else:
            try:
                partial_func = partial(self.func, *self.args, **self.kwargs)
                self.result = anyio.run(partial_func, backend=_async_backend)
            except RuntimeError:
                coro = self.func(*self.args, **self.kwargs)
                self.result = async_run(coro)            
            except Exception as e:
                logger.error(f'Unable to Complete Run for {self.function_name}:{self.pid}. Error: {e}')
                
        if self.callback: self.callback(self.result)



class ThreadzManager:
    lock: RLock = RLock()
    threadz: Dict[str, Threadz] = {} # need to figure a way to limit it?

    @classmethod
    def make_threadz(cls, func, *args, callback: Optional[Callable] = None, daemon: bool = None, **kwargs) -> Threadz:
        threadz_id = len(cls.threadz) # temp
        #cls.lock.acquire()
        #with cls.lock:
        if cls.threadz.get(func.__name__):
            logger.warning(f'Got Existing Thread for {func.__name__}. Terminating')
            cls.threadz[func.__name__].join()
            cls.threadz[func.__name__]._stop()

        threadz = Threadz(func, *args, callback = callback, threadz_id = threadz_id, deamon = daemon, **kwargs)
        cls.threadz[func.__name__] = threadz
        return threadz
    
    @classmethod
    def kill_all(cls):
        logger.warning(f'Attempting to Kill {len(cls.threadz)} Threadz')
        for threadz in list(cls.threadz.values()):
            try:
                threadz._stop()
                logger.info(f'Successfully Killed {threadz.threadz_name}')
                _ = cls.threadz.pop(threadz.function_name, None)
            except Exception as e:
                logger.error(f'Error Killing {threadz.threadz_name}: {e}')
        logger.warning(f'Remaining Threadz: {len(cls.threadz)}')


def background_thread_wrap(func: Union[Callable, Coroutine]) -> Threadz:
    @wraps(func)
    def background_func(*args, **kwargs) -> Any:
        threadz = ThreadzManager.make_threadz(func, *args, daemon = True, **kwargs)
        threadz.start()
        return threadz
    return background_func








