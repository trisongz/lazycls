"""
Performance Utilz
"""
import time
import anyio
import multiprocessing
from functools import wraps, partial

from typing import Callable, Dict, List, Any, Union, Iterable, Generator, Optional


from .utils import *
from ..helpers import get_logger

try:
    import trio
    _async_backend = 'trio'

except: _async_backend = 'asyncio'

#except ImportError:
#    _async_backend = 'asyncio'

"""
Some Base Variables that don't rely on other 
modules from lazy to ensure no circular dependencies
"""


logger = get_logger('lazy:ops')

NUM_CPUS = multiprocessing.cpu_count()
MAX_THREADS = round(NUM_CPUS * 0.7)
MAX_PROCESSES = round(NUM_CPUS * 0.5)
MAX_WORKERS = round(NUM_CPUS * 0.5)


def get_mp():
    """
    Gets the correct Multiprocessing Library for win/mac/linux
    """
    return (multiprocessing.get_context('fork') if hasattr(multiprocessing, 'get_context') else multiprocessing)



def ProcessPool(process: Callable, workers: int = MAX_WORKERS, has_args: bool = False, has_kwargs: bool = False, verbose: bool = False, timed: bool = False, show_progress: bool = False):
    # sourcery no-metrics
    """
    Creates a wrapper function that leverages
    multiprocessing to pass all items yielded by this
    function to the [process]

    :process                = Callable
    :workers                = num of max workers
    :has_args [False]       = if the returned result is a list, will pass as *args if True
    :has_kwargs [False]     = if the returned result is a dict, will pass as **kwargs if True
    :verbose [False]        = Will log start/stop
    :timed  [False]         = Measures the time of function call.
    :show_progress [False]  = Displays a Progress bar (tqdm) if available
    """

    def put_to_queue(input_queue: multiprocessing.Queue, wrapped_func: Union[List[Any], Dict[Any, Any], Callable, Generator, Iterable, Any]):
        if isinstance(wrapped_func, Callable):
            for item in wrapped_func():
                input_queue.put(item)

        else:
            for item in wrapped_func:
                input_queue.put(item)

        for _ in range(workers):
            input_queue.put(None)
    
    def process_item(input_queue: multiprocessing.Queue, output_queue: multiprocessing.Queue):
        while True:
            item = input_queue.get()
            if item is None:
                output_queue.put(item)
                break
            if has_kwargs and isinstance(item, dict):
                output_queue.put(process(**item))
            elif has_args and isinstance(item, list):
                output_queue.put(process(*item))
            else:
                output_queue.put(process(item))

    def wrapped_generator(func):
        
        @wraps(func)
        def process_pool(*args, **kwargs):
            mp = get_mp()
            input_queue = mp.Queue(maxsize=workers)
            output_queue = mp.Queue(maxsize=workers)

            wrapped_func = partial(func, *args, **kwargs)
            _generator_pool = mp.Pool(1, initializer=put_to_queue, initargs=(input_queue, wrapped_func))
            _process_pool = mp.Pool(workers, initializer=process_item, initargs=(input_queue, output_queue))
            if verbose: logger.info(f'Starting ProcessPool for Func: {func.__name__} with {workers} workers. Timed = {timed}')
            ts = time.perf_counter() if timed else None
            pbar: Optional['trange'] = wrap_tqdm_iterable(wrapped_func, desc = f'ProcessPool: {func.__name__}') if show_progress and _tqdm_enabled else None
            workers_done = 0
            while True:
                item = output_queue.get()
                if item is None:
                    workers_done += 1
                    if workers_done == workers:
                        break
                else:
                    if pbar: pbar.update()
                    yield item
                    
            _generator_pool.close()
            _generator_pool.join()
            _process_pool.close()
            _process_pool.join()

            if timed:
                done_time = time.perf_counter() - ts
                if verbose: logger.info(f'Completed ProcessPool for Func: {func.__name__} with {workers} workers in {done_time:.2f} secs')
                return done_time
            if verbose: logger.info(f'Completed ProcessPool for Func: {func.__name__} with {workers} workers')
        
        return process_pool
        
    return wrapped_generator


    
def AsyncTaskGroup(process: Callable, verbose: bool = False, timed: bool = False, show_progress: bool = False):
    """
    Creates an async wrapper function that leverages
    anyio.create_task_group to pass all items yielded by this
    function to the [process]

    :process                = Callable. Will convert to async function.
    :verbose [False]        = Will log start/stop
    :timed  [False]         = Measures the time of function call.
    :show_progress [False]  = Displays a Progress bar (tqdm) if available / Not used atm
    """

    async def task_group_process(wrapped_func):
        process_func = process
        if not iscoroutinefunction(process_func):
            process_func = AsyncFunction(process_func)

        async with anyio.create_task_group() as tg:
            for item in await wrapped_func():
                tg.start_soon(process_func, item)

    def wrapped_task_group(func):
        @wraps(func)
        def async_task_group(*args, **kwargs):
            if not iscoroutinefunction(func): 
                wrapped_func = AsyncFunction(func)
            else: 
                wrapped_func = partial(func, *args, **kwargs)

            if verbose: logger.info(f'Starting AsyncTaskGroup for Func: {func.__name__}. Timed = {timed}')
            ts = time.perf_counter() if timed else None
            anyio.run(task_group_process, wrapped_func, backend=_async_backend)
            if timed:
                done_time = time.perf_counter() - ts
                if verbose: logger.info(f'Completed AsyncTaskGroup for Func: {func.__name__} in {done_time:.2f} secs')
                return done_time
            if verbose: logger.info(f'Completed AsyncTaskGroup for Func: {func.__name__}')

        return async_task_group
        
    return wrapped_task_group

