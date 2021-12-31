import asyncio
import functools
import contextvars


async def _to_thread(func, *args, **kwargs):
    """Asynchronously run function *func* in a separate thread.
    Any *args and **kwargs supplied for this function are directly passed
    to *func*. Also, the current :class:`contextvars.Context` is propogated,
    allowing context variables from the main thread to be accessed in the
    separate thread.
    Return a coroutine that can be awaited to get the eventual result of *func*.
    """
    loop = asyncio.events.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)


def asyncify(obj, template: str = "async_[func_name]"):
    """
    Creates async function attributes for a class
    using the template
    i.e. obj.norm_call [sync] -> obj.async_norm_call [async]
    returns obj.
    
    AsyncCls = asyncify(SyncCls)

    a = AsyncCls.norm_call()
    x = await AsyncCls.async_norm_call()
    
    """
    def create_async_func(base_func):
        async def async_func(*args, **kwargs):
            return await _to_thread(base_func, *args, **kwargs)
        return async_func

    for attr in dir(obj):
        attr_val = getattr(obj, attr)
        if callable(attr_val):
            if not attr.startswith('_'):
                async_func = create_async_func(attr_val)
                setattr(obj, template.replace('[func_name]', attr), async_func)
        
    return obj