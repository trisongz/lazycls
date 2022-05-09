import sys
import signal
import threading
from typing import Union, List, Callable

class ProtectedRuntime:
    exit_funcs: List[Callable] = []

    """ Protect a piece of code from being killed by SIGINT or SIGTERM.
    It can still be killed by a force kill.
    Example:
        with ProtectedRuntime():
            ProtectedRuntime.add_func(foo)
            run_func_1()

    Both functions will be executed even if a sigterm or sigkill has been received.
    """
    def __init__(self):
        self.killed = False
    
    @classmethod
    def add_func(cls, func: Callable):
        cls.exit_funcs.append(func)

    @property
    def alive(self):
        return not self.killed

    def _handler(self, signum, frame):
        from lazy.utils import logger
        logger.error("Runtime Client Received SIGINT or SIGTERM!")
        if self.exit_funcs:
            for func in self.exit_funcs:
                func()
        self.killed = True


    def __enter__(self):
        # self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        # self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)
        if threading.current_thread() is threading.main_thread():
            self.old_sigint = signal.signal(signal.SIGINT, self._handler)
            self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)
        return self


    def __exit__(self, type, value, traceback):
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.old_sigint)
            signal.signal(signal.SIGTERM, self.old_sigterm)
        else:
            signal.signal(signal.SIGINT, self._handler)
            signal.signal(signal.SIGTERM, self._handler)
        # signal.signal(signal.SIGINT, self.old_sigint)
        # signal.signal(signal.SIGTERM, self.old_sigterm)
        if self.killed:
            sys.exit(0)

