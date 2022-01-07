import os
import signal
import threading
import subprocess
import multiprocessing

from typing import Union
from types import ModuleType

try: import psutil
except: psutil: ModuleType = None


def get_mp(require_singlethread: bool = False):
    # We can't use forkserver because we have to make sure
    # that the server inherits the per-test stdout/stderr file
    # descriptors.
    if hasattr(multiprocessing, 'get_context'): mp = multiprocessing.get_context('fork')
    else: mp = multiprocessing
    if require_singlethread and threading.active_count() != 1: raise RuntimeError("Single-threaded is required")
    return mp

def exitcode(process: Union[subprocess.Popen, multiprocessing.Process]):
    if isinstance(process, subprocess.Popen): return process.poll()
    if process.is_alive(): return None
    else: return process.exitcode

def process_kill(pid: int, sig: int = signal.SIGTERM):
    """
    Used to kill a running process
    """
    os.kill(pid, sig)

def process_alive(pid: int) -> bool:
    """
    Used to check if a process is running
    """
    global psutil
    if not psutil:
        from lazy.libz import Lib
        psutil = Lib['psutil']
    return psutil.pid_exists(pid)


__all__ = (
    get_mp,
    exitcode,
    process_kill,
    process_alive
)