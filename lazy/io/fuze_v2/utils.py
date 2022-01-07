import os
import time
import threading
import multiprocessing
import platform
import subprocess

from typing import Union
from lazy.utils import _get_logger

logger = _get_logger('Fuze')

def get_multiproc():
    # We can't use forkserver because we have to make sure
    # that the server inherits the per-test stdout/stderr file
    # descriptors.
    if hasattr(multiprocessing, 'get_context'): mp = multiprocessing.get_context('fork')
    else: mp = multiprocessing
    #if threading.active_count() != 1: raise RuntimeError("Multi-threaded test running is not supported")
    if threading.active_count() != 1: pass
    return mp


def exitcode(process: Union[subprocess.Popen, multiprocessing.Process]):
    if isinstance(process, subprocess.Popen): return process.poll()
    if process.is_alive(): return None
    else: return process.exitcode

def wait_for(callable, timeout=10, interval=0.1):
    '''Wait until *callable* returns something True and return it
    If *timeout* expires, return None
    '''

    waited = 0
    while True:
        ret = callable()
        if ret: return ret
        if waited > timeout: return None
        waited += interval
        time.sleep(interval)



def cleanup_fuze(mount_process: Union[subprocess.Popen, multiprocessing.Process], mnt_dir: str):
    if platform.system() == 'Darwin':
        subprocess.call(['umount', '-l', mnt_dir], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    else:
        subprocess.call(['fusermount', '-z', '-u', mnt_dir], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    mount_process.terminate()
    if isinstance(mount_process, subprocess.Popen):
        try: mount_process.wait(1)
        except subprocess.TimeoutExpired: mount_process.kill()
    else:
        mount_process.join(5)
        if mount_process.exitcode is None: mount_process.kill()


def umount(mnt_dir: str, mount_process: Union[subprocess.Popen, multiprocessing.Process] = None):
    if platform.system() == 'Darwin': subprocess.check_call(['umount', '-l', mnt_dir])
    else: subprocess.check_call(['fusermount', '-z', '-u', mnt_dir])
    assert not os.path.ismount(mnt_dir)
    if not mount_process: return

    if isinstance(mount_process, subprocess.Popen):
        try:
            code = mount_process.wait(5)
            if code == 0: return
            logger.error('file system process terminated with code %s' % (code,))
        except subprocess.TimeoutExpired:
            mount_process.terminate()
            try: mount_process.wait(1)
            except subprocess.TimeoutExpired: mount_process.kill()
    else:
        mount_process.join(5)
        code = mount_process.exitcode
        if code == 0: return
        elif code is None:
            mount_process.terminate()
            mount_process.join(1)
        else:
            logger.error('file system process terminated with code %s' % (code,))

    logger.error('mount process did not terminate')