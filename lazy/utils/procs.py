import os
import sys
import subprocess
import logging
import platform
from pathlib import Path
from typing import Union, List, Type

from logz import get_logger

logger = get_logger('procs')


"""
Deprecated Exec Functions
"""
def exec_cmd(cmd, raise_error: bool = True):
    try:
        out = subprocess.check_output(cmd, shell=True)
        if isinstance(out, bytes): out = out.decode('utf8')
        return out.strip()
    except Exception as e:
        if not raise_error: return ""
        raise e

def exec_daemon(cmd: Union[List[str], str], stdout = subprocess.PIPE, stderr = subprocess.STDOUT, set_proc_uid: bool = False, *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    if set_proc_uid and platform.system() != 'Darwin': return subprocess.Popen(cmd, stdout = stdout, stderr = stderr, preexec_fn = lambda: os.setuid(1), *args, **kwargs)
    return subprocess.Popen(cmd, stdout = stdout, stderr = stderr, *args, **kwargs)

def exec_run(cmd: Union[List[str], str], *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    return subprocess.run(cmd, *args, **kwargs)

def exec_out(cmd: Union[List[str], str], shell: bool = True, *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    return subprocess.check_output(cmd, shell = shell, *args, **kwargs)


def exec_shell(cmd): return os.system(cmd)