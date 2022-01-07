
import os
import sys
import signal
import shlex
import threading
import subprocess
import multiprocessing

from typing import Union, List
from lazy.types import classproperty
from lazy.io.pathz import get_path, PathLike, get_lazydir
from lazy.io.cachez import Cache
from .utils import *

runz_dir: PathLike = None #get_lazydir(True).joinpath('runz')
runzCache: Cache = None #  = Cache(directory = )

def get_runzcache():
    global runzCache, runz_dir
    if runzCache is None:
        runz_dir = get_lazydir(True).joinpath('runz')
        runzCache = Cache(directory = runz_dir.as_posix())
    return runzCache


class SubProcArgs:
    base = {'stdout': subprocess.PIPE, 'stderr': subprocess.STDOUT}
    win = {'creationflags': subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP}
    darwin = {'preexec_fn': os.setpgrp}
    linux = {}
    
    @classmethod
    def get_host_args(cls):
        if sys.platform.startswith('win'): return cls.win
        if sys.platform.startswith('darwin'): return cls.darwin
        return cls.linux

    @classmethod
    def get_background_args(cls, **kwargs):
        args = cls.get_host_args()
        args.update(cls.base)
        if kwargs: args.update(kwargs)


class RunzType(type):

    @classmethod
    def run_system(cls, cmd: str):
        """
        alias for os.system(cmd)
        """
        return os.system(cmd)

    @classmethod
    def run_cmd(cls, cmd: Union[List[str], str], shell: bool = True, raise_error: bool = True, **kwargs):
        """
        Uses subprocess.check_output(cmd, shell=shell, **kwargs)
        """
        if isinstance(cmd, str): cmd = shlex.split(cmd)
        try:
            out = subprocess.check_output(cmd, shell=shell, **kwargs)
            if isinstance(out, bytes): out = out.decode('utf8')
            return out.strip()
        except Exception as e:
            if not raise_error: return ""
            raise e
    
    @classmethod
    def sp_run(cls, cmd: Union[List[str], str], shell: bool = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, **kwargs):
        """
        Uses subprocess.run(cmd, shell=shell, stdout=stdout, stderr=stderr, **kwargs)
        """
        if isinstance(cmd, str): cmd = shlex.split(cmd)
        return subprocess.run(cmd, shell=shell, stdout=stdout, stderr=stderr, **kwargs)
    
    @classmethod
    def sp_output(cls, cmd: Union[List[str], str], shell: bool = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, **kwargs):
        """
        Uses subprocess.check_output(cmd, shell=shell, stdout=stdout, stderr=stderr, **kwargs)
        """
        if isinstance(cmd, str): cmd = shlex.split(cmd)
        return subprocess.check_output(cmd, shell=shell, stdout=stdout, stderr=stderr, **kwargs)
    

    @classmethod
    def sp_open(cls, cmd: Union[List[str], str], shell: bool = True, stdout = subprocess.PIPE, stderr = subprocess.STDOUT, new_process: bool = False, **kwargs):
        """
        Uses subprocess.Popen(cmd, shell=shell, stdout=stdout, stderr=stderr, **kwargs)

        if new_process, sets the args for the host system to run in an independent process.
        """
        if isinstance(cmd, str): cmd = shlex.split(cmd)
        args = SubProcArgs.get_host_args() if new_process else {}
        if kwargs: args.update(kwargs)
        return subprocess.Popen(cmd, shell=shell, stdout=stdout, stderr=stderr, **args)
    

    @classmethod
    def exec_sed(path: Union[str, PathLike], changes: List[set], new_path: Union[str, PathLike] = None, chmod: int = None) -> PathLike:
        """ Does a cleaner `sed` command 
            - path: path to file
            - changes: List of {find, replace, n_times}
            - new_path: new filepath, otherwise will write to original
            - chmod: int 
        """
        f = get_path(path)
        txt = f.read_text(encoding='utf-8')
        for c in changes:
            txt = txt.replace(str(c[0]), str(c[1]), c[2]) if len(c) == 3 else txt.replace(str(c[0]), str(c[1]))
        if new_path: f = get_path(new_path)
        f.write_text(txt, encoding='utf-8')
        if chmod: f.chmod(chmod)
        return f
    

    
    

    