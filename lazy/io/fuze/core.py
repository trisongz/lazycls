

""" 
Requires fusepy and fuse
- does not support win
- uses multiprocess instead of multiprocessing
"""
import abc
import os
import sys
import signal
import pathlib
from importlib import import_module
from fsspec import AbstractFileSystem
from fsspec.asyn import AsyncFileSystem
from fsspec.fuse import run as run_fuze
from lazy.libz import Lib
from typing import List, Dict, Type, Tuple, Any, Optional, Union
from types import ModuleType

from logz import get_logger
#try: from multiprocess import Process
#except ImportError: 
import multiprocessing as mp
#from multiprocessing import Process

## Keep track of all mounted
_ALL_FUZES: Dict[str, Type[mp.Process]] = {}

_FUZE_READY: bool = False
_FUZE_ALLOWED: bool = not sys.platform.startswith('win')

FuseSystemType = Union[Type[AbstractFileSystem], Type[AsyncFileSystem]]
#FuseSystemType = tuple(Type[AbstractFileSystem], Type[AsyncFileSystem])
logger = get_logger('fuze')

def _prepare_fuze():
    global _FUZE_READY
    assert _FUZE_ALLOWED, 'Windows is not supported'
    if _FUZE_READY: return
    Lib.import_cmd('fuse')
    Lib._ensure_lib_installed('fusepy')
    Lib.import_lib('multiprocess')
    _FUZE_READY = True

def _add_proc(path: str, proc: Type[mp.Process]):
    global _ALL_FUZES
    _ALL_FUZES[path] = proc

def _kill_proc(path: str, timeout: int = 5):
    global _ALL_FUZES
    if _ALL_FUZES.get(path):
        proc = _ALL_FUZES.pop(path)
        os.kill(proc.pid, signal.SIGKILL)
        proc.join(timeout=timeout)

_PROCESS_SET: bool = False

def _get_process():
    global _PROCESS_SET
    if not _PROCESS_SET: 
        if sys.platform.startswith('darwin'): mp.set_start_method('spawn')
        _PROCESS_SET = True
    return mp.Process


try: from lazy import CloudAuthz
except ImportError: CloudAuthz = object

_authz: CloudAuthz = None

def get_cloudauthz():
    global _authz
    if _authz is None:
        from lazy import CloudAuthz
        _authz = CloudAuthz
    return _authz

"""
Will use MemoryFileSystem as default
"""

class BaseFuzerCls:
    """
    Class that manages filesystem fuse mounts
    supports gcs/s3(?)
    """
    _FSX: ModuleType = None
    _FSX_LIB: str = 'fsspec'
    _FSX_MODULE: Optional[str] = 'fsspec.implementations.memory'
    _FSX_CLS: str = 'MemoryFileSystem'
    _AUTHZ: 'CloudAuthz' = None

    @classmethod
    def _ensure_lib(cls, *args, **kwargs):
        if cls._FSX is not None: return
        cls._FSX = Lib.import_lib(cls._FSX_LIB)
        if cls._FSX_MODULE: cls._FSX = import_module(cls._FSX_MODULE, package=cls._FSX_LIB)

    @classmethod
    def get_authz(cls, reload: bool = False, **config):
        if cls._AUTHZ and not reload: return cls._AUTHZ
        cls._AUTHZ = get_cloudauthz()
        if config: cls._AUTHZ.update_authz(**config)
        return cls._AUTHZ

    @classmethod
    def get_configz(cls, reload: bool = False, **config):
        return {}

    @classmethod
    def get_filesystem(cls, fs_args: Dict[str, Any] = {}, *args, **kwargs) -> FuseSystemType:
        cls._ensure_lib()
        configz = cls.get_configz(*args, **kwargs)
        if fs_args: configz.update(fs_args)
        return getattr(cls._FSX, cls._FSX_CLS)(**configz)
    

    @classmethod
    def _run_fuze(cls, source: str, mount_path: str, ready_file: bool = True, foreground: bool = False, threads: bool = True, fs_args: Dict[str, Any] = {}, *args, **kwargs):
        p = pathlib.Path(mount_path).resolve(True)
        p.mkdir(parents=True, exist_ok=True)
        fs = cls.get_filesystem(fs_args, *args, **kwargs)
        run_fuze(fs, source, p.as_posix(), ready_file=ready_file, foreground=foreground, threads=threads)


    @classmethod
    def mount(cls, source: str, mount_path: str, ready_file: bool = True, foreground: bool = False, threads: bool = False, fs_args: Dict[str, Any] = {}, daemon: bool = False, *args, **kwargs) -> None:
        assert mount_path not in _ALL_FUZES, f'{mount_path} is already mounted from {source}'
        _prepare_fuze()
        #from multiprocess import Process
        logger.info('starting mount')
        mount_proc = _get_process()(target=cls._run_fuze, args=(source, mount_path, ready_file, foreground, threads, fs_args, *args), kwargs=kwargs, daemon=daemon)
        mount_proc.start()
        logger.info(f'started proc: {mount_proc.pid}')
        _add_proc(mount_path, mount_proc)
    
    @classmethod
    def unmount(cls, mount_path: str) -> None:
        logger.info(f'Unmounting {mount_path}')
        _kill_proc(mount_path)



