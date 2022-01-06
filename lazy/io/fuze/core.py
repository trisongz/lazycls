

""" 
Requires fusepy and fuse
- does not support win
"""
import sys
import pathlib
import threading

from importlib import import_module
from fsspec import AbstractFileSystem
from fsspec.asyn import AsyncFileSystem
#from fsspec.fuse import FUSEr, FUSE

from lazy.libz import Lib
from typing import Dict, Type, Any, Optional, Union
from types import ModuleType
from lazy.models import BaseCls
from logz import get_logger

try: from fsspec.fuse import FUSEr, FUSE
except ImportError: FUSEr = FUSE = object

logger = get_logger('fuze')
FuseSystemType = Union[Type[AbstractFileSystem], Type[AsyncFileSystem]]

class MountPoint(BaseCls):
    fuzer: str # Name of the class
    source: str # User provided Mount Source
    mount_point: str # User provided mount point
    cleanup: bool = True # Cleanup dir after unmount
    thread: Optional[threading.Thread] = None
    alive: bool = True

    @property
    def target_path(self):
        return pathlib.Path(self.mount_point).resolve()
    
    @property
    def target_path_str(self):
        return self.target_path.as_posix()

    def kill(self, timeout: int = 5):
        if self.thread: self.thread.join(timeout=timeout)
        self.alive = False
        self.thread = None
        logger.info(f'[{self.fuzer}] Completed Unmount: {self.source}')

    def unmount(self, timeout: int = 5):
        if not self.alive and not self.thread: return
        logger.info(f'[{self.fuzer}] Unmounting: {self.source} -> {self.target_path.as_posix()}')
        self.kill(timeout)
        if self.cleanup:
            logger.info(f'[{self.fuzer}] Cleaning Up Mount Point: {self.target_path.as_posix()}')
            self.target_path.unlink()



## Keep track of all mounted
_ALL_FUZES: Dict[str, MountPoint] = {}
_FUZE_READY: bool = False
_FUZE_ALLOWED: bool = not sys.platform.startswith('win')


def _prepare_fuze():
    global _FUZE_READY
    assert _FUZE_ALLOWED, 'Windows is not supported'
    if _FUZE_READY: return
    Lib.import_cmd('fuse')
    Lib._ensure_lib_installed('fuse', 'fusepy')
    _FUZE_READY = True

def _add_proc(path: str, proc: threading.Thread):
    global _ALL_FUZES
    _ALL_FUZES[path] = proc

def _kill_proc(path: str, timeout: int = 5, force: bool = False):
    global _ALL_FUZES
    if _ALL_FUZES.get(path):
        proc = _ALL_FUZES.pop(path)
        proc.unmount(timeout)
    elif force:
        logger.warning(f'Forcefully unmounting {path}')
        umount = Lib.import_cmd('umount')
        umount(path).val

    

def _kill_all(fuzername: str = None, timeout: int = 5):
    """
    Kills all active mounts. If name is provided, will kill those matching the
    fusername, otherwise, removes all.
    """
    global _ALL_FUZES
    fuzerlist = list(_ALL_FUZES.keys()) if not fuzername else [k for k,v in _ALL_FUZES.items() if v.fuzer == fuzername]
    for path in fuzerlist:
        proc = _ALL_FUZES.pop(path)
        proc.unmount(timeout)


try: from lazy import CloudAuthz
except ImportError: CloudAuthz = object

_authz: CloudAuthz = None

def get_cloudauthz():
    global _authz
    if _authz is None:
        from lazy import CloudAuthz
        _authz = CloudAuthz
    return _authz

def run_fuze(fs: FuseSystemType, mount_point: MountPoint, foreground: bool = True, threads: bool = False, ready_file: bool = False, ops_class: 'FUSEr' = None):
    """Mount stuff in a local directory
    This uses fusepy to make it appear as if a given path on an fsspec
    instance is in fact resident within the local file-system.
    This requires that fusepy by installed, and that FUSE be available on
    the system (typically requiring a package to be installed with
    apt, yum, brew, etc.).
    Parameters
    ----------
    fs: FuseSystemType
        From one of the compatible implementations
    mount_point: MountPoint
        An empty directory on the local file-system where the contents of
        the remote path will appear.
    foreground: bool
        Whether or not calling this function will block. Operation will
        typically be more stable if True.
    threads: bool
        Whether or not to create threads when responding to file operations
        within the mounter directory. Operation will typically be more
        stable if False.
    ready_file: bool
        Whether the FUSE process is ready. The `.fuse_ready` file will
        exist in the `mount_point` directory if True. Debugging purpose.
    ops_class: FUSEr or Subclass of FUSEr
        To override the default behavior of FUSEr. For Example, logging
        to file.
    """
    from fsspec.fuse import FUSE, FUSEr
    if not ops_class: ops_class = FUSEr
    mount_point.target_path.mkdir(parents=True, exist_ok=True)
    func = lambda: FUSE(ops_class(fs, mount_point.source, ready_file=ready_file), mount_point.target_path_str, nothreads=not threads, foreground=foreground)
    if not foreground:
        th = threading.Thread(target=func)
        th.daemon = True
        th.start()
        logger.info(f'Started Mount {mount_point.source} -> {mount_point.target_path_str} in Background')
        mount_point.thread = th
        _add_proc(mount_point.mount_point, mount_point)
        return th
    else:  # pragma: no cover
        try:
            logger.info(f'Started Mount {mount_point.source} -> {mount_point.target_path_str} in Foreground')
            func()
        except KeyboardInterrupt:
            _kill_proc(mount_point.mount_point)
            sys.exit()



"""
Will use MemoryFileSystem as default
"""

class BaseFuzerCls:
    """
    Class that manages filesystem fuse mounts
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
    def mount(cls, source: str, mount_path: str, ready_file: bool = True, foreground: bool = False, threads: bool = False, fs_args: Dict[str, Any] = {}, cleanup: bool = True, *args, **kwargs) -> None:
        """ 
        Mounts a source path/bucket/etc with this FuzeCls at mount_path

        source: Bucket/path that will be the source of the mount
        mount_path: the directory that will be mounted to
        foreground: return a thread blocking function, otherwise will be a background thread that will need to be killed explicitly.
        fs_args: Dict[str, Any] that will be passed when initializing the FileSystem (such as auth)
        cleanup: removes the mount_path on unmount
        """
        assert mount_path not in _ALL_FUZES, f'{mount_path} is already mounted from {source}'
        _prepare_fuze()
        logger.info(f'[{cls.__name__}] Starting Mount')
        fs = cls.get_filesystem(fs_args, *args, **kwargs)
        mp = MountPoint(fuzer = cls.__name__, source=source, mount_point=mount_path, cleanup=cleanup)
        run_fuze(fs, mp, foreground=foreground, threads=threads, ready_file=ready_file)
    
    @classmethod
    def unmount(cls, mount_path: str, timeout: int = 5, force: bool = False) -> None:
        """ 
        Unmounts an existing mount at mount_path
        """
        return _kill_proc(mount_path, timeout = timeout, force = force)
    
    @classmethod
    def unmount_all(cls, global_unmount: bool = False, timeout: int = 5) -> None:
        """ 
        Unmounts all existing mounts for this Fuze Class, i.e. all s3, or all gcs. 
        
        If global_unmount is True, then unmount all, regardless of class
        """
        fzername = None if global_unmount else cls.__name__
        return _kill_all(fzername, timeout = timeout)



