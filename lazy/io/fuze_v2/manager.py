

""" Manages the Fuzes using Cachez """
import os
import signal
import pathlib
import threading
import multiprocessing


from types import ModuleType
from typing import Dict, Type, Any, Optional, Union

from lazy.models import BaseCls
from lazy.utils import _get_logger

try: from lazy.io import cachez
except ImportError: cachez = ModuleType
from lazy.types import classproperty
from lazy.configz.common import FuzeConfigz

from .generic import get_fuze, FuzeTypes
from . import utils

logger = _get_logger('Fuze')



class FuzeMount(object):
    def __init__(self, source: str, mount_target: str):
        self._source = source
        self._mount_target = mount_target
        self._mount_path = pathlib.Path(mount_target)
        #self._do_cleanup = cleanup
        self._fs = None
        self._fuzer_name = None
        self._process: Union[threading.Thread, multiprocessing.Process] = None
        self._alive = False
    
    @property
    def source(self): return self._source
    @property
    def mount_target(self): return self._mount_target

    @property
    def mount_path(self): return self._mount_path.as_posix()

    @property
    def source_path(self):
        uri_splits = self._source.split('://', maxsplit=1)
        if len(uri_splits) > 1: return uri_splits[-1]        
        return self._source

    def _cleanup(self):
        if self._mount_path.exists():
            utils.umount(self.mount_path)
            try: self._mount_path.unlink()
            except: pass

    def _prestart(self):
        self._cleanup()
        self._mount_path.mkdir(exist_ok=True, parents=True)
    
    def _run_background(self):
        from .ops import FUZE

        func = lambda: FUZE(
            _FUSEr(fs, self.source_path, ready_file=ready_file), 
            self.target_path_str, 
            nothreads=not threads, 
            foreground=foreground)

    def _start_process(self, daemon: bool = True, **kwargs):
        assert not self._process, 'Process Active. Unable to Start'
        mp = utils.get_multiproc()
        self._process = mp.Process(target=self._run_background, daemon = daemon,  **kwargs)
        self._alive = True

    def _start_thread(self, daemon: bool = True, **kwargs):
        assert not self._process, 'Process Active. Unable to Start'

        mp = utils.get_multiproc()
        self._process = mp.Process(target=self._run_background, daemon = daemon,  **kwargs)
        self._alive = True

    def start(self, use_mp: bool = True, daemon: bool = True, **kwargs):
        if use_mp: return self._start_process(daemon = daemon, **kwargs)
        


    @property
    def fs(self) -> FuzeTypes:
        if not self._fs: 
            self._fs = get_fuze(self._source)
            self._fuzer_name = self._fs.__class__.__name__
            logger.info(f'Initialized Fuzer {self._fuzer_name} for {self._source}')
        return self._fs
    
    @property
    def process_id(self) -> Optional[int]:
        if not self._process: return None
        return self._process.pid


    @property
    def data(self) -> Dict[str, Any]:
        return {
            'fuzer': self._fuzer_name,
            'source': self._source,
            'source_path': self.source_path,
            'mount_target': self._mount_target,
            'mount_path': self.mount_path,
            'pid': self.process_id,
            'alive': self._alive,
        }


"""
Stores in a temp sqllite in ~/.cachez/cache.db

Stores a dict of:
mount_target: {
    'fuzer': str,
    'source': source,
    'mount_target': mount_target,
    'pid': process_id,
    'alive': bool,
},
source: {
    'fuzer': str,
    'source': source,
    'mount_target': mount_target,
    'pid': process_id,
    'alive': bool,
},

"""

class FuzeManager:
    _cache: cachez.Cache = None
    _cachedir: str = None
    _mounts: Dict[str, FuzeMount] = {} # [source, FuzeMount]

    @classproperty
    def cache(cls):
        if not cls._cache:
            from lazy.io import cachez
            cls._cachedir = FuzeConfigz.cache_dir.as_posix()
            logger.info(f"Initialized FuzeManager Cache at {cls._cachedir}")
            cls._cache = cachez.Cache(directory=cls._cachedir)
        return cls._cache
    
    @classmethod
    def _kill_pid(cls, pid: int, sig: int = signal.SIGTERM):
        return os.kill(pid, sig)

    @classmethod
    def _cleanup_process(cls, src: Dict[str, Any]):
        try: utils.umount(src.get('mount_path', src.get('mount_target')))
        except Exception as e:
            logger.error(e)
        if src.get('pid'): cls._kill_pid(src['pid'])

    @classmethod
    def cleanup_mount(cls, mount_target: str = None, source: str = None, src: Dict[str, Any] = None):
        assert source is not None and mount_target is not None and src is not None, 'Must provide source, mount_target or src dict'
        if not src:
            src = cls.cache.get(source or mount_target)
        cls._cleanup_process(src)
        logger.info(f'Completed Cleanup for {src}')
        if source: 
            cls.cache.pop(source, None)
            cls._mounts.pop(source, None)
        if mount_target: 
            cls.cache.pop(mount_target, None)
            cls._mounts.pop(mount_target, None)

    @classmethod
    def get_mount(cls, source: str, mount_target: str, fix_errors: bool = True):
        if cls._mounts.get(source): return cls._mounts[source]
        if cls._mounts.get(mount_target): return cls._mounts[mount_target]
        if cls.cache.get(source) or cls.cache.get(mount_target):
            src = cls.cache.get(source) or cls.cache.get(mount_target)
            logger.warning(f'Source Exists in Cache: {src}')
            assert src.get('alive', False) or fix_errors, 'Exiting as Fix Errors = False or Mount Point is still active'
            cls.cleanup_mount(mount_target, source, src)
        mountz = FuzeMount(source, mount_target)
        cls.cache[mountz.source] = mountz.data
        cls.cache[mountz.mount_target] = mountz.data
        cls._mounts[mountz.source] = mountz
        return mountz


        









    






class MountPoint(BaseCls):
    fuzer: str # Name of the class
    source: str # User provided Mount Source
    mount_point: str # User provided mount point
    cleanup: bool = True # Cleanup dir after unmount
    thread: Optional[threading.Thread] = None
    process: Optional[multiprocessing.Process] = None
    alive: bool = True

    @property
    def source_path(self):
        uri_splits = self.source.split('://', maxsplit=1)
        if len(uri_splits) > 1: return uri_splits[-1].strip()
        return self.source

    @property
    def target_path(self):
        return pathlib.Path(self.mount_point).resolve()
    
    @property
    def target_path_str(self):
        return self.target_path.as_posix()

    def kill(self, timeout: int = 5):
        if self.thread: self.thread.join(timeout=timeout)
        elif self.process:
            os.kill(self.process.pid, signal.SIGTERM) 
            self.process.join(timeout=timeout)

        self.alive = False
        self.thread = None
        logger.info(f'[{self.fuzer}] Completed Unmount: {self.source}')
    
    def _background(self, fs: FuseSystemType, foreground: bool = True, threads: bool = False, ready_file: bool = False):
        self.alive = True
        from fsspec.fuse import FUSE as _FUSE
        from fsspec.fuse import FUSEr as _FUSEr
        func = lambda: _FUSE(
            _FUSEr(fs, self.source_path, ready_file=ready_file), 
            self.target_path_str, 
            nothreads=not threads, 
            foreground=foreground)
        
        while self.alive:
            try:
                logger.info(f'Started Mount {self.source} -> {self.target_path_str} in Background')
                func()
            except KeyboardInterrupt:
                logger.error('Got KeyboardInterrupt. Unmounting')
                self.kill()
                #_kill_proc(self.mount_point)
                break
            except Exception as e:
                logger.error(f'Got Exception {e}. Unmounting')
                self.kill()
                #_kill_proc(self.mount_point)
                break

    
    def mount(self, fs: FuseSystemType, foreground: bool = True, threads: bool = False, ready_file: bool = False, use_mp: bool = False, daemon: bool = True, ops_class: 'FUSEr' = None):
        #from fsspec.fuse import FUSE as _FUSE
        #from fsspec.fuse import FUSEr as _FUSEr
        #if not ops_class: ops_class = _FUSEr
        self.target_path.mkdir(parents=True, exist_ok=True)
        if use_mp:
            #with _lock.acquire():
            #with threading.Lock() as _lockz:
            _lock.acquire()
            self.process = mproc.Process(target=self._background, args=(fs, foreground, threads, ready_file,), daemon=daemon)
            self.process.start()
            _lock.release()

            #with threading.RLock() as _lock:
            #    _lock.acquire()
        elif not foreground:
            #with _lock.acquire():
            #with threading._RLock() as _lock:
            _lock.acquire()
            self.thread = threading.Thread(target=self._background, args=(fs, foreground, threads, ready_file,), daemon = daemon)
            self.thread.start()
            _lock.release()
        
        else:
            self._background(fs, foreground = foreground, threads = threads, ready_file = ready_file)
        



        """
        func = lambda: _FUSE(
            ops_class(fs, self.source_path, ready_file=ready_file), 
            self.target_path_str, 
            nothreads=not threads, 
            foreground=foreground)
        if not foreground:
            th = threading.Thread(target=func)
            th.daemon = True
            th.start()
            logger.info(f'Started Mount {self.source} -> {self.target_path_str} in Background')
            self.thread = th
            _add_proc(self.mount_point, self)

        else:  # pragma: no cover
            try:
                logger.info(f'Started Mount {self.source} -> {self.target_path_str} in Foreground')
                func()
            except KeyboardInterrupt:
                _kill_proc(self.mount_point)
                #sys.exit()
        """


    def unmount(self, timeout: int = 5):
        #if not self.alive and self.thread is None: return
        logger.info(f'[{self.fuzer}] Unmounting: {self.source} -> {self.target_path.as_posix()}')
        self.kill(timeout)
        if self.cleanup:
            logger.info(f'[{self.fuzer}] Cleaning Up Mount Point: {self.target_path.as_posix()}')
            self.target_path.unlink()

