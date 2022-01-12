from __future__ import annotations

import os
import io
import inspect
import ntpath
import pathlib
import posixpath
from pathlib import PosixPath, WindowsPath, _NormalAccessor, Path, PurePath, _ignore_error
from pathlib import _make_selector as _sync_make_selector
from typing import Optional, List, Union, AsyncIterable, Iterable, IO
from os import stat_result, PathLike
from stat import S_ISDIR, S_ISLNK, S_ISREG, S_ISSOCK, S_ISBLK, S_ISCHR, S_ISFIFO

from anyio import open_file

from aiopath.selectors import _make_selector
from aiopath.flavours import _async_windows_flavour, _async_posix_flavour
from aiopath.wrap import coro_as_method_coro, func_as_method_coro, to_thread, method_as_method_coro, func_to_async_func
from aiopath.handle import IterableAIOFile, get_handle
from aiopath.scandir import EntryWrapper, scandir_async, _scandir_results
from aiopath.types import Final, Literal, FileMode

from fsspec.asyn import AsyncFileSystem

from lazy.serialize import Serialize
from .types import *
from ._flavours import _async_sync_windows_flavour, _async_sync_posix_flavour
from ._cloud_static import _ASYNC_SYNTAX_MAPPING

try: import s3fs
except ImportError: s3fs: ModuleType = None

try: import gcsfs
except ImportError: gcsfs: ModuleType = None

try: from lazy import CloudAuthz
except ImportError: CloudAuthz: object = None


URI_PREFIXES = ('gs://', 's3://', 'minio://', 's3compat://')
_URI_SCHEMES = frozenset(('gs', 's3', 'minio', 's3compat'))
_URI_MAP_ROOT = {
    'gs://': '/gs/',
    's3://': '/s3/',
    'minio://': '/minio/',
    's3compat://': '/s3compat/'
}
_PROVIDER_MAP = {
    'gs': 'GoogleCloudStorage',
    's3': 'AmazonS3',
    'minio': 'MinIO',
    's3compat': 'S3Compatible'
}

DEFAULT_ENCODING: Final[str] = 'utf-8'
ON_ERRORS: Final[str] = 'ignore'
NEWLINE: Final[str] = '\n'

_s3_filesystem: 's3fs.S3FileSystem' = None
_s3_async_filesystem: 's3fs.S3FileSystem' = None

_minio_filesystem: 's3fs.S3FileSystem' = None
_minio_async_filesystem: 's3fs.S3FileSystem' = None

_s3compat_filesystem: 's3fs.S3FileSystem' = None
_s3compat_async_filesystem: 's3fs.S3FileSystem' = None

_gcs_filesystem: 'gcsfs.GCSFileSystem' = None
_gcs_async_filesystem: 'gcsfs.GCSFileSystem' = None



def iscoroutinefunction(obj):
    if inspect.iscoroutinefunction(obj): return True
    if hasattr(obj, '__call__') and inspect.iscoroutinefunction(obj.__call__): return True
    return False

"""
Lol why so inconsistent.
https://docs.google.com/spreadsheets/d/1Lj8WXPUuw8T_VWJBUaaa3U41akuBWNDbqL09hviI8oc/edit#gid=0

"""

def _rewrite_async_syntax(obj, provider: str = 's3'):
    """
    Basically - we're rewriting all the fsspec's async method
    from _method to async_method for syntax
    """
    _names = _ASYNC_SYNTAX_MAPPING[provider]
    for attr in dir(obj):
        if attr.startswith('_') and not attr.startswith('__'):
            attr_val = getattr(obj, attr)
            if iscoroutinefunction(attr_val) and _names.get(attr):
                setattr(obj, _names[attr], attr_val)
    return obj



if CloudAuthz:
    authz = CloudAuthz()
    if s3fs is not None:
        ## Setup for AWS S3 ##
        _config = {}
        if authz.aws_access_key_id:
            _config['key'] = authz.aws_access_key_id
            _config['secret'] = authz.aws_secret_access_key
        elif authz.aws_access_token: _config['token'] = authz.aws_access_token
        elif not authz.boto_config: _config['anon'] = True
        if authz.s3_config: _config['config_kwargs'] = authz.s3_config

        _s3_filesystem: 's3fs.S3FileSystem' = s3fs.S3FileSystem(**_config)
        _s3_async_filesystem: 's3fs.S3FileSystem' = _rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))

        ## Setup for Minio S3 ##
        _config = {}
        if authz.minio_secret_key:
            _config['key'] = authz.minio_access_key
            _config['secret'] = authz.minio_secret_key
        elif authz.minio_access_token: _config['token'] = authz.minio_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.minio_endpoint}
        if authz.minio_config: _config['config_kwargs'] = authz.minio_config
        
        _minio_filesystem: 's3fs.S3FileSystem'  = s3fs.S3FileSystem(**_config)
        _minio_async_filesystem: 's3fs.S3FileSystem'  = _rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))

        ## Setup for S3 Compat ##
        _config = {}
        if authz.s3compat_secret_key:
            _config['key'] = authz.s3compat_access_key
            _config['secret'] = authz.s3compat_secret_key
        elif authz.s3compat_access_token: _config['token'] = authz.s3compat_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.s3compat_endpoint}
        if authz.s3compat_region: _config['client_kwargs']['region_name'] = authz.s3compat_region
        if authz.s3compat_config: _config['config_kwargs'] = authz.s3compat_config

        _s3compat_filesystem: 's3fs.S3FileSystem' = s3fs.S3FileSystem(**_config)
        _s3compat_async_filesystem: 's3fs.S3FileSystem' = _rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))

    if gcsfs is not None:
        _config = {}
        if authz.gcp_auth or authz.gauth or authz.gcp_authb64 or authz.gcp_authbgz: _config['token'] = authz.gcp_auth or authz.gauth or authz.gcp_authb64 or authz.gcp_authbgz
        if authz.gcloud_project or authz.google_cloud_project: _config['project'] = authz.gcloud_project or authz.google_cloud_project
        if authz.gcs_client_config: _config['client_kwargs'] = authz.gcs_client_config
        if authz.gcs_config: _config['config_kwargs'] = authz.gcs_config

        _gcs_filesystem: 'gcsfs.GCSFileSystem' = gcsfs.GCSFileSystem( **_config)
        _gcs_async_filesystem: 'gcsfs.GCSFileSystem' = _rewrite_async_syntax(gcsfs.GCSFileSystem(asynchronous=True, **_config), 'gs')

class _CFS:
    s3: 's3fs.S3FileSystem' = _s3_filesystem
    s3a: 's3fs.S3FileSystem' = _s3_async_filesystem

    minio: 's3fs.S3FileSystem' = _minio_filesystem
    minioa: 's3fs.S3FileSystem' = _minio_async_filesystem

    s3c: 's3fs.S3FileSystem' = _s3compat_filesystem
    s3ca: 's3fs.S3FileSystem' = _s3compat_async_filesystem

    gs: 'gcsfs.GCSFileSystem' = _gcs_filesystem
    gsa: 'gcsfs.GCSFileSystem' = _gcs_async_filesystem

class _AsyncSyncS3Accessor(_NormalAccessor):
    # Sync methods
    info = _CFS.s3.info
    stat = _CFS.s3.stat
    open = _CFS.s3.open
    listdir = _CFS.s3.ls

    mkdir = _CFS.s3.mkdir
    unlink = _CFS.s3.rm_file
    rmdir = _CFS.s3.rmdir

    rename = _CFS.s3.rename
    replace = _CFS.s3.rename
    remove = _CFS.s3.rm

    # Async Methods
    async_info = _CFS.s3a.async_info
    async_stat = func_as_method_coro(_CFS.s3.stat)
    async_listdir = _CFS.s3a.async_list_objects
    async_open = _CFS.s3a._open

    async_mkdir = _CFS.s3a.async_mkdir
    async_unlink = _CFS.s3a.async_rm_file
    async_rmdir = _CFS.s3a.async_rmdir

    async_rename = func_as_method_coro(_CFS.s3.rename)
    async_replace = func_as_method_coro(_CFS.s3.rename)
    async_remove = _CFS.s3a.async_rm
    async_touch = func_as_method_coro(_CFS.s3.touch)



class _AsyncSyncGSAccessor(_NormalAccessor):
    # Sync methods
    stat = _CFS.gs.stat
    open = _CFS.gs.open
    listdir = _CFS.gs.ls

    mkdir = _CFS.gs.mkdir
    unlink = _CFS.gs.rm_file
    rmdir = _CFS.gs.rmdir

    rename = _CFS.gs.rename
    replace = _CFS.gs.rename
    remove = _CFS.gs.rm
    touch = _CFS.gs.touch

    # Async Methods
    async_stat = func_as_method_coro(_CFS.gs.stat)
    async_listdir = _CFS.gsa.async_list_objects

    async_mkdir = _CFS.gsa.async_mkdir
    async_unlink = _CFS.gsa.async_rm_file
    async_rmdir = _CFS.gsa.async_rmdir

    async_rename = func_as_method_coro(_CFS.gs.rename)
    async_replace = func_as_method_coro(_CFS.gs.rename)
    async_remove = _CFS.gsa.async_rm
    async_touch = func_as_method_coro(_CFS.gs.touch)


_async_sync_s3_accessor = _AsyncSyncS3Accessor()
_async_sync_gs_accessor = _AsyncSyncGSAccessor()


class AsyncSyncS3PurePath(PurePath):
    def _init(self, template: Optional[PurePath] = None):
        self._accessor = _async_sync_s3_accessor

    def __new__(cls, *args):
        if cls is AsyncSyncS3PurePath: cls = PureAsyncSyncS3WindowsPath if os.name == 'nt' else PureAsyncSyncS3PosixPath
        return cls._from_parts(args)

    def _new(self, *parts):
        """Create a new `Path` child of same type."""
        return type(self)(*parts)


class PureAsyncSyncS3PosixPath(AsyncSyncS3PurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_sync_posix_flavour
    _pathlike = posixpath
    __slots__ = ()



class PureAsyncSyncS3WindowsPath(AsyncSyncS3PurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_sync_windows_flavour
    _pathlike = ntpath
    __slots__ = ()


class AsyncSyncS3Path(Path, AsyncSyncS3PurePath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _async_sync_windows_flavour if os.name == 'nt' else _async_sync_posix_flavour
    _accessor = _async_sync_s3_accessor
    _pathlike = posixpath
    _prefix = 's3'

    def _init(self, template: Optional[AsyncSyncS3Path] = None):
        self._accessor = _async_sync_s3_accessor
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is AsyncSyncS3Path: cls = AsyncSyncS3WindowsPath if os.name == 'nt' else AsyncSyncS3PosixPath
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self

    @property
    def _path(self) -> str:
        return str(self)
    
    @property
    def _cloudpath(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        if self._prefix in self.parts[0]: return self._pathlike.join(*self.parts[1:])
        return self._pathlike.join(*self.parts)
    
    @property
    def _cloudstr(self) -> str:
        """
        Reconstructs the proper cloud URI
        """
        if self._prefix not in self.parts[0]: return self._prefix + '://' + '/'.join(self.parts)
        return self._prefix + '://' + '/'.join(self.parts[1:])
    
    @property
    def string(self) -> str:
        return str(self)
    
    @property
    def filename_(self) -> str:
        """
        Returns the filename if is file, else ''
        """
        if self.is_file(): return self.parts[-1]
        return ''

    @property
    def ext_(self) -> str:
        """
        Returns the extension for a file
        """
        return self.suffix

    @property
    def extension(self) -> str:
        """
        Returns the extension for a file
        """
        return self.suffix
    
    @property
    def is_cloud(self) -> bool:
        return True
    
    @property
    def exists_(self) -> bool:
        return self.exists()
    
    @property
    def is_file_(self) -> bool:
        return self.is_file()
    
    @property
    def is_dir_(self) -> bool:
        return self.is_dir()
    
    @property
    def home_(self) -> AsyncSyncS3Path:
        return self.home()

    @property
    async def async_exists_(self) -> bool:
        return await self.async_exists()
    
    @property
    async def async_is_file_(self) -> bool:
        return await self.async_is_file()
    
    @property
    async def async_is_dir_(self) -> bool:
        return await self.async_is_dir()
    
    @property
    async def async_home_(self) -> AsyncSyncS3Path:
        return await self.async_home()
    
    
    def open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)

    
    def async_open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline))
    
    def reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
    
    def async_reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return open_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline))
    
    def appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
    
    def async_appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return open_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline))
    
    def writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
    
    def async_writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return open_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline))

    def read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS) -> str:
        with self.open('r', encoding=encoding, errors=errors) as file:
            return file.read()

    async def async_read_text(self, encoding: str | None = DEFAULT_ENCODING, errors: str | None = ON_ERRORS) -> str:
        async with self.async_open('r', encoding=encoding, errors=errors) as file:
            return await file.read()

    def read_bytes(self) -> bytes:
        with self.open('rb') as file:
            return file.read()

    async def async_read_bytes(self) -> bytes:
        async with self.async_open('rb') as file:
            return await file.read()

    def write_bytes(self, data: bytes) -> int:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode='wb') as f:
            return f.write(data)

    async def async_write_bytes(self, data: bytes) -> int:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        async with self.async_open(mode='wb') as f:
            return await f.write(data)

    def append_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        with self.open(mode='a', encoding=encoding, errors=errors, newline=newline) as f:
            n = f.write(data)
            n += f.write(newline)
            return n

    async def async_append_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        async with self.async_open(mode='a', encoding=encoding, errors=errors, newline=newline) as f:
            n = await f.write(data)
            n += await f.write(newline)
            return n

    def write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        with self.open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return f.write(data)

    async def async_write_text(self, data: str, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE) -> int:
        """
        Open the file in text mode, write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError(f'data must be str, not {type(data).__name__}')
        async with self.async_open(mode='w', encoding=encoding, errors=errors, newline=newline) as f:
            return await f.write(data)


    def touch(self, truncate: bool = True, data = None, exist_ok: bool = True, **kwargs):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: self._accessor.stat(self._cloudpath)
            # Avoid exception chaining
            except OSError: pass
            else: return
        self._accessor.touch(self._cloudpath, truncate = truncate, data = data, **kwargs)

    async def async_touch(self, truncate: bool = True, data = None, exist_ok: bool = True, **kwargs):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: await self._accessor.async_stat(self._cloudpath)
            # Avoid exception chaining
            except OSError: pass
            else: return
        await self._accessor.async_touch(self._cloudpath, truncate = truncate, data = data, **kwargs)



class AsyncSyncS3PosixPath(PosixPath, AsyncSyncS3Path, PureAsyncSyncS3PosixPath):
    __slots__ = ()


class AsyncSyncS3WindowsPath(WindowsPath, AsyncSyncS3Path, PureAsyncSyncS3WindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("AsyncSyncPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("AsyncSyncPath.async_is_mount() is unsupported on this system")