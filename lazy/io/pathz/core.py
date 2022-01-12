import os
import typing
import ntpath
import pathlib
import posixpath
from importlib import import_module
from typing import Any, ClassVar, Iterator, Optional, Type, TypeVar, Union, Callable, List, TYPE_CHECKING
from lazy.libz import Lib

from . import types
from . import base

_P = TypeVar('_P')


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


PathLike = types.PathLike
ReadOnlyPath = base.ReadOnlyPath
ReadWritePath = base.ReadWritePath
PathLikeCls = Union[Type[ReadOnlyPath], Type[ReadWritePath]]

class _IOPath(pathlib.PurePath, ReadWritePath):
    """Pathlib-like API around `fsspec` providing Async Capabilities"""
    _PATH: ClassVar[types.ModuleType]
    _FSX: ClassVar[types.ModuleType] = None
    _FSX_LIB: str = None
    _FSX_MODULE: Optional[str] = None
    _FSX_CLS: Optional[str] = None
    _SYNC_FS: ClassVar[types.ModuleType] = None
    _ASYNC_FS: ClassVar[types.ModuleType] = None

    @classmethod
    def _ensure_lib(cls, *args, **kwargs):
        if cls._FSX is not None: return
        cls._FSX = Lib.import_lib(cls._FSX_LIB)
        if cls._FSX_MODULE: cls._FSX = import_module(cls._FSX_MODULE, package=cls._FSX_LIB)
        #cls._FSX = Lib.import_lib(cls._FSX_LIB)

    @classmethod
    def get_filesystem(cls, is_async: bool = False, *args, **kwargs):
        cls._ensure_lib()
        if is_async and cls._ASYNC_FS: return cls._ASYNC_FS
        if cls._SYNC_FS: return cls._SYNC_FS
        authz = cls.get_configz(*args, **kwargs)
        if is_async:
            cls._ASYNC_FS = getattr(cls._FSX, cls._FSX_CLS)(asynchronous = True, **authz)
            return cls._ASYNC_FS
        cls._SYNC_FS = getattr(cls._FSX, cls._FSX_CLS)(**authz)
        return cls._SYNC_FS
    
    @classmethod
    def get_configz(cls, *args, **kwargs):        
        return {}

    @property
    def async_fs(self) :
        if not self._ASYNC_FS:
            self.get_filesystem(is_async=True)
        return self._ASYNC_FS
    
    @property
    def sync_fs(self):
        if not self._SYNC_FS:
            self.get_filesystem()
        return self._SYNC_FS

    def __new__(cls: Type[_P], *parts: types.PathLike) -> _P:
        full_path = '/'.join(os.fspath(p) for p in parts)
        if not full_path.startswith(URI_PREFIXES): return super().__new__(cls, *parts)
        prefix = full_path[:5]
        new_prefix = _URI_MAP_ROOT[prefix]
        return super().__new__(cls, full_path.replace(prefix, new_prefix, 1))

    def _new(self: _P, *parts: types.PathLike) -> _P:
        """Create a new `Path` child of same type."""
        return type(self)(*parts)

    @property
    def _uri_scheme(self) -> Optional[str]:
        if (len(self.parts) >= 2 and self.parts[0] == '/' and self.parts[1] in _URI_SCHEMES): return self.parts[1]
        else: return None
    
    def get_bucket(self, prefix: bool = True) -> Optional[str]:
        """
        Returns the root bucket with optional prefix
        """
        if not self.is_cloud: return None
        parts = self.split_path()
        uri_scheme = self._uri_scheme
        if prefix: return f'{uri_scheme}://' + parts[0]
        return parts[0]
    
    def get_bucket_path(self, prefix: bool = True) -> Optional[str]:
        """
        Returns the root bucket + path with optional prefix
        removes the versioning
        """
        if not self.is_cloud: return None
        parts = self.split_path()
        uri_scheme = self._uri_scheme
        p = '/'.join(parts[:2])
        if prefix: return f'{uri_scheme}://' + p
        return p

    @property
    def bucket(self) -> Optional[str]:
        if self._uri_scheme: return self._PATH.join(f'{self._uri_scheme}://', self.parts[3])
        return None

    @property
    def bucket_path(self) -> Optional[str]:
        if self._uri_scheme: return self._PATH.join(*self.parts[3:])
        return None
    

    @property
    def _path_str(self) -> str:
        """
        Returns the `__fspath__` string representation.
        """
        uri_scheme = self._uri_scheme
        if uri_scheme: return self._PATH.join(f'{uri_scheme}://', *self.parts[2:])
        else: return self._PATH.join(*self.parts) if self.parts else '.'
    
    @property
    def string(self) -> str:
        """
        Returns the extension for a file
        """
        return self._path_str

    @property
    def _filename_str(self) -> str:
        """
        Returns the filename if is file, else ''
        """
        if self.is_file(): return self.parts[-1]
        return ''
    

    @property
    def extension(self) -> str:
        """
        Returns the extension for a file
        """
        return self.suffix

    @property
    def _cpath_str(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        uri_scheme = self._uri_scheme
        if uri_scheme: return self._PATH.join(*self.parts[2:])
        else: return self._PATH.join(*self.parts) if self.parts else '.'

    def __fspath__(self) -> str:
        return self._path_str

    def __str__(self) -> str:    # pylint: disable=invalid-str-returned
        return self._path_str

    def __repr__(self) -> str:
        return f'{type(self).__name__}({self._path_str!r})'

    def expanduser(self: _P) -> _P:
        """
        Returns a new path with expanded `~` and `~user` constructs.
        """
        return self._new(self._PATH.expanduser(self._path_str))
    

    def resolve(self: _P, strict: bool = False) -> _P:
        """
        Returns the abolute path.
        """
        if self.is_cloud: return self._new(self.as_posix())
        return self._new(self._PATH.abspath(self._path_str))
        
    def copydir(self: _P, dst: base.PathLike, ignore=['.git'], overwrite: bool = False, dryrun: bool = False):
        """
        Copies the Current Top Level Parent Dir to the Dst Dir without recursion
        """
        dst = self._new(dst)
        assert dst.is_dir(), 'Destination is not a valid directory'
        if not dryrun: dst.ensure_dir()
        copied_files = []
        fnames = self.listdir(ignore=ignore)
        curdir = self.absolute_parent
        for fname in fnames:
            dest_path = dst.joinpath(fname.relative_to(curdir))
            if not dryrun: fname.copy(dest_path, overwrite=overwrite, skip_errors=True)
            copied_files.append(dest_path)
        return copied_files


    def copydirs(self: _P, dst: base.PathLike, mode: str = 'shallow', pattern='*', ignore=['.git'], overwrite: bool = False, levels: int = 2, dryrun: bool = False):
        """Copies the Current Parent Dir to the Dst Dir.
        modes = [shallow for top level recursive. recursive for all nested]
        levels = number of recursive levels
        dryrun = returns all files that would have been copied without copying
        """
        assert mode in {'shallow', 'recursive'}, 'Invalid Mode Option: [shallow, recursive]'
        dst = self._new(dst)
        assert dst.is_dir(), 'Destination is not a valid directory'
        levels = max(1, levels)
        dst.ensure_dir()
        curdir = self.absolute_parent
        copied_files = []
        if levels > 1 and mode == 'recursive' and '/' not in pattern:
            for _ in range(1, levels): pattern += '/*'
        if self.is_dir() and not pattern.startswith('/'): pattern = '*/' + pattern
        fiter = curdir.glob(pattern) if mode == 'shallow' else curdir.rglob(pattern)
        fnames = [f for f in fiter if not bool(set(f.parts).intersection(ignore))]
        for f in fnames:
            dest_path = dst.joinpath(f.relative_to(curdir))
            if not dryrun:
                if f.is_dir(): dest_path.ensure_dir()
                else: f.copy(dest_path, overwrite=overwrite, skip_errors=True)
            copied_files.append(dest_path)
        return copied_files

    def listdir(self: _P, ignore=['.git'], skip_dirs=True, skip_files=False):
        fnames = [f for f in self.iterdir() if not bool(set(f.parts).intersection(ignore))]
        fnames = [f.resolve() for f in fnames]
        if skip_dirs:
            return [f for f in fnames if f.is_file()]
        if skip_files:
            return [f for f in fnames if f.is_dir()]
        return fnames

    def listdirs(self: _P, mode: str = 'shallow', pattern='*', ignore=['.git'], skip_dirs=True, skip_files=False, levels: int = 2):
        """Lists all files in current parent dir
        modes = [shallow for top level recursive. recursive for all nested]
        """
        assert mode in {'shallow', 'recursive'}, 'Invalid Mode Option: [shallow, recursive]'
        curdir = self.absolute_parent
        levels = max(1, levels)
        if levels > 1 and mode == 'recursive' and '/' not in pattern:
            for _ in range(1, levels): pattern += '/*'
        if self.is_dir() and not pattern.startswith('*/'): pattern = '*/' + pattern
        fiter = curdir.glob(pattern) if mode == 'shallow' else curdir.rglob(pattern)
        fnames = [f for f in fiter if not bool(set(f.parts).intersection(ignore))]
        if skip_dirs: return [f for f in fnames if f.is_file()]
        if skip_files: return [f for f in fnames if f.is_dir()]
        return list(fnames)

    def ensure_dir(self: _P, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """Ensures the parent directory exists, creates if not"""
        return self.absolute_parent.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
    
    def _get_dest(self: _P, dest: base.PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Validates the Destination
        """
        _dest = self._new(dest)
        if self.is_dir() and recursive: return _dest
        if _dest.is_dir(): _dest = _dest.joinpath(self._filename_str)
        if _dest.exists() and not overwrite:
            if skip_errors: return _dest
            raise ValueError(f'{_dest.as_posix()} exists and overwrite = False')
        return _dest

    @property
    def absolute_parent(self) -> _P:
        uri_scheme = self._uri_scheme
        if uri_scheme: return self._new(self._PATH.join(f'{uri_scheme}://', '/'.join(self.parts[2:-1])))
        p = self.resolve()
        if p.is_dir(): return p
        return p.parent

    @property
    def is_cloud(self) -> bool:
        return bool(self._uri_scheme)
    
    @property
    def cloud_provider(self) -> Optional[str]:
        return _PROVIDER_MAP.get(self._uri_scheme, None)

    @property
    def provider(self) -> Optional[str]:
        return _PROVIDER_MAP.get(self._uri_scheme, 'Local')

    @property
    def bucket(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def bucket_path(self) -> Optional[str]:
        raise NotImplementedError

    @property
    def is_cloud(self) -> bool:
        return bool(self._uri_scheme)
    
    @property
    def is_gs(self) -> bool:
        return bool(self._uri_scheme == 'gs')
    
    @property
    def is_s3(self) -> bool:
        return bool(self._uri_scheme == 's3')
    
    @property
    def is_minio(self) -> bool:
        return bool(self._uri_scheme == 'minio')
    
    def exists(self) -> bool:
        """
        Returns True if self exists.
        """
        raise NotImplementedError

    def is_dir(self) -> bool:
        """
        Returns True if self is a directory.
        """
        raise NotImplementedError

    def is_file(self) -> bool:
        """
        Returns True if self is a file.
        """
        raise NotImplementedError

    def cat(self: _P, recursive: bool = False, on_error: Optional[str] = None, **kwargs):
        """
        Fetch (potentially multiple) paths contents
        """
        raise NotImplementedError
    
    def cat_file(self: _P, start: Optional[int] = None, end: Optional[int] = None, **kwargs):
        """
        Get the content of a file
        """
        raise NotImplementedError

    def copy(self: _P, dest: base.PathLike, overwrite: bool = False, skip_errors: bool = False) -> _P:
        """
        Copies the File to the Dir/File.
        """
        raise NotImplementedError
    
    def download(self: _P, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file(s) to local.
        """
        raise NotImplementedError
    
    def du(self: _P, total: Optional[int] = None, maxdepth: Optional[int] = None, **kwargs):
        """
        Fetch (potentially multiple) paths contents
        """
        raise NotImplementedError
    
    def find(self: _P, maxdepth: Optional[int] = None, withdirs: bool = False, detail: bool = False, prefix: Optional[str] = None, **kwargs):
        """
        List all files below path.
        """
        raise NotImplementedError
    
    def get_checksum(self: _P, refresh: bool = False, **kwargs):
        """
        Unique value for current version of file
        """
        raise NotImplementedError
    
    def get_file(self: _P, dest: base.PathLike, callback: Optional[Callable] = None, **kwargs):
        """
        Copy single remote file to local
        """
        raise NotImplementedError

    def get_files(self: _P, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file(s) to local.
        """
        raise NotImplementedError

    def glob(self: _P, pattern: str) -> Iterator[_P]:
        """
        Yielding all matching files (of any kind).
        """
        raise NotImplementedError

    def head(self: _P, size: Optional[int] = None):
        """ 
        Get the first size bytes from file
        """
        raise NotImplementedError

    def iterdir(self: _P) -> Iterator[_P]:
        """
        Iterates over the directory.
        """
        raise NotImplementedError


    def get_modified(self: _P, version_id: str = None, refresh: bool = False, **kwargs):
        """
        Return the last modified timestamp of file at path as a datetime
        """
        raise NotImplementedError
    
    def ls(self: _P, detail: bool = False, **kwargs):
        """
        List objects at path.
        """
        raise NotImplementedError
    
    def mkdir(self: _P, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """
        Create a new directory at this given path.
        """
        raise NotImplementedError

    def move(self: _P, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Move file(s) from one location to another
        """
        raise NotImplementedError
    
    def mv(self: _P, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Move file(s) from one location to another
        """
        raise NotImplementedError

    def open(self: _P, mode: str = 'r', encoding: Optional[str] = base.DEFAULT_ENCODING, errors: Optional[str] = None, **kwargs: Any) -> typing.IO[Union[str, bytes]]:
        """Opens the file."""
        raise NotImplementedError
    
    def put_file(self: _P, dest: base.PathLike, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file from local.
        """
        raise NotImplementedError
    
    def put_files(self: _P, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file(s) from local.
        """
        raise NotImplementedError
    

    def remove(self: _P, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Delete files.
        """
        raise NotImplementedError

    def rename(self: _P, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Rename file or directory to the given target.
        """
        raise NotImplementedError    

    def replace(self: _P, target: base.PathLike) -> _P:
        """
        Replace file or directory to the given target.
        """
        raise NotImplementedError

    def rm(self: _P, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Delete files.
        """
        raise NotImplementedError
    
    def rm_file(self: _P, missing_ok: bool = True, **kwargs):
        """
        Delete a file
        """
        raise NotImplementedError

    def rmdir(self: _P, force: bool = False) -> None:
        """
        Remove the empty directory.
        """
        raise NotImplementedError
        
    def rmtree(self: _P) -> None:
        """
        Remove the directory.
        """
        raise NotImplementedError

    
    def sign(self: _P, expiration: int = 100, **kwargs):
        """
        Create a signed URL representing the given path
        Some implementations allow temporary URLs to be generated, as a way of delegating credentials
        """
        raise NotImplementedError
    
    def split_path(self: _P, **kwargs) -> List[str]:
        """
        Normalise path string into bucket and key
        """
        raise NotImplementedError
    
    def tail(self: _P, size: Optional[int] = None):
        """ 
        Get the last size bytes from file
        """
        raise NotImplementedError

    def touch(self: _P, truncate: bool = True, data = None, **kwargs):
        """
        Create empty file or truncate
        """
        raise NotImplementedError
    
    def unlink(self: _P, missing_ok: bool = True) -> None:
        """
        Remove this file or symbolic link.
        """
        raise NotImplementedError
    

    def url(self: _P, expires: int = 3600, client_method: str = 'get_object', **kwargs):
        """
        Generate presigned URL to access path by HTTP
        """
        raise NotImplementedError

    def upload(self: _P, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file(s) from local.
        """
        raise NotImplementedError

    @property
    def checksum(self):
        """
        Unique value for current version of file without refreshing
        """
        return self.get_checksum()
    
    @property
    def created(self):
        """
        Return the created timestamp of a file as a datetime
        """
        return self.get_checksum()
    

    @property
    def exist(self) -> bool:
        """
        Returns True if self exists.
        """
        return self.exists()

    @property
    def info(self):
        """
        Give details of entry at path
        """
        raise NotImplementedError
    
    @property
    def isdir(self) -> bool:
        """
        Returns True if self is a directory.
        """
        return self.is_dir()

    @property
    def isfile(self) -> bool:
        """
        Returns True if self is a file.
        """
        return self.is_file()

    @property
    def modified(self):
        """
        Return the last modified timestamp of file at path as a datetime without refreshing
        """
        return self.get_modified()
    
    @property
    def size(self: _P):
        """
        Size in bytes of file
        """
        raise NotImplementedError
    
    @property
    def stat(self: _P):
        """
        Give details of entry at path
        """
        raise NotImplementedError
    
    @property
    def home(self):
        """
        Returns the home directory
        """
        if self.is_cloud: return self._new(self.get_bucket(True))
        p = os.path.expanduser('~')
        return self._new(p)
    
    @property
    def userdir(self):
        return self.home



import upath
from fsspec.asyn import AsyncFileSystem
from fsspec.implementations.local import LocalFileSystem

class AsyncFSx(AsyncFileSystem, LocalFileSystem):
    pass

class PosixFSxPath(_IOPath, pathlib.PurePosixPath):
    """
    Pathlib-like API around `fsspec` providing Async Capabilities
    """
    _PATH = posixpath
    _FSX: 'LocalFileSystem' = None
    _SYNC_FS: 'LocalFileSystem' = None
    _ASYNC_FS: 'LocalFileSystem' = None
    _FSX_LIB: str = 'fsspec'
    _FSX_MODULE: Optional[str] = 'fsspec.implementations.local'
    _FSX_CLS: str = 'LocalFileSystem'

    @classmethod
    def _get_filesystem(cls, is_async: bool = False, *args, **kwargs):
        cls._ensure_lib()
        if is_async and cls._ASYNC_FS: return cls._ASYNC_FS
        if cls._SYNC_FS: return cls._SYNC_FS
        # remove this since we will have it be for sure available.
        #from fsspec.implementations.local import LocalFileSystem
        if is_async:
            cls._ASYNC_FS = AsyncFSx(asynchronous = True)
            return cls._ASYNC_FS
        cls._SYNC_FS = LocalFileSystem()
        return cls._SYNC_FS

    @property
    def async_fs(self) -> 'AsyncFSx':
        if not self._ASYNC_FS: self._get_filesystem(is_async=True)
        return self._ASYNC_FS
    
    @property
    def sync_fs(self) -> 'LocalFileSystem':
        if not self._SYNC_FS: self._get_filesystem()
        return self._SYNC_FS

    
    @classmethod
    def get_configz(cls, *args, **kwargs):
        return {}
    
    def exists(self) -> bool:
        """
        Returns True if self exists.
        """
        return self.sync_fs.exists(self._cpath_str)
    
    def expand_path(self, recursive: bool = False, maxdepth: Optional[int] = None):
        """
        Turn one or more globs or directories into a list of all matching paths to files or directories.
        """
        paths = self.sync_fs.expand_path(recursive = recursive, maxdepth = maxdepth)
        return [self._new(p) for p in paths]

    def is_dir(self) -> bool:
        """
        Returns True if self is a directory.
        """
        return self.sync_fs.isdir(self._cpath_str)

    def is_file(self) -> bool:
        """Returns True if self is a file."""
        return self.sync_fs.isfile(self._cpath_str)


    def cat(self, recursive: bool = False, on_error: Optional[str] = None, **kwargs):
        """
        Fetch (potentially multiple) paths contents
        """
        return self.sync_fs.cat(self._cpath_str, recursive, on_error, **kwargs)
    
    def cat_file(self, start: Optional[int] = None, end: Optional[int] = None, **kwargs):
        """
        Get the content of a file
        """
        return self.sync_fs.cat_file(self._cpath_str, start=start, end=end, **kwargs)

    def copy(self, dest: base.PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False) -> _P:
        """
        Copies the File to the Dir/File.
        """
        _dest = self._get_dest(dest, recursive, overwrite, skip_errors)
        self.sync_fs.copy(self._cpath_str, _dest._cpath_str, recursive = recursive)
        return _dest

    
    def download(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) to local.
        """
        _dest = self._get_dest(dest, recursive, overwrite, skip_errors)
        self.sync_fs.download(self._cpath_str, _dest._cpath_str, recursive = recursive, callback = callback)
        return _dest
    
    def du(self, total: Optional[int] = None, maxdepth: Optional[int] = None, **kwargs):
        """
        Fetch (potentially multiple) paths contents
        """
        return self.sync_fs.du(self._cpath_str, total, maxdepth, **kwargs)
    
    def find(self, path: Optional[str] = None, maxdepth: Optional[int] = None, withdirs: bool = False, detail: bool = False, prefix: Optional[str] = None, **kwargs):
        """
        List all files below path.
        """
        p = self if not path else self.joinpath(path)
        return self.sync_fs.find(p, maxdepth=maxdepth, withdirs=withdirs, detail=detail, prefix=prefix, **kwargs)
    
    def get_checksum(self, refresh: bool = False, **kwargs):
        """
        Unique value for current version of file
        """
        return self.sync_fs.checksum(self._cpath_str, refresh=refresh, **kwargs)
    
    def get_file(self, dest: base.PathLike, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy single remote file to local
        """
        _dest = self._get_dest(dest, overwrite=overwrite, skip_errors=skip_errors)
        return self.sync_fs.get_file(self._cpath_str, _dest._cpath_str, callback=callback, **kwargs)


    def get_files(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) to local.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        return self.sync_fs.get(self._cpath_str, _dest._cpath_str, recursive=recursive, callback=callback, **kwargs)
        

    def glob(self, pattern: str, **kwargs) -> Iterator[_P]:
        """
        Yielding all matching files (of any kind).
        """
        uri_scheme = self._uri_scheme
        for f in self.sync_fs.glob(self._PATH.join(self._path_str, pattern)._cpath_str,  **kwargs):
            if self.is_cloud: yield self._new(f'{uri_scheme}://' + f)
            else: yield self._new(f)

    def head(self, size: Optional[int] = None):
        """ 
        Get the first size bytes from file
        """
        return self.sync_fs.head(self._cpath_str, size)

    def iterdir(self, **kwargs) -> Iterator[_P]:
        """
        Iterates over the directory.
        """
        uri_scheme = self._uri_scheme
        for f in self.sync_fs.glob(self._cpath_str, **kwargs):
            if self.is_cloud: yield self._new(f'{uri_scheme}://' + f)
            else: yield self._new(f)

    def get_modified(self, version_id: str = None, refresh: bool = False, **kwargs):
        """
        Return the last modified timestamp of file at path as a datetime
        """
        return self.sync_fs.modified(self._cpath_str, version_id=version_id, refresh=refresh, **kwargs)
    
    ## return self.sync_fs.modified(self._cpath_str, 

    def ls(self, detail: bool = False, **kwargs):
        """
        List objects at path.
        """
        return self.sync_fs.ls(self._cpath_str, detail=detail, **kwargs)

    
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """
        Create a new directory at this given path.
        """
        ## Will return if exist_ok first, otherwise will fail check
        if self.exists() and exist_ok: return
        return self.sync_fs.mkdir(self._cpath_str, mode=mode, create_parents=parents, exist_ok=exist_ok)

    def move(self, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Move file(s) from one location to another
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.sync_fs.move(self._cpath_str, _dest._cpath_str, recursive = recursive, maxdepth=maxdepth)
        return _dest

    
    def mv(self, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Move file(s) from one location to another
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.sync_fs.move(self._cpath_str, _dest._cpath_str, recursive = recursive, maxdepth=maxdepth)
        return _dest

    def open(self, mode: str = 'r', encoding: Optional[str] = base.DEFAULT_ENCODING, errors: Optional[str] = None, block_size: int = 5242880, compression: str = 'infer', **kwargs: Any) -> typing.IO[Union[str, bytes]]:
        """Opens the file."""
        filelike = self.sync_fs.open(self._cpath_str, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, **kwargs)
        filelike = typing.cast(typing.IO[Union[str, bytes]], filelike)
        return filelike

    def put_file(self, dest: base.PathLike, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file from local.
        """
        _dest = self._get_dest(dest, overwrite=overwrite, skip_errors=skip_errors)
        self.sync_fs.put_file(self._cpath_str, _dest._cpath_str, callback = callback, **kwargs)
        return _dest
    
    def put_files(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) from local.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.sync_fs.put(self._cpath_str, _dest._cpath_str, recursive=recursive, callback = callback, **kwargs)
        return _dest

    def remove(self, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Delete files.
        """
        return self.sync_fs.rm(self._cpath_str, recursive=recursive, maxdepth = maxdepth, **kwargs)

    def rename(self, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Rename file or directory to the given target.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.sync_fs.rename(self._cpath_str, _dest._cpath_str, recursive=recursive, maxdepth = maxdepth, **kwargs)
        return _dest

    def replace(self, target: base.PathLike) -> _P:
        """
        Replace file or directory to the given target.
        """
        _dest = self._get_dest(target, overwrite=True)
        self.sync_fs.rename(self._cpath_str, _dest._cpath_str)

    def rm(self, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Delete files.
        """
        self.sync_fs.rm(self._cpath_str, recursive, maxdepth, **kwargs)
    
    def rm_file(self, missing_ok: bool = True, **kwargs):
        """
        Delete a file
        """
        if not self.exists():
            if missing_ok: return
            raise ValueError(f"{self._path_str} does not exist")
        self.sync_fs.rm_file(self._cpath_str, **kwargs)

    def rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True) -> None:
        """
        Remove the empty directory.
        If force, will recursively remove even if not empty.
        """
        try:
            return self.sync_fs.rmdir(self._cpath_str)
        except Exception as e:
            if force: return self.sync_fs.rm(self._cpath_str, recursive = recursive)
            if skip_errors: return
            raise e

    def rmtree(self, recursive: bool = True, maxdepth: Optional[int] = None) -> None:
        """Remove the directory."""
        return self.sync_fs.rm(self._cpath_str, recursive, maxdepth)
    
    def sign(self, expiration: int = 100, **kwargs):
        """
        Create a signed URL representing the given path
        Some implementations allow temporary URLs to be generated, as a way of delegating credentials
        """
        return self.sync_fs.sign(self._cpath_str, expiration, **kwargs)
    
    def split_path(self, **kwargs) -> List[str]:
        """
        Normalise path string into bucket and key
        """
        return self.sync_fs.split_path(self._cpath_str, **kwargs)
    
    def tail(self, size: Optional[int] = None):
        """ 
        Get the last size bytes from file
        """
        return self.sync_fs.tail(self._cpath_str, size)

    def touch(self, truncate: bool = True, data = None, **kwargs):
        """
        Create empty file or truncate
        """
        return self.sync_fs.touch(self._cpath_str, truncate = truncate, data = data, **kwargs)
    
    def unlink(self, missing_ok: bool = True, **kwargs) -> None:
        """
        Remove this file or symbolic link.
        """
        if getattr(self.sync_fs, 'unlink', None): return self.sync_fs.unlink(self._cpath_str, missing_ok = missing_ok, **kwargs)
        return self.rm_file(missing_ok, **kwargs)
    
    def url(self, expires: int = 3600, client_method: str = 'get_object', **kwargs):
        """
        Generate presigned URL to access path by HTTP
        """
        return self.sync_fs.url(self._cpath_str, expires, client_method, **kwargs)

    def upload(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) from local.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        return self.sync_fs.url(self._cpath_str, _dest._cpath_str, recursive=recursive, callback=callback, **kwargs)

    def get_bucket(self, prefix: bool = True) -> Optional[str]:
        """
        Returns the root bucket with optional prefix
        """
        if not self.is_cloud: return None
        parts = self.split_path()
        uri_scheme = self._uri_scheme
        if prefix: return f'{uri_scheme}://' + parts[0]
        return parts[0]
    
    def get_bucket_path(self, prefix: bool = True) -> Optional[str]:
        """
        Returns the root bucket + path with optional prefix
        removes the versioning
        """
        if not self.is_cloud: return None
        parts = self.split_path()
        uri_scheme = self._uri_scheme
        p = '/'.join(parts[:2])
        if prefix: return f'{uri_scheme}://' + p
        return p
    
    def resolve(self, strict: bool = False):
        """
        Returns the abolute path.
        """
        if self.is_cloud: return self._new(self.as_posix())
        return self._new(self._PATH.abspath(self._path_str))

    @property
    def bucket(self) -> Optional[str]:
        if not self.is_cloud: return None
        parts = self.split_path()
        return parts[0]

    @property
    def bucket_path(self) -> Optional[str]:
        if not self.is_cloud: return None
        parts = self.split_path()
        return parts[1]

    @property
    def _path_str(self) -> str:
        """
        Returns the `__fspath__` string representation.
        """
        uri_scheme = self._uri_scheme
        if uri_scheme: return self._PATH.join(f'{uri_scheme}://', *self.parts[2:])
        else: return self._PATH.join(*self.parts) if self.parts else '.'
    
    @property
    def string(self) -> str:
        """
        Returns the extension for a file
        """
        return self._path_str

    @property
    def _filename_str(self) -> str:
        """
        Returns the filename if is file, else ''
        """
        if self.is_file(): return self.parts[-1]
        return ''

    @property
    def extension(self) -> str:
        """
        Returns the extension for a file
        """
        return self.suffix

    @property
    def _cpath_str(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        uri_scheme = self._uri_scheme
        if uri_scheme: return self._PATH.join(*self.parts[2:])
        else: return self._PATH.join(*self.parts) if self.parts else '.'


    @property
    def checksum(self):
        """
        Unique value for current version of file without refreshing
        """
        return self.get_checksum()
    
    @property
    def created(self):
        """
        Return the created timestamp of a file as a datetime
        """
        return self.get_checksum()
    

    @property
    def exist(self) -> bool:
        """
        Returns True if self exists.
        """
        return self.exists()

    @property
    def info(self):
        """
        Give details of entry at path
        """
        return self.sync_fs.info(self._cpath_str)
    
    
    @property
    def isdir(self) -> bool:
        """
        Returns True if self is a directory.
        """
        return self.is_dir()

    @property
    def isfile(self) -> bool:
        """
        Returns True if self is a file.
        """
        return self.is_file()

    @property
    def modified(self):
        """
        Return the last modified timestamp of file at path as a datetime without refreshing
        """
        return self.get_modified()
    
    @property
    def size(self):
        """
        Size in bytes of file
        """
        return self.sync_fs.size(self._cpath_str)
    
    @property
    def stat(self):
        """
        Give details of entry at path
        """
        return self.sync_fs.stat(self._cpath_str)
    
    @property
    def home(self) -> Optional[str]:
        """
        Returns the home directory/bucket
        """
        if self.is_cloud: return self._new(self.get_bucket(True))
        p = os.path.expanduser('~')
        return self._new(p)
    
    ###############################################################
    ####         Async Versions of the Sync Functions           ###
    ###############################################################

    def _get_afs_attr(self, name: str, default: Optional[Callable] = None):
        return getattr(self.async_fs, f'_{name}', getattr(self.async_fs, name, default))

    async def async_info(self):
        """
        Give details of entry at path
        """
        try: return await self._get_afs_attr('info')(self._cpath_str)
        except: return self.sync_fs.info(self._cpath_str)
        #return await self.async_fs._info(self._cpath_str)

    async def async_exists(self) -> bool:
        """
        Returns True if self exists.
        """
        try: return await self._get_afs_attr('exists')(self._cpath_str)
        except: return self.sync_fs.exists(self._cpath_str)
        #return await self.async_fs._exists(self._cpath_str)

    async def async_is_dir(self) -> bool:
        """
        Returns True if self is a directory.
        """
        try: return await self._get_afs_attr('info')(self._cpath_str)
        except: return self.sync_fs.info(self._cpath_str)
        #return await self.async_fs._isdir(self._cpath_str)

    async def async_is_file(self) -> bool:
        """Returns True if self is a file."""
        try: return await self._get_afs_attr('isfile')(self._cpath_str)
        except: return self.sync_fs.isfile(self._cpath_str)
        # return await self.async_fs._isfile(self._cpath_str)


    async def async_cat(self, recursive: bool = False, on_error: Optional[str] = None, **kwargs):
        """
        Fetch (potentially multiple) paths contents
        """
        try: return self._get_afs_attr('cat')(self._cpath_str, recursive, on_error, **kwargs)
        except: return self.sync_fs.cat(self._cpath_str, recursive, on_error, **kwargs)
        #return await self.async_fs._cat(self._cpath_str, recursive, on_error, **kwargs)
    
    async def async_cat_file(self, start: Optional[int] = None, end: Optional[int] = None, **kwargs):
        """
        Get the content of a file
        """
        return await self.async_fs._cat_file(self._cpath_str, start=start, end=end, **kwargs)

    async def async_copy(self, dest: base.PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False) -> _P:
        """
        Copies the File to the Dir/File.
        """
        _dest = self._get_dest(dest, recursive, overwrite, skip_errors)
        await self.async_fs._copy(self._cpath_str, _dest._cpath_str, recursive = recursive)
        return _dest

    
    async def async_download(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) to local.
        """
        _dest = self._get_dest(dest, recursive, overwrite, skip_errors)
        await self.async_fs._download(self._cpath_str, _dest._cpath_str, recursive = recursive, callback = callback)
        return _dest
    
    async def async_du(self, total: Optional[int] = None, maxdepth: Optional[int] = None, **kwargs):
        """
        Fetch (potentially multiple) paths contents
        """
        return await self.async_fs._du(self._cpath_str, total, maxdepth, **kwargs)
    
    async def async_expand_path(self, recursive: bool = False, maxdepth: Optional[int] = None):
        """
        Turn one or more globs or directories into a list of all matching paths to files or directories.
        """
        paths = await self.async_fs._expand_path(recursive = recursive, maxdepth = maxdepth)
        return [self._new(p) for p in paths]
    
    async def async_find(self, path: Optional[str] = None, maxdepth: Optional[int] = None, withdirs: bool = False, detail: bool = False, prefix: Optional[str] = None, **kwargs):
        """
        List all files below path.
        """
        p = self if not path else self.joinpath(path)
        return await self.async_fs._find(p, maxdepth=maxdepth, withdirs=withdirs, detail=detail, prefix=prefix, **kwargs)
    
    async def async_get_checksum(self, refresh: bool = False, **kwargs):
        """
        Unique value for current version of file
        """
        return await self.async_fs.checksum(self._cpath_str, **kwargs)
        #return self.async_fs.checksum(self._cpath_str, refresh=refresh, **kwargs)
    
    async def async_get_file(self, dest: base.PathLike, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy single remote file to local
        """
        _dest = self._get_dest(dest, overwrite=overwrite, skip_errors=skip_errors)
        return await self.async_fs._get_file(self._cpath_str, _dest._cpath_str, callback=callback, **kwargs)


    async def async_get_files(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) to local.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        return await self.async_fs._get(self._cpath_str, _dest._cpath_str, recursive=recursive, callback=callback, **kwargs)
        

    async def async_glob(self, pattern: str, **kwargs) -> Iterator[_P]:
        """
        Yielding all matching files (of any kind).
        """
        uri_scheme = self._uri_scheme
        for f in await self.async_fs._glob(self._PATH.join(self._path_str, pattern)._cpath_str,  **kwargs):
            if self.is_cloud: yield self._new(f'{uri_scheme}://' + f)
            else: yield self._new(f)

    async def async_head(self, size: Optional[int] = None):
        """ 
        Get the first size bytes from file
        """
        return self.async_fs.head(self._cpath_str, size)

    async def async_iterdir(self, **kwargs) -> Iterator[_P]:
        """
        Iterates over the directory.
        """
        uri_scheme = self._uri_scheme
        for f in await self.async_fs._glob(self._cpath_str, **kwargs):
            if self.is_cloud: yield self._new(f'{uri_scheme}://' + f)
            else: yield self._new(f)

    async def async_get_modified(self, version_id: str = None, refresh: bool = False, **kwargs):
        """
        Return the last modified timestamp of file at path as a datetime
        """
        return self.async_fs.modified(self._cpath_str, version_id=version_id, refresh=refresh, **kwargs)

    async def async_ls(self, detail: bool = False, **kwargs):
        """
        List objects at path.
        """
        return await self.async_fs._ls(self._cpath_str, detail=detail, **kwargs)

    
    async def async_mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """
        Create a new directory at this given path.
        """
        if self.exists() and exist_ok: return
        return await self.async_fs._mkdir(self._cpath_str, mode=mode, create_parents=parents, exist_ok=exist_ok)

    async def async_move(self, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Move file(s) from one location to another
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.async_fs.move(self._cpath_str, _dest._cpath_str, recursive = recursive, maxdepth=maxdepth)
        return _dest

    
    async def async_mv(self, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Move file(s) from one location to another
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.async_fs.move(self._cpath_str, _dest._cpath_str, recursive = recursive, maxdepth=maxdepth)
        return _dest

    async def async_open(self, mode: str = 'r', encoding: Optional[str] = base.DEFAULT_ENCODING, errors: Optional[str] = None, block_size: int = 5242880, compression: str = 'infer', **kwargs: Any) -> typing.IO[Union[str, bytes]]:
        """Opens the file."""
        filelike = await self.async_fs._open(self._cpath_str, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, **kwargs)
        filelike = typing.cast(typing.IO[Union[str, bytes]], filelike)
        return filelike

    async def async_put_file(self, dest: base.PathLike, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file from local.
        """
        _dest = self._get_dest(dest, overwrite=overwrite, skip_errors=skip_errors)
        await self.async_fs._put_file(self._cpath_str, _dest._cpath_str, callback = callback, **kwargs)
        return _dest
    
    async def async_put_files(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) from local.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        await self.async_fs._put(self._cpath_str, _dest._cpath_str, recursive=recursive, callback = callback, **kwargs)
        return _dest

    async def async_remove(self, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Delete files.
        """
        return await self.async_fs._rm(self._cpath_str, recursive=recursive, maxdepth = maxdepth, **kwargs)

    async def async_rename(self, dest: base.PathLike, recursive: bool = False, maxdepth: Optional[int] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Rename file or directory to the given target.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        self.async_fs.rename(self._cpath_str, _dest._cpath_str, recursive=recursive, maxdepth = maxdepth, **kwargs)
        return _dest

    async def async_replace(self, target: base.PathLike) -> _P:
        """
        Replace file or directory to the given target.
        """
        _dest = self._get_dest(target, overwrite=True)
        self.async_fs.rename(self._cpath_str, _dest._cpath_str)

    async def async_rm(self, recursive: bool = False, maxdepth: Optional[int] = None, **kwargs):
        """
        Delete files.
        """
        await self.async_fs._rm(self._cpath_str, recursive, maxdepth, **kwargs)
    
    async def async_rm_file(self, missing_ok: bool = True, **kwargs):
        """
        Delete a file
        """
        if not await self.async_exists():
            if missing_ok: return
            raise ValueError(f"{self._path_str} does not exist")
        await self.async_fs._rm_file(self._cpath_str, **kwargs)

    async def async_rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True) -> None:
        """
        Remove the empty directory.
        If force, will recursively remove even if not empty.
        """
        try:
            return self.async_fs.rmdir(self._cpath_str)
        except Exception as e:
            if force: return await self.async_fs._rm(self._cpath_str, recursive = recursive)
            if skip_errors: return
            raise e

    async def async_rmtree(self, recursive: bool = True, maxdepth: Optional[int] = None) -> None:
        """Remove the directory."""
        return await self.async_fs._rm(self._cpath_str, recursive, maxdepth)
    
    async def async_sign(self, expiration: int = 100, **kwargs):
        """
        Create a signed URL representing the given path
        Some implementations allow temporary URLs to be generated, as a way of delegating credentials
        """
        return self.async_fs.sign(self._cpath_str, expiration, **kwargs)
    
    async def async_size(self, **kwargs):
        """
        Create a signed URL representing the given path
        Some implementations allow temporary URLs to be generated, as a way of delegating credentials
        """
        return await self.async_fs._size(self._cpath_str, **kwargs)
    
    async def async_split_path(self, **kwargs) -> List[str]:
        """
        Normalise path string into bucket and key
        """
        return self.async_fs.split_path(self._cpath_str, **kwargs)
    
    async def async_tail(self, size: Optional[int] = None):
        """ 
        Get the last size bytes from file
        """
        return self.async_fs.tail(self._cpath_str, size)

    async def async_touch(self, truncate: bool = True, data = None, **kwargs):
        """
        Create empty file or truncate
        """
        return await self.async_fs.touch(self._cpath_str, truncate = truncate, data = data, **kwargs)
    
    async def async_unlink(self, missing_ok: bool = True, **kwargs) -> None:
        """
        Remove this file or symbolic link.
        """
        if getattr(self.async_fs, 'unlink', None): return self.async_fs.unlink(self._cpath_str, missing_ok = missing_ok, **kwargs)
        return await self.async_rm_file(missing_ok, **kwargs)
    
    async def async_url(self, expires: int = 3600, client_method: str = 'get_object', **kwargs):
        """
        Generate presigned URL to access path by HTTP
        """
        return self.async_fs.url(self._cpath_str, expires, client_method, **kwargs)

    async def async_upload(self, dest: base.PathLike, recursive: bool = False, callback: Optional[Callable] = None, overwrite: bool = False, skip_errors: bool = False, **kwargs):
        """
        Copy file(s) from local.
        """
        _dest = self._get_dest(dest, recursive=recursive, overwrite=overwrite, skip_errors=skip_errors)
        return self.async_fs.url(self._cpath_str, _dest._cpath_str, recursive=recursive, callback=callback, **kwargs)


class WindowsFSxPath(PosixFSxPath, pathlib.PureWindowsPath):
    _PATH = ntpath


class PosixIOPath(PosixFSxPath, pathlib.PurePosixPath):
    """
    Pathlib-like API around `upath` providing compatability with many i/o. Does not provide Async
    """
    _PATH = posixpath
    _FSX: 'upath.UPath' = None
    _SYNC_FS: 'upath.UPath' = None
    _ASYNC_FS: Any = None
    _FSX_LIB: str = 'upath'
    _FSX_MODULE: Optional[str] = None
    _FSX_CLS: str = 'UPath'


    @property
    def async_fs(self) -> 'AsyncFileSystem':
        raise NotImplementedError
    
    @property
    def sync_fs(self) -> 'upath.UPath':
        return upath.UPath


class WindowsIOPath(PosixIOPath, pathlib.PureWindowsPath):
    """
    Pathlib-like API around `upath` providing compatability with many i/o. Does not provide Async
    """
    _PATH = ntpath

    

os.PathLike.register(PosixFSxPath)
os.PathLike.register(WindowsFSxPath)

os.PathLike.register(PosixIOPath)
os.PathLike.register(WindowsIOPath)

