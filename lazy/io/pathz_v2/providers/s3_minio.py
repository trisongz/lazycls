import os

from .base import *
from ..flavours import _async_sync_windows_flavour, _async_sync_posix_flavour

if TYPE_CHECKING:
    import datetime

try: import s3fs
except ImportError: s3fs: ModuleType = None

try: from lazy.configz.cloudz import CloudAuthz
except ImportError: CloudAuthz: object = None


class _CFS:
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None

    @classmethod
    def is_ready(cls):
        return bool(cls.fsa and cls.fs)

    @classmethod
    def build_filesystems(cls, force: bool = False, **auth_config):
        """
        Lazily inits the filesystems
        """
        if cls.fs and cls.fsa and not force: return
        from lazy.libz import Lib
        from lazy.configz.cloudz import CloudAuthz
        #import importlib

        _s3fs: ModuleType = Lib.import_lib('s3fs')
        Lib.reload_module(s3fs)
        #importlib.reload(s3fs)

        authz = CloudAuthz()
        if auth_config: authz.update_authz(**auth_config)
        _config = {}
        
        if authz.minio_secret_key:
            _config['key'] = authz.minio_access_key
            _config['secret'] = authz.minio_secret_key
        elif authz.minio_access_token: _config['token'] = authz.minio_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.minio_endpoint}
        if authz.minio_config: _config['config_kwargs'] = authz.minio_config

        cls.fs = s3fs.S3FileSystem(**_config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))

    @classmethod
    def reload_filesystem(cls):
        """ 
        Reinitializes the Filesystem
        """
        #global _pathz_s3_accessor, _PathzS3Accessor
        global _PathzS3Accessor
        cls.build_filesystems(force=True)
        _PathzS3Accessor = _create_accessor()
        _pathz_s3_accessor = _PathzS3Accessor()


class _PathzS3Accessor(NormalAccessor):
    # Sync methods
    # For type checking... annoying
    if _CFS.is_ready():
        info = _CFS.fs.info
        stat = _CFS.fs.stat
        open = _CFS.fs.open
        listdir = _CFS.fs.ls
        exists = _CFS.fs.exists
        glob = _CFS.fs.glob
        is_dir = _CFS.fs.isdir
        is_file = _CFS.fs.isfile
        touch = _CFS.fs.touch
        copy = _CFS.fs.copy
        copy_file = staticmethod(_CFS.fs.cp_file)
        put = _CFS.fs.put
        put_file = staticmethod(_CFS.fs.put_file)

        ukey = _CFS.fs.ukey
        size = _CFS.fs.size
        url = _CFS.fs.url
        modified = _CFS.fs.modified
        metadata = _CFS.fs.metadata

        mkdir = _CFS.fs.mkdir
        makedirs = _CFS.fs.makedirs
        unlink = _CFS.fs.rm_file
        rmdir = _CFS.fs.rmdir

        rename = _CFS.fs.rename
        replace = _CFS.fs.rename
        remove = _CFS.fs.rm

        filesys = _CFS.fs
        async_filesys = _CFS.fsa

        # Async Methods
        async_info = _CFS.fsa.async_info
        async_stat = func_as_method_coro(_CFS.fs.stat)
        async_listdir = _CFS.fsa.async_list_objects
        async_exists = _CFS.fsa.async_exists
        async_glob = _CFS.fsa.async_glob
        async_is_dir = _CFS.fsa.async_isdir
        async_is_file = _CFS.fsa.async_isfile
        async_copy = _CFS.fsa.async_copy
        async_copy_file = _CFS.fsa.async_copy_file
        async_get = _CFS.fsa.async_get
        async_get_file = _CFS.fsa.async_get_file
        async_put = _CFS.fsa.async_put
        async_put_file = _CFS.fsa.async_put_file


        async_touch = func_as_method_coro(_CFS.fs.touch)
        async_ukey = func_as_method_coro(_CFS.fs.ukey)
        async_size = func_as_method_coro(_CFS.fs.size)
        async_url = func_as_method_coro(_CFS.fs.url)
        async_modified = func_as_method_coro(_CFS.fs.modified)
        async_metadata = func_as_method_coro(_CFS.fs.metadata)

        async_open = _CFS.fsa._open

        async_mkdir = _CFS.fsa.async_mkdir
        async_makedirs = _CFS.fsa.async_makedirs
        async_unlink = _CFS.fsa.async_rm_file
        async_rmdir = _CFS.fsa.async_rmdir

        async_rename = func_as_method_coro(_CFS.fs.rename)
        async_replace = func_as_method_coro(_CFS.fs.rename)
        async_remove = _CFS.fsa.async_rm
        async_touch = func_as_method_coro(_CFS.fs.touch)

## Recreates the class after 
## Fully initializing
## To allow for lazyloading of s3fs

def _create_accessor():
    class _PathzS3Accessor(NormalAccessor):
        # Sync methods
        info = _CFS.fs.info
        stat = _CFS.fs.stat
        open = _CFS.fs.open
        listdir = _CFS.fs.ls
        exists = _CFS.fs.exists
        glob = _CFS.fs.glob
        is_dir = _CFS.fs.isdir
        is_file = _CFS.fs.isfile
        touch = _CFS.fs.touch
        copy = _CFS.fs.copy
        copy_file = staticmethod(_CFS.fs.cp_file)
        get = _CFS.fs.get
        get_file = staticmethod(_CFS.fs.get_file)
        put = _CFS.fs.put
        put_file = staticmethod(_CFS.fs.put_file)

        ukey = _CFS.fs.ukey
        size = _CFS.fs.size
        url = _CFS.fs.url
        modified = _CFS.fs.modified
        metadata = _CFS.fs.metadata

        mkdir = _CFS.fs.mkdir
        makedirs = _CFS.fs.makedirs
        unlink = _CFS.fs.rm_file
        rmdir = _CFS.fs.rmdir

        rename = _CFS.fs.rename
        replace = _CFS.fs.rename
        remove = _CFS.fs.rm

        filesys = _CFS.fs
        async_filesys = _CFS.fsa

        # Async Methods
        async_info = _CFS.fsa.async_info
        async_stat = func_as_method_coro(_CFS.fs.stat)
        async_listdir = _CFS.fsa.async_list_objects
        async_exists = _CFS.fsa.async_exists
        async_glob = _CFS.fsa.async_glob
        async_is_dir = _CFS.fsa.async_isdir
        async_is_file = _CFS.fsa.async_isfile
        async_copy = _CFS.fsa.async_copy
        async_copy_file = _CFS.fsa.async_cp_file
        async_get = _CFS.fsa.async_get
        async_get_file = _CFS.fsa.async_get_file
        async_put = _CFS.fsa.async_put
        async_put_file = _CFS.fsa.async_put_file


        async_touch = func_as_method_coro(_CFS.fs.touch)
        async_ukey = func_as_method_coro(_CFS.fs.ukey)
        async_size = func_as_method_coro(_CFS.fs.size)
        async_url = func_as_method_coro(_CFS.fs.url)
        async_modified = func_as_method_coro(_CFS.fs.modified)
        async_metadata = func_as_method_coro(_CFS.fs.metadata)

        async_open = _CFS.fsa._open

        async_mkdir = _CFS.fsa.async_mkdir
        async_makedirs = _CFS.fsa.async_makedirs
        async_unlink = _CFS.fsa.async_rm_file
        async_rmdir = _CFS.fsa.async_rmdir

        async_rename = func_as_method_coro(_CFS.fs.rename)
        async_replace = func_as_method_coro(_CFS.fs.rename)
        async_remove = _CFS.fsa.async_rm
        async_touch = func_as_method_coro(_CFS.fs.touch)
    return _PathzS3Accessor


_pathz_s3_accessor: _PathzS3Accessor = None

def _get_accessor(**kwargs) -> _PathzS3Accessor:
    global _pathz_s3_accessor, _PathzS3Accessor
    if not _pathz_s3_accessor:
        _CFS.build_filesystems(**kwargs)
        _PathzS3Accessor = _create_accessor()
        _pathz_s3_accessor = _PathzS3Accessor()
    return _pathz_s3_accessor


class PathzS3MinioPurePath(PurePath):
    def _init(self, template: Optional[PurePath] = None):
        self._accessor: _PathzS3Accessor = _get_accessor()

    def __new__(cls, *args):
        if cls is PathzS3MinioPurePath: cls = PurePathzS3MinioWindowsPath if os.name == 'nt' else PurePathzS3MinioPosixPath
        return cls._from_parts(args)

    def _new(self, *parts):
        """Create a new `Path` child of same type."""
        return type(self)(*parts)


class PurePathzS3MinioPosixPath(PathzS3MinioPurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_sync_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PurePathzS3MinioWindowsPath(PathzS3MinioPurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_sync_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

from .s3_aws import PathzS3Path

class PathzS3MinioPath(PathzS3MinioPurePath, PathzS3Path):
    _flavour = _async_sync_windows_flavour if os.name == 'nt' else _async_sync_posix_flavour
    _accessor: _PathzS3Accessor = _get_accessor()
    _pathlike = posixpath
    _prefix = 'minio'
    _provider = 'MinIO'

    def _init(self, template: Optional['PathzS3MinioPath'] = None):
        self._accessor: _PathzS3Accessor = _get_accessor()
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is PathzS3MinioPath: cls = PathzS3MinioWindowsPath if os.name == 'nt' else PathzS3MinioPosixPath
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")
        self._init()
        return self



class PathzS3MinioPosixPath(PosixPath, PathzS3Path, PurePathzS3MinioPosixPath):
    __slots__ = ()


class PathzS3MinioWindowsPath(WindowsPath, PathzS3Path, PurePathzS3MinioWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("PathzS3MinioPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("PathzS3MinioPath.async_is_mount() is unsupported on this system")


os.PathLike.register(PathzS3MinioPurePath)
os.PathLike.register(PathzS3MinioPath)
os.PathLike.register(PurePathzS3MinioPosixPath)
os.PathLike.register(PathzS3MinioWindowsPath)
os.PathLike.register(PathzS3MinioPosixPath)
os.PathLike.register(PurePathzS3MinioWindowsPath)

MinioFileSystem = _CFS


__all__ = (
    'PathzS3MinioPurePath',
    'PathzS3MinioPath',
    'PurePathzS3MinioPosixPath',
    'PathzS3MinioWindowsPath',
    'PathzS3MinioPosixPath',
    'PurePathzS3MinioWindowsPath',
    'MinioFileSystem'
)
