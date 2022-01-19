from __future__ import annotations
import os
from .base import *


from .cfs_base import AWS_CFS
from .cfs_pathz_base import *


class PathzS3PurePath(PathzCFSPurePath):
    _prefix: str = 's3'
    _provider: str = 'AmazonS3'
    _win_pathz: ClassVar = 'PurePathzS3WindowsPath'
    _posix_pathz: ClassVar = 'PurePathzS3PosixPath'


class PurePathzS3PosixPath(PurePathzCFSPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PurePathzS3WindowsPath(PurePathzCFSWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

class PathzS3Path(PathzCFSPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 's3'
    _provider = 'AmazonS3'

    _win_pathz: ModuleType = 'PathzS3WindowsPath'
    _posix_pathz: ModuleType = 'PathzS3PosixPath'

    def _init(self, template: Optional['PathzS3Path'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is PathzS3Path or issubclass(PathzS3Path): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self


class PathzS3PosixPath(PosixPath, PathzS3Path, PurePathzS3PosixPath):
    __slots__ = ()


class PathzS3WindowsPath(WindowsPath, PathzS3Path, PurePathzS3WindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("PathzS3Path.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("PathzS3Path.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        PathzS3PurePath, PathzS3Path, PurePathzS3PosixPath, PathzS3WindowsPath, PathzS3PosixPath, PurePathzS3WindowsPath
    ]
)

AWSFileSystem = AWS_CFS


__all__ = (
    'PathzS3PurePath',
    'PathzS3Path',
    'PurePathzS3PosixPath',
    'PathzS3WindowsPath',
    'PathzS3PosixPath',
    'PurePathzS3WindowsPath',
    'AWSFileSystem'
)
