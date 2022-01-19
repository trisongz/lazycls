from __future__ import annotations

import os

from .base import *
from .cfs_base import Minio_CFS
from .cfs_pathz_base import *


class PathzMinioPurePath(PathzCFSPurePath):
    _prefix: str = 'minio'
    _provider: str = 'Minio'
    _win_pathz: ClassVar = 'PurePathzMinioWindowsPath'
    _posix_pathz: ClassVar = 'PurePathzMinioPosixPath'


class PurePathzMinioPosixPath(PurePathzCFSPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PurePathzMinioWindowsPath(PurePathzCFSWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()

class PathzMinioPath(PathzCFSPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 's3'
    _provider = 'Minio'

    _win_pathz: ModuleType = 'PathzMinioWindowsPath'
    _posix_pathz: ModuleType = 'PathzMinioPosixPath'

    def _init(self, template: Optional['PathzMinioPath'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is PathzMinioPath or issubclass(PathzMinioPath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self


class PathzMinioPosixPath(PosixPath, PathzMinioPath, PurePathzMinioPosixPath):
    __slots__ = ()


class PathzMinioWindowsPath(WindowsPath, PathzMinioPath, PurePathzMinioWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("PathzMinioPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("PathzMinioPath.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        PathzMinioPurePath, PathzMinioPath, PurePathzMinioPosixPath, PathzMinioWindowsPath, PathzMinioPosixPath, PurePathzMinioWindowsPath
    ]
)

MinioFileSystem = Minio_CFS


__all__ = (
    'PathzMinioPurePath',
    'PathzMinioPath',
    'PurePathzMinioPosixPath',
    'PathzMinioWindowsPath',
    'PathzMinioPosixPath',
    'PurePathzMinioWindowsPath',
    'MinioFileSystem'
)
