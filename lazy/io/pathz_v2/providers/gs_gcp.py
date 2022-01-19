from __future__ import annotations
import os
from .base import *


from .cfs_base import GCP_CFS
from .cfs_pathz_base import *


class PathzGSPurePath(PathzCFSPurePath):
    _prefix: str = 'gs'
    _provider: str = 'GoogleCloudStorage'
    _win_pathz: ClassVar = 'PurePathzGSWindowsPath'
    _posix_pathz: ClassVar = 'PurePathzGSPosixPath'


class PurePathzGSPosixPath(PurePathzCFSPosixPath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()
    _prefix = 'gs'
    _provider = 'GoogleCloudStorage'


class PurePathzGSWindowsPath(PurePathzCFSWindowsPath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()
    _prefix = 'gs'
    _provider = 'GoogleCloudStorage'

class PathzGSPath(PathzCFSPath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = 'gs'
    _provider = 'GoogleCloudStorage'

    _win_pathz: ModuleType = 'PathzGSWindowsPath'
    _posix_pathz: ModuleType = 'PathzGSPosixPath'

    def _init(self, template: Optional['PathzGSPath'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is PathzGSPath or issubclass(PathzGSPath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        self = cls._from_parts(parts, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self


class PathzGSPosixPath(PosixPath, PathzGSPath, PurePathzGSPosixPath):
    __slots__ = ()


class PathzGSWindowsPath(WindowsPath, PathzGSPath, PurePathzGSWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("PathzGSPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("PathzGSPath.async_is_mount() is unsupported on this system")

register_pathlike(
    [
        PathzGSPurePath, PathzGSPath, PurePathzGSPosixPath, PathzGSWindowsPath, PathzGSPosixPath, PurePathzGSWindowsPath
    ]
)

GCPFileSystem = GCP_CFS


__all__ = (
    'PathzGSPurePath',
    'PathzGSPath',
    'PurePathzGSPosixPath',
    'PathzGSWindowsPath',
    'PathzGSPosixPath',
    'PurePathzGSWindowsPath',
    'GCPFileSystem'
)
