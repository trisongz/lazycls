from __future__ import annotations

import os
import io

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

from lazy.serialize import Serialize
from .types import *
from ._flavours import _async_sync_windows_flavour, _async_sync_posix_flavour

DEFAULT_ENCODING: Final[str] = 'utf-8'
ON_ERRORS: Final[str] = 'ignore'
NEWLINE: Final[str] = '\n'


"""
Taken from https://github.com/alexdelorenzo/aiopath/blob/Python-3.9/aiopath/path.py
"""


def scandir_sync(*args, **kwargs) -> Iterable[EntryWrapper]:
    results = _scandir_results(*args, **kwargs)
    yield from results

Paths = Union[Path, PathLike, str]
close = func_to_async_func(os.close)
sync_close = os.close


class _AsyncAccessor(_NormalAccessor):
    stat = func_as_method_coro(os.stat)
    lstat = func_as_method_coro(os.lstat)
    open = func_as_method_coro(os.open)
    listdir = func_as_method_coro(os.listdir)
    chmod = func_as_method_coro(os.chmod)

    if hasattr(_NormalAccessor, 'lchmod'):
        lchmod = method_as_method_coro(_NormalAccessor.lchmod)

    mkdir = func_as_method_coro(os.mkdir)
    unlink = func_as_method_coro(os.unlink)

    if hasattr(_NormalAccessor, 'link'):
        link = method_as_method_coro(_NormalAccessor.link)

    rmdir = func_as_method_coro(os.rmdir)
    rename = func_as_method_coro(os.rename)
    replace = func_as_method_coro(os.replace)
    symlink = staticmethod(method_as_method_coro(_NormalAccessor.symlink))
    utime = func_as_method_coro(os.utime)
    readlink = method_as_method_coro(_NormalAccessor.readlink)
    remove = func_as_method_coro(os.remove)

    async def owner(self, path: str) -> str:
        try:
            import pwd
            stat = await self.stat(path)
            return pwd.getpwuid(stat.st_uid).pw_name

        except ImportError:
            raise NotImplementedError("Path.owner() is unsupported on this system")

    async def group(self, path: str) -> str:
        try:
            import grp

            stat = await self.stat(path)
            return grp.getgrgid(stat.st_gid).gr_name

        except ImportError:
            raise NotImplementedError("Path.group() is unsupported on this system")

    async def scandir(self, *args, **kwargs) -> AsyncIterable[EntryWrapper]:
        async for entry in scandir_async(*args, **kwargs):
            yield entry


class _AsyncSyncAccessor(_NormalAccessor):
    # Sync methods
    stat = os.stat
    lstat = os.lstat
    open = os.open
    listdir = os.listdir
    chmod = os.chmod

    # Async Methods
    async_stat = func_as_method_coro(os.stat)
    async_lstat = func_as_method_coro(os.lstat)
    async_open = func_as_method_coro(os.open)
    async_listdir = func_as_method_coro(os.listdir)
    async_chmod = func_as_method_coro(os.chmod)

    if hasattr(_NormalAccessor, 'lchmod'):
        lchmod = _NormalAccessor.lchmod
        async_lchmod = method_as_method_coro(_NormalAccessor.lchmod)

    mkdir = os.mkdir
    unlink = os.unlink

    async_mkdir = func_as_method_coro(os.mkdir)
    async_unlink = func_as_method_coro(os.unlink)

    if hasattr(_NormalAccessor, 'link'):
        link = _NormalAccessor.link
        async_link = method_as_method_coro(_NormalAccessor.link)

    rmdir = os.rmdir
    rename = os.rename
    replace = os.replace
    symlink = staticmethod(_NormalAccessor.symlink)
    utime = os.utime
    readlink = _NormalAccessor.readlink
    remove = os.remove

    async_rmdir = func_as_method_coro(os.rmdir)
    async_rename = func_as_method_coro(os.rename)
    async_replace = func_as_method_coro(os.replace)
    async_symlink = staticmethod(method_as_method_coro(_NormalAccessor.symlink))
    async_utime = func_as_method_coro(os.utime)
    async_readlink = method_as_method_coro(_NormalAccessor.readlink)
    async_remove = func_as_method_coro(os.remove)


    def owner(self, path: str) -> str:
        try:
            import pwd
            stat = self.stat(path)
            return pwd.getpwuid(stat.st_uid).pw_name

        except ImportError:
            raise NotImplementedError("Path.owner() is unsupported on this system")

    async def async_owner(self, path: str) -> str:
        try:
            import pwd
            stat = await self.async_stat(path)
            return pwd.getpwuid(stat.st_uid).pw_name
        except ImportError: raise NotImplementedError("Path.owner() is unsupported on this system")

    def group(self, path: str) -> str:
        try:
            import grp
            stat = self.stat(path)
            return grp.getgrgid(stat.st_gid).gr_name

        except ImportError: raise NotImplementedError("Path.group() is unsupported on this system")

    async def async_group(self, path: str) -> str:
        try:
            import grp
            stat = await self.async_stat(path)
            return grp.getgrgid(stat.st_gid).gr_name

        except ImportError: raise NotImplementedError("Path.group() is unsupported on this system")

    def scandir(self, *args, **kwargs) -> Iterable[EntryWrapper]:
        yield from scandir_sync(*args, **kwargs)

    async def async_scandir(self, *args, **kwargs) -> AsyncIterable[EntryWrapper]:
        async for entry in scandir_async(*args, **kwargs):
            yield entry

_async_accessor = _AsyncAccessor()
_async_sync_accessor = _AsyncSyncAccessor()


class AsyncPurePath(PurePath):
    def _init(self, template: Optional[PurePath] = None):
        self._accessor = _async_accessor

    def __new__(cls, *args):
        if cls is AsyncPurePath: cls = PureAsyncWindowsPath if os.name == 'nt' else PureAsyncPosixPath
        return cls._from_parts(args)

class AsyncSyncPurePath(PurePath):
    def _init(self, template: Optional[PurePath] = None):
        self._accessor = _async_sync_accessor

    def __new__(cls, *args):
        if cls is AsyncSyncPurePath: cls = PureAsyncSyncWindowsPath if os.name == 'nt' else PureAsyncSyncPosixPath
        return cls._from_parts(args)

class PureAsyncPosixPath(AsyncPurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_posix_flavour
    __slots__ = ()

class PureAsyncSyncPosixPath(AsyncSyncPurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_sync_posix_flavour
    __slots__ = ()


class PureAsyncWindowsPath(AsyncPurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_windows_flavour
    __slots__ = ()


class PureAsyncSyncWindowsPath(AsyncSyncPurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _async_sync_windows_flavour
    __slots__ = ()



class AsyncSyncPath(Path, AsyncSyncPurePath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _async_sync_windows_flavour if os.name == 'nt' else _async_sync_posix_flavour
    _accessor = _async_sync_accessor

    def _init(self, template: Optional[AsyncSyncPath] = None):
        self._accessor = _async_sync_accessor
        self._closed = False
        self._fileio = None

    def __new__(cls, *args, **kwargs):
        if cls is AsyncSyncPath: cls = AsyncSyncWindowsPath if os.name == 'nt' else AsyncSyncPosixPath

        self = cls._from_parts(args, init=False)
        if not self._flavour.is_supported:
            name: str = cls.__name__
            raise NotImplementedError(f"cannot instantiate {name} on your system")

        self._init()
        return self

    @property
    def _path(self) -> str:
        return str(self)
    
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
        return False
    

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
    def home_(self) -> AsyncSyncPath:
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
    async def async_home_(self) -> AsyncSyncPath:
        return await self.async_home()
    
    
    def open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)

    
    def async_open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)
    
    def reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)
    
    def async_reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)
    
    def appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)
    
    def async_appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)
    
    def writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        if self._closed: self._raise_closed()
        return io.open(self, mode, buffering, encoding, errors, newline, opener=self._opener)
    
    def async_writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, **kwargs) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_handle(self._path, mode, encoding=encoding, errors=errors, newline=newline)

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

    def readlink(self) -> AsyncSyncPath:
        """
        Return the path to which the symbolic link points.
        """
        path: str = self._accessor.readlink(self)
        obj = self._from_parts((path,), init=False)
        obj._init(template=self)
        return obj

    async def async_readlink(self) -> AsyncSyncPath:
        """
        Return the path to which the symbolic link points.
        """
        path: str = await self._accessor.async_readlink(self)
        obj = self._from_parts((path,), init=False)
        obj._init(template=self)
        return obj

    def _opener(self, name, flags, mode=0o666):
        # A stub for the opener argument to built-in open()
        return self._accessor.open(self, flags, mode)
    
    def _async_opener(self, name, flags, mode=0o666):
        # A stub for the opener argument to built-in open()
        return self._accessor.async_open(self, flags, mode)

    def _raw_open(self, flags: int, mode: int = 0o777) -> int:
        """
        Open the file pointed by this path and return a file descriptor,
        as os.open() does.
        """
        if self._closed: self._raise_closed()
        return self._accessor.open(self, flags, mode)

    async def _async_raw_open(self, flags: int, mode: int = 0o777) -> int:
        """
        Open the file pointed by this path and return a file descriptor,
        as os.open() does.
        """
        return await self._accessor.async_open(self, flags, mode)

    def touch(self, mode: int = 0o666, exist_ok: bool = True):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: self._accessor.utime(self, None)
            # Avoid exception chaining
            except OSError: pass
            else: return

        flags: int = os.O_CREAT | os.O_WRONLY
        if not exist_ok: flags |= os.O_EXCL
        fd = self._raw_open(flags, mode)
        sync_close(fd)

    async def async_touch(self, mode: int = 0o666, exist_ok: bool = True):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if exist_ok:
            try: await self._accessor.async_utime(self, None)
            # Avoid exception chaining
            except OSError: pass
            else: return

        flags: int = os.O_CREAT | os.O_WRONLY
        if not exist_ok: flags |= os.O_EXCL
        fd = await self._async_raw_open(flags, mode)
        await close(fd)

    def mkdir(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: self._accessor.mkdir(self, mode)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            self.parent.mkdir(parents=True, exist_ok=True)
            self.mkdir(mode, parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not self.is_dir(): raise

    async def async_mkdir(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: await self._accessor.async_mkdir(self, mode)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            await self.parent.async_mkdir(parents=True, exist_ok=True)
            await self.async_mkdir(mode, parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not await self.async_is_dir(): raise

    async def chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        self._accessor.chmod(self, mode)

    async def async_chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        await self._accessor.async_chmod(self, mode)

    def lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        self._accessor.lchmod(self, mode)

    async def lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        await self._accessor.async_lchmod(self, mode)

    def unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: self._accessor.unlink(self)
        except FileNotFoundError:
            if not missing_ok: raise

    async def async_unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: await self._accessor.async_unlink(self)
        except FileNotFoundError:
            if not missing_ok: raise

    def rmdir(self):
        """
        Remove this directory.  The directory must be empty.
        """
        self._accessor.rmdir(self)

    async def async_rmdir(self):
        """
        Remove this directory.  The directory must be empty.
        """
        await self._accessor.async_rmdir(self)

    def link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        self._accessor.link_to(self, target)
    
    async def async_link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        await self._accessor.async_link_to(self, target)

    def rename(self, target: Union[str, AsyncSyncPath]) -> AsyncSyncPath:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.rename(self, target)
        return type(self)(target)
    
    async def async_rename(self, target: Union[str, AsyncSyncPath]) -> AsyncSyncPath:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_rename(self, target)
        return type(self)(target)

    def replace(self, target: str) -> AsyncSyncPath:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.replace(self, target)
        return type(self)(target)
    
    async def async_replace(self, target: str) -> AsyncSyncPath:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_replace(self, target)
        return type(self)(target)

    def symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        self._accessor.symlink(target, self, target_is_directory)
    
    async def async_symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        await self._accessor.async_symlink(target, self, target_is_directory)

    def exists(self) -> bool:
        """
        Whether this path exists.
        """
        try: self.stat()
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
        return True

    async def async_exists(self) -> bool:
        """
        Whether this path exists.
        """
        try: await self.async_stat()
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
        return True

    @classmethod
    def cwd(cls: type) -> str:
        """Return a new path pointing to the current working directory
        (as returned by os.getcwd()).
        """
        cwd: str = os.getcwd()
        return cls(cwd)

    @classmethod
    def home(cls: type) -> AsyncSyncPath:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        homedir: str = cls()._flavour.gethomedir(None)
        return cls(homedir)

    @classmethod
    async def async_home(cls: type) -> AsyncSyncPath:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        coro = cls()._flavour.async_gethomedir(None)
        homedir: str = await coro
        return cls(homedir)

    def samefile(self, other_path: Union[AsyncSyncPath, Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = AsyncSyncPath(other_path)
        if isinstance(other_path, AsyncSyncPath):
            try: other_st = other_path.stat()
            except AttributeError: other_st = self._accessor.stat(other_path)

        else:
            try: other_st = other_path.stat()
            except AttributeError: other_st = other_path._accessor.stat(other_path)
        return os.path.samestat(self.stat(), other_st)

    async def async_samefile(self, other_path: Union[AsyncSyncPath, Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = AsyncSyncPath(other_path)
        if isinstance(other_path, AsyncSyncPath):
            try: other_st = await other_path.async_stat()
            except AttributeError: other_st = await self._accessor.async_stat(other_path)

        else:
            try: other_st = await to_thread(other_path.stat)
            except AttributeError: other_st = await to_thread(other_path._accessor.stat, other_path)

        return os.path.samestat(await self.async_stat(),other_st)

    def iterdir(self) -> Iterable[AsyncSyncPath]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in self._accessor.listdir(self):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    async def async_iterdir(self) -> AsyncIterable[AsyncSyncPath]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in await self._accessor.async_listdir(self):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    def glob(self, pattern: str) -> Iterable[AsyncSyncPath]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))

        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        selector = _sync_make_selector(tuple(pattern_parts), self._flavour)
        yield from selector.select_from(self)

    async def async_glob(self, pattern: str) -> AsyncIterable[AsyncSyncPath]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))

        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        selector = _make_selector(tuple(pattern_parts), self._flavour)
        async for p in selector.select_from(self):
            yield p

    def rglob(self, pattern: str) -> Iterable[AsyncSyncPath]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))

        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        parts = ("**", *pattern_parts)
        selector = _sync_make_selector(parts, self._flavour)
        yield from selector.select_from(self)

    async def async_rglob(self, pattern: str) -> AsyncIterable[AsyncSyncPath]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        drv, root, pattern_parts = self._flavour.parse_parts((pattern,))
        if drv or root: raise NotImplementedError("Non-relative patterns are unsupported")
        parts = ("**", *pattern_parts)
        selector = _make_selector(parts, self._flavour)
        async for p in selector.select_from(self):
            yield p

    def absolute(self) -> AsyncSyncPath:
        """Return an absolute version of this path.  This function works
        even if the path doesn't point to anything.
        No normalization is done, i.e. all '.' and '..' will be kept along.
        Use resolve() to get the canonical path to a file.
        """
        if self.is_absolute(): return self
        obj = self._from_parts([os.getcwd()] + self._parts, init=False)
        obj._init(template=self)
        return obj

    def resolve(self, strict: bool = False) -> AsyncSyncPath:
        """
        Make the path absolute, resolving all symlinks on the way and also
        normalizing it (for example turning slashes into backslashes under
        Windows).
        """
        s: Optional[str] = self._flavour.resolve(self, strict=strict)

        if s is None:
            self.stat()
            path = self.absolute()
            s = str(path)

        # Now we have no symlinks in the path, it's safe to normalize it.
        normed: str = self._flavour.pathmod.normpath(s)
        obj = self._from_parts((normed,), init=False)
        obj._init(template=self)
        return obj

    async def async_resolve(self, strict: bool = False) -> AsyncSyncPath:
        """
        Make the path absolute, resolving all symlinks on the way and also
        normalizing it (for example turning slashes into backslashes under
        Windows).
        """
        s: Optional[str] = await self._flavour.async_resolve(self, strict=strict)

        if s is None:
            await self.async_stat()
            path = await self.absolute()
            s = str(path)

        # Now we have no symlinks in the path, it's safe to normalize it.
        normed: str = self._flavour.pathmod.normpath(s)
        obj = self._from_parts((normed,), init=False)
        obj._init(template=self)
        return obj

    def stat(self) -> stat_result:
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        return self._accessor.stat(self)
    
    async def async_stat(self) -> stat_result:
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        return await self._accessor.async_stat(self)

    def lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        return self._accessor.lstat(self)
    
    async def async_lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        return await self._accessor.async_lstat(self)

    def owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        return self._accessor.owner(self)
    
    async def async_owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        return await self._accessor.async_owner(self)

    def group(self) -> str:
        """
        Return the group name of the file gid.
        """
        return self._accessor.group(self)
    
    async def async_group(self) -> str:
        """
        Return the group name of the file gid.
        """
        return await self._accessor.async_group(self)

    def is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        try:
            stat = self.stat()
            return S_ISDIR(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False
    
    async def async_is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        try:
            stat = await self.async_stat()
            return S_ISDIR(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        try:
            lstat = self.lstat()
            return S_ISLNK(lstat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False
    
    async def async_is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        try:
            lstat = await self.async_lstat()
            return S_ISLNK(lstat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try:
            stat = self.stat()
            return S_ISREG(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    async def async_is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try:
            stat = await self.async_stat()
            return S_ISREG(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not self.exists() or not self.is_dir(): return False
        
        try:
            parent_stat = self.parent.stat()
            parent_dev = parent_stat.st_dev
        except OSError: return False

        stat = self.stat()
        dev = stat.st_dev
        if dev != parent_dev: return True
        ino = stat.st_ino
        parent_ino = parent_stat.st_ino
        return ino == parent_ino

    async def async_is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not await self.async_exists() or not await self.async_is_dir(): return False
        
        try:
            parent_stat = await self.parent.async_stat()
            parent_dev = parent_stat.st_dev
        except OSError: return False

        stat = await self.async_stat()
        dev = stat.st_dev
        if dev != parent_dev: return True
        ino = stat.st_ino
        parent_ino = parent_stat.st_ino
        return ino == parent_ino

    def is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        try:
            stat = self.stat()
            return S_ISBLK(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    async def async_is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        try:
            stat = await self.async_stat()
            return S_ISBLK(stat.st_mode)

        except OSError as e:
            if not _ignore_error(e): raise
            return False

        except ValueError: return False

    def is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        try:
            stat = self.stat()
            return S_ISCHR(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
    
    async def async_is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        try:
            stat = await self.stat()
            return S_ISCHR(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    def is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        try:
            stat = self.stat()
            return S_ISFIFO(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    async def async_is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        try:
            stat = await self.async_stat()
            return S_ISFIFO(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    def is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        try:
            stat = self.stat()
            return S_ISSOCK(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False
    
    async def async_is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        try:
            stat = await self.async_stat()
            return S_ISSOCK(stat.st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    def expanduser(self) -> AsyncSyncPath:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = self._flavour.gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self
    
    async def async_expanduser(self) -> AsyncSyncPath:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = await self._flavour.async_gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self

    def iterdir(self) -> Iterable[AsyncSyncPath]:
        names = self._accessor.listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)
    
    async def async_iterdir(self) -> AsyncIterable[AsyncSyncPath]:
        names = await self._accessor.async_listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)

    def _raise_closed(self):
        raise ValueError("I/O operation on closed path")
    
    def _raise_open(self):
        raise ValueError("I/O operation on already open path")
    
    # We sort of assume that it will be used to open a file
    def __enter__(self):
        #if self._fileio: self._raise_open()
        #if not self._fileio:
        #    self._fileio = self.open()
        if self._closed: self._raise_closed()
        return self

    def __exit__(self, t, v, tb):
        self._closed = True
    
    async def __aenter__(self):
        if self._closed: self._raise_closed()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._closed = True
    
    """
    Custom Serialization Class
    """
    def read_json(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        """
        Reads JSON
        """
        return Serialize.Json.loads(self.read_text(encoding = encoding), **kwargs)
        
    def read_jsonlines(self,  mode: str = 'r', skip_errors: bool = True, as_iterable: bool = True, **kwargs) -> Iterator[T]:
        """
        Reads JSON Lines
        """
        with self.open(mode=mode) as f:
            return Serialize.Json.readlines(f, as_iterable = as_iterable, skip_errors = skip_errors, **kwargs)
    
    def read_yaml(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        """
        Reads YAML
        """
        return Serialize.Yaml.loads(self.read_text(encoding = encoding), **kwargs)
    
    def read_pickle(self, mode: str = 'rb', **kwargs):
        """
        Reads Pickle File
        """
        with self.open(mode=mode) as f:
            return Serialize.Pkl.loads(f.read(), **kwargs)

    def append_jsonlines(self, data: List[JsonType], encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        """
        Appends JSON Lines to File
        """
        if ensure_file_exists and not self.exists(): self.touch()
        with self.open(mode='a', encoding=encoding) as f:
            Serialize.Json.write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)

    def write_json(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, ensure_ascii: bool = False, indent: int = 2, **kwargs) -> None:
        """
        Writes JSON to File
        """
        with self.open('w', encoding = encoding) as f:
            f.write(Serialize.Json.dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
    
    def write_jsonlines(self, data: List[JsonType], append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        """
        Writes JSON Lines to File
        """
        if ensure_file_exists and not self.exists(): self.touch()
        mode = 'a' if (append and self.exists()) or ensure_file_exists else 'w'
        with self.open(mode=mode, encoding=encoding) as f:
            Serialize.Json.write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)

    def write_yaml(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> None:
        """
        Writes YAML to File
        """
        with self.open('w', encoding = encoding) as f:
            f.write(Serialize.Yaml.dumps(data, **kwargs))

    def write_pickle(self, obj: Any, **kwargs) -> None:
        """
        Writes Pickle to File
        """
        data = Serialize.Pkl.dumps(obj, **kwargs)
        return self.write_bytes(data)
    
    """
    Async Custom Serialization Class
    """
    async def async_read_json(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        """
        Reads JSON Asyncronously
        """
        return await Serialize.Json.async_loads(await self.async_read_text(encoding = encoding), **kwargs)
        
    async def async_read_jsonlines(self, mode: str = 'r', skip_errors: bool = True, as_iterable: bool = True, **kwargs) -> Iterator[T]:
        """
        Reads JSON Lines Asyncronously
        """
        async with self.async_open(mode=mode) as f:
            return await Serialize.Json.async_readlines(f, as_iterable = as_iterable, skip_errors = skip_errors, **kwargs)
    
    async def async_read_yaml(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        """
        Reads YAML Asyncronously
        """
        return await Serialize.Yaml.async_loads(await self.async_read_text(encoding = encoding), **kwargs)
    
    async def async_read_pickle(self, mode: str = 'rb', **kwargs):
        """
        Reads Pickle File Asyncronously
        """
        async with self.async_open(mode=mode) as f:
            return await Serialize.Pkl.async_loads(await f.read(), **kwargs)

    async def async_append_jsonlines(self, data: List[JsonType], encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        """
        Appends JSON Lines to File Asyncronously
        """
        if ensure_file_exists and not await self.async_exists_: await self.async_touch()
        async with self.async_open(mode='a', encoding = encoding) as f:
            await Serialize.Json.write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)

    async def async_write_json(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, ensure_ascii: bool = False, indent: int = 2, **kwargs) -> None:
        """
        Writes JSON to File Asyncronously
        """
        async with self.async_open('w', encoding = encoding) as f:
            await f.write(await Serialize.Json.async_dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
    
    async def async_write_jsonlines(self, data: List[JsonType], append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        """
        Writes JSON Lines to File Asyncronously
        """
        if ensure_file_exists and not await self.async_exists_: await self.async_touch()
        mode = 'a' if (append and await self.async_exists_) or ensure_file_exists else 'w'
        async with self.async_open(mode=mode, encoding=encoding) as f:
            await Serialize.Json.async_write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)

    async def async_write_yaml(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> None:
        """
        Writes YAML to File Asyncronously
        """ 
        async with self.async_open('w', encoding = encoding) as f:
            await f.write(await Serialize.Yaml.async_dumps(data, **kwargs))

    async def async_write_pickle(self, obj: Any, **kwargs) -> None:
        """
        Writes Pickle to File Asyncronously
        """
        data = await Serialize.Pkl.async_dumps(obj, **kwargs)
        return await self.async_write_bytes(data)


    
    



    




class AsyncSyncPosixPath(PosixPath, AsyncSyncPath, PureAsyncSyncPosixPath):
    __slots__ = ()


class AsyncSyncWindowsPath(WindowsPath, AsyncSyncPath, PureAsyncSyncWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("AsyncSyncPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("AsyncSyncPath.async_is_mount() is unsupported on this system")