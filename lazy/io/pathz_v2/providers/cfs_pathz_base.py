from __future__ import annotations

import os
import datetime
from typing import ClassVar

from lazy.serialize import Serialize
from .base import *
from ..flavours import _pathz_windows_flavour, _pathz_posix_flavour


if TYPE_CHECKING:
    try: from lazy.io.pathz_v2 import PathLike
    except ImportError: PathLike = type
    CloudAuthz: object = None

from .cfs_base import get_accessor, get_cloud_filesystem, AccessorLike, CFSLike

class PathzCFSPurePath(PurePath):
    _prefix: str = None
    _provider: str = None
    _win_pathz: ClassVar = 'PurePathzCFSWindowsPath'
    _posix_pathz: ClassVar = 'PurePathzCFSPosixPath'

    def _init(self, template: Optional[PurePath] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)

    def __new__(cls, *args):
        if cls is PathzCFSPurePath or issubclass(cls, PathzCFSPurePath):
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
        return cls._from_parts(args)

    def _new(self, *parts):
        """Create a new `Path` child of same type."""
        return type(self)(*parts)


class PurePathzCFSPosixPath(PathzCFSPurePath):
    """PurePath subclass for non-Windows systems.
    On a POSIX system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_posix_flavour
    _pathlike = posixpath
    __slots__ = ()


class PurePathzCFSWindowsPath(PathzCFSPurePath):
    """PurePath subclass for Windows systems.
    On a Windows system, instantiating a PurePath should return this object.
    However, you can also instantiate it directly on any system.
    """
    _flavour = _pathz_windows_flavour
    _pathlike = ntpath
    __slots__ = ()


class PathzCFSPath(Path, PathzCFSPurePath):
    """
    Our customized class that incorporates both sync and async methods
    """
    _flavour = _pathz_windows_flavour if os.name == 'nt' else _pathz_posix_flavour
    _accessor: AccessorLike = None
    _pathlike = posixpath
    _prefix = None
    _provider = None
    _win_pathz: ClassVar = 'PathzCFSWindowsPath'
    _posix_pathz: ClassVar = 'PathzCFSPosixPath'

    def _init(self, template: Optional['PathzCFSPath'] = None):
        self._accessor: AccessorLike = get_accessor(self._prefix)
        self._closed = False
        self._fileio = None

    def __new__(cls, *parts, **kwargs):
        if cls is PathzCFSPath or issubclass(cls, PathzCFSPath): 
            cls = cls._win_pathz if os.name == 'nt' else cls._posix_pathz
            cls = globals()[cls]
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
    def _bucket(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        if self._prefix in self.parts[0]: return self.parts[1]
        return self.parts[0]
    
    @property
    def _bucketstr(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        return self._prefix + '://' + self._bucket
    
    @property
    def _pathkeys(self) -> str:
        """
        Returns the `__fspath__` string representation without the uri_scheme
        """
        if self._bucket in self.parts[0]: return self._pathlike.join(*self.parts[1:])
        if self._bucket in self.parts[1]: return self._pathlike.join(*self.parts[2:])
        return self._pathlike.join(*self.parts)
    
    @property
    def _cloudstr(self) -> str:
        """
        Reconstructs the proper cloud URI
        """
        if self._prefix not in self.parts[0]: return self._prefix + '://' + '/'.join(self.parts)
        return self._prefix + '://' + '/'.join(self.parts[1:])
    
    @property
    def posix_(self):
        """Return the string representation of the path with forward (/)
        slashes."""
        f = self._flavour
        return str(self).replace(f.sep, '/')

    @property
    def string(self) -> str:
        if self.is_cloud:
            return self._cloudstr
        return self.posix_
    
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
    def stat_(self) -> stat_result:
        """
        Returns the stat results for path
        """
        return self.stat()
    
    @property
    def hash_(self) -> str:
        """
        Hash of file properties, to tell if it has changed
        """
        return self._accessor.ukey(self._cloudpath)
    
    @property
    def size_(self) -> Optional[Union[float, int]]:
        """
        Size in bytes of file
        """
        if self.is_file_: return self._accessor.size(self._cloudpath)
        return None
    
    @property
    def modified_(self) -> 'datetime.datetime':
        """
        Return the last modified timestamp of file at path as a datetime
        """
        r = self.stat_ #self._accessor.modified(self._cloudpath)
        ts = r.get('updated', '')
        if ts: return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')
        return None
        #return r.get('updated')
    
    @property
    def metadata_(self):
        """
        Return metadata of path
        """
        return self._accessor.metadata(self._cloudpath)
    
    @property
    def path_info_(self):
        """
        Return info of path
        """
        return self._accessor.info(path=self._cloudpath)
    
    @property
    def is_cloud(self) -> bool:
        if not self._prefix: return False
        return bool(self._prefix in self.parts[0] or self._prefix in self.parts[1])
    
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
    def home_(self) -> Type['PathzCFSPath']:
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
    async def async_home_(self) -> Type['PathzCFSPath']:
        return await self.async_home()
    
    @property
    async def async_stat_(self) -> stat_result:
        """
        Returns the stat results for path
        """
        return await self.async_stat()
    
    @property
    async def async_hash_(self) -> str:
        """
        Hash of file properties, to tell if it has changed
        """
        return await self._accessor.async_ukey(self._cloudpath)
    
    @property
    async def async_size_(self) -> Optional[Union[float, int]]:
        """
        Size in bytes of file
        """
        if await self.async_is_file_: return await self._accessor.async_size(self._cloudpath)
        return None
    
    @property
    async def async_metadata_(self):
        """
        Return metadata of path
        """
        return await self._accessor.async_metadata(self._cloudpath)

    @property
    async def async_modified_(self) -> 'datetime.datetime':
        """
        Return the last modified timestamp of file at path as a datetime
        """
        if self._prefix == 'gs':
            r = await self.async_stat_
            ts = r.get('updated', '')
            if ts: return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')#.isoformat()
            return ts
        return await self._accessor.async_modified(self._cloudpath)
    
    @property
    async def async_path_info_(self):
        """
        Return info of path
        """
        return await self.async_info()
    
    def open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)

    
    def async_open(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        compression = infer doesn't work all that well.
        """
        #self._fileio = self._accessor.open(self._cloudpath, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, buffering=buffering, **kwargs)
        #print(type(self._fileio))
        #return get_cloud_file(self._fileio)
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, buffering=buffering, **kwargs))


    def reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs)
    
    def async_reader(self, mode: FileMode = 'r', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs))
    
    def appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs)
    
    def async_appender(self, mode: FileMode = 'a', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs))
    
    def writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IO[Union[str, bytes]]:
        """
        Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        #self.touch()
        return self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs)
    
    def async_writer(self, mode: FileMode = 'w', buffering: int = -1, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = ON_ERRORS, newline: Optional[str] = NEWLINE, block_size: int = 5242880, compression: str = None, **kwargs: Any) -> IterableAIOFile:
        """
        Asyncronously Open the file pointed by this path and return a file object, as
        the built-in open() function does.
        """
        #self.touch()
        return get_cloud_file(self._accessor.open(self._cloudpath, mode=mode, buffering=buffering, encoding=encoding, errors=errors, block_size=block_size, compression=compression, newline=newline, **kwargs))

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
        try:
            self._accessor.touch(self._cloudpath, truncate = truncate, data = data, **kwargs)
        except:
            with self.open('wb') as f:
                f.write(b'')
                f.flush()


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

    def mkdir(self, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: self._accessor.mkdir(self._cloudpath, parents = parents, exist_ok = exist_ok)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            self.parent.mkdir(parents=True, exist_ok=True)
            self.mkdir(mode, parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not self.is_dir(): raise

    async def async_mkdir(self, parents: bool = True, exist_ok: bool = True):
        """
        Create a new directory at this given path.
        """
        try: await self._accessor.async_mkdir(self._cloudpath, create_parents = parents, exist_ok = exist_ok)

        except FileNotFoundError:
            if not parents or self.parent == self: raise
            await self.parent.async_mkdir(parents=True, exist_ok=True)
            await self.async_mkdir(parents=False, exist_ok=exist_ok)

        except OSError:
            # Cannot rely on checking for EEXIST, since the operating system
            # could give priority to other errors like EACCES or EROFS
            if not exist_ok or not await self.async_is_dir(): raise

    async def chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        raise NotImplementedError

    async def async_chmod(self, mode: int):
        """
        Change the permissions of the path, like os.chmod().
        """
        raise NotImplementedError

    def lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        raise NotImplementedError

    async def async_lchmod(self, mode: int):
        """
        Like chmod(), except if the path points to a symlink, the symlink's
        permissions are changed, rather than its target's.
        """
        raise NotImplementedError

    def unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: self._accessor.unlink(self._cloudpath)
        except FileNotFoundError:
            if not missing_ok: raise

    async def async_unlink(self, missing_ok: bool = False):
        """
        Remove this file or link.
        If the path is a directory, use rmdir() instead.
        """
        try: await self._accessor.async_unlink(self._cloudpath, missing_ok = missing_ok)
        except FileNotFoundError:
            if not missing_ok: raise

    def rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True):
        """
        Remove this directory.  The directory must be empty.
        """
        try:
            return self._accessor.rmdir(self._cloudpath)
        except Exception as e:
            if force: return self._accessor.rmdir(self._cloudpath, recursive = recursive)
            if skip_errors: return
            raise e


    async def async_rmdir(self, force: bool = False, recursive: bool = True, skip_errors: bool = True):
        """
        Remove this directory.  The directory must be empty.
        """
        try:
            return await self._accessor.async_rmdir(self._cloudpath)
        except Exception as e:
            if force: return await self._accessor.async_rmdir(self._cloudpath, recursive = recursive)
            if skip_errors: return
            raise e

    def link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        raise NotImplementedError
    
    async def async_link_to(self, target: str):
        """
        Create a hard link pointing to a path named target.
        """
        raise NotImplementedError

    def rename(self, target: Union[str, Type['PathzCFSPath']]) -> Type['PathzCFSPath']:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.rename(self._cloudpath, target)
        return type(self)(target)
    
    async def async_rename(self, target: Union[str, Type['PathzCFSPath']]) -> Type['PathzCFSPath']:
        """
        Rename this path to the target path.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_rename(self._cloudpath, target)
        return type(self)(target)

    def replace(self, target: str) -> Type['PathzCFSPath']:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        self._accessor.replace(self._cloudpath, target)
        return type(self)(target)
    
    async def async_replace(self, target: str) -> Type['PathzCFSPath']:
        """
        Rename this path to the target path, overwriting if that path exists.
        The target path may be absolute or relative. Relative paths are
        interpreted relative to the current working directory, *not* the
        directory of the Path object.
        Returns the new Path instance pointing to the target path.
        """
        await self._accessor.async_replace(self._cloudpath, target)
        return type(self)(target)

    def symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        raise NotImplementedError
    
    async def async_symlink_to(self, target: str, target_is_directory: bool = False):
        """
        Make this path a symlink pointing to the given path.
        Note the order of arguments (self, target) is the reverse of os.symlink's.
        """
        raise NotImplementedError

    def exists(self) -> bool:
        """
        Whether this path exists.
        """
        return self._accessor.exists(self._cloudpath)
        

    async def async_exists(self) -> bool:
        """
        Whether this path exists.
        """
        return await self._accessor.async_exists(self._cloudpath)

    @classmethod
    def cwd(cls: type) -> str:
        """Return a new path pointing to the current working directory
        (as returned by os.getcwd()).
        """
        cwd: str = os.getcwd()
        return cls(cwd)

    @classmethod
    def home(cls: type) -> Type['PathzCFSPath']:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        homedir: str = cls()._flavour.gethomedir(None)
        return cls(homedir)

    @classmethod
    async def async_home(cls: type) -> Type['PathzCFSPath']:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        coro = cls()._flavour.async_gethomedir(None)
        homedir: str = await coro
        return cls(homedir)

    def samefile(self, other_path: Union[Type['PathzCFSPath'], Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = Type['PathzCFSPath'](other_path)
        if isinstance(other_path, Type['PathzCFSPath']):
            try: other_st = other_path.stat()
            except AttributeError: other_st = self._accessor.stat(other_path)

        else:
            try: other_st = other_path.stat()
            except AttributeError: other_st = other_path._accessor.stat(other_path)
        return os.path.samestat(self.stat(), other_st)

    async def async_samefile(self, other_path: Union[Type['PathzCFSPath'], Paths]) -> bool:
        """Return whether other_path is the same or not as this file
        (as returned by os.path.samefile()).
        """
        if isinstance(other_path, Paths.__args__): other_path = Type['PathzCFSPath'](other_path)
        if isinstance(other_path, Type['PathzCFSPath']):
            try: other_st = await other_path.async_stat()
            except AttributeError: other_st = await self._accessor.async_stat(other_path)

        else:
            try: other_st = await to_thread(other_path.stat)
            except AttributeError: other_st = await to_thread(other_path._accessor.stat, other_path)

        return os.path.samestat(await self.async_stat(),other_st)

    def iterdir(self) -> Iterable[Type['PathzCFSPath']]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in self._accessor.listdir(self):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    async def async_iterdir(self) -> AsyncIterable[Type['PathzCFSPath']]:
        """Iterate over the files in this directory.  Does not yield any
        result for the special paths '.' and '..'.
        """
        for name in await self._accessor.async_listdir(self):
            if name in {'.', '..'}: continue
            yield self._make_child_relpath(name)

    def glob(self, pattern: str = '*') -> Iterable[Type['PathzCFSPath']]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        return self._accessor.glob(pattern)


    async def async_glob(self, pattern: str = '*') -> AsyncIterable[Type['PathzCFSPath']]:
        """Iterate over this subtree and yield all existing files (of any
        kind, including directories) matching the given relative pattern.
        """
        if not pattern: raise ValueError("Unacceptable pattern: {!r}".format(pattern))
        return await self._accessor.async_glob(pattern)

    def rglob(self, pattern: str) -> Iterable[Type['PathzCFSPath']]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        return self.glob(f'**/{pattern}')
        
    async def async_rglob(self, pattern: str) -> AsyncIterable[Type['PathzCFSPath']]:
        """Recursively yield all existing files (of any kind, including
        directories) matching the given relative pattern, anywhere in
        this subtree.
        """
        return await self.async_glob(f'**/{pattern}')

    def absolute(self) -> Type['PathzCFSPath']:
        """Return an absolute version of this path.  This function works
        even if the path doesn't point to anything.
        No normalization is done, i.e. all '.' and '..' will be kept along.
        Use resolve() to get the canonical path to a file.
        """
        raise NotImplementedError
        

    def resolve(self, strict: bool = False) -> Type['PathzCFSPath']:
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

    async def async_resolve(self, strict: bool = False) -> Type['PathzCFSPath']:
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
        return self._accessor.stat(self._cloudpath)
    
    async def async_stat(self) -> stat_result:
        """
        Return the result of the stat() system call on this path, like
        os.stat() does.
        """
        return await self._accessor.async_stat(self._cloudpath)
    
    def info(self):
        """
        Return the result of the info() system call on this path, like
        """
        #_info = syncify(self.async_info, raise_sync_error=False)()
        #_info = syncify(self._accessor.async_info, False)(self._cloudpath)
        #_info = runnify(self._accessor.async_info)(self._cloudpath)
        #return _info#.result()
        #return self._accessor.info(self._cloudpath)
        return self._accessor.info(self._cloudpath)
    
    async def async_info(self):
        """
        Return the result of the info() system call on this path, like
        os.stat() does.
        """
        return await self._accessor.async_info(self._cloudpath)

    def lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        raise NotImplementedError
    
    async def async_lstat(self) -> stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        raise NotImplementedError

    def owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        raise NotImplementedError
    
    async def async_owner(self) -> str:
        """
        Return the login name of the file owner.
        """
        raise NotImplementedError

    def group(self) -> str:
        """
        Return the group name of the file gid.
        """
        raise NotImplementedError
    
    async def async_group(self) -> str:
        """
        Return the group name of the file gid.
        """
        raise NotImplementedError

    def is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        return self._accessor.is_dir(self._cloudpath)

    
    async def async_is_dir(self) -> bool:
        """
        Whether this path is a directory.
        """
        return await self._accessor.async_is_dir(self._cloudpath)

    def is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        raise NotImplementedError
        
    
    async def async_is_symlink(self) -> bool:
        """
        Whether this path is a symbolic link.
        """
        raise NotImplementedError

    def is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        return self._accessor.is_file(self._cloudpath)
    
    async def async_is_file(self) -> bool:
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        return await self._accessor.async_is_file(self._cloudpath)
    
    @staticmethod
    def _get_pathlike(path: PathLike):
        """
        Returns the path of the file.
        """
        from lazy.io.pathz_v2 import get_path
        return get_path(path)
    
    def copy(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies the File to the Dir/File.
        """
        dest = self._get_pathlike(dest)
        if dest.is_cloud:
            return self._accessor.copy(self._cloudpath, dest._cloudpath, recursive = recursive)
        return self._accessor.get(self._cloudpath, dest.string, recursive = recursive)
    
    async def async_copy(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies the File to the Dir/File.
        """
        dest = self._get_pathlike(dest)
        if dest.is_cloud:
            return await self._accessor.async_copy(self._cloudpath, dest._cloudpath, recursive = recursive)
        return await self._accessor.async_get(self._cloudpath, dest.string, recursive = recursive)

    def copy_file(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies this File to the the Dest Path
        """
        dest = self._get_pathlike(dest)
        if dest.is_cloud:
            return self._accessor.copy_file(self._cloudpath, dest._cloudpath, recursive = recursive)
        return self._accessor.copy_file(self._cloudpath, dest.string, recursive = recursive)
    
    async def async_copy_file(self, dest: PathLike, recursive: bool = False, overwrite: bool = False, skip_errors: bool = False):
        """
        Copies this File to the the Dest Path
        """
        dest = self._get_pathlike(dest)
        if dest.is_cloud:
            return await self._accessor.async_copy_file(self._cloudpath, dest._cloudpath, recursive = recursive)
        return await self._accessor.async_copy_file(self._cloudpath, dest.string, recursive = recursive)

    def put(self, src: PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file(s) from src to this FilePath
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.put(src.string, self._cloudpath, recursive = recursive, callback = callback)
    
    async def async_put(self, src: PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy file(s) from src to this FilePath
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_put(src.string, self._cloudpath, recursive = recursive, callback = callback)

    def put_file(self, src: PathLike, callback: Optional[Callable] = None, **kwargs):
        """
        Copy single file to remote
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.put_file(src.string, self._cloudpath, callback = callback)
    
    async def async_put_file(self, src: PathLike, callback: Optional[Callable] = None, **kwargs):
        """
        Copy single file to remote
        WIP support for cloud-to-cloud
        """
        src = self._get_pathlike(src)
        assert not src.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_put_file(src.string, self._cloudpath, callback = callback)
    
    def get(self, dest: PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy the remote file(s) to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.get(self._cloudpath, dest.string, recursive = recursive, callback = callback)
    
    async def async_get(self, dest: PathLike, recursive: bool = False, callback: Optional[Callable] = None, **kwargs):
        """
        Copy the remote file(s) to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_get(self._cloudpath, dest.string, recursive = recursive, callback = callback)

    def get_file(self, dest: PathLike, callback: Optional[Callable] = None, **kwargs):
        """
        Copies this file to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return self._accessor.get_file(self._cloudpath, dest.string, callback = callback)
    
    async def async_get_file(self, dest: PathLike, callback: Optional[Callable] = None, **kwargs):
        """
        Copies this file to dest (local)
        WIP support for cloud-to-cloud
        """
        dest = self._get_pathlike(dest)
        assert not dest.is_cloud, 'Cloud to Cloud support not supported at this time'
        return await self._accessor.async_get_file(self._cloudpath, dest.string, callback = callback)
        

    def is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not self.exists() or not self.is_dir(): return False
        return False
        #raise NotImplementedError
        

    async def async_is_mount(self) -> bool:
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not await self.async_exists() or not await self.async_is_dir(): return False
        return False
        #raise NotImplementedError
        

    def is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        return False
        #raise NotImplementedError

    async def async_is_block_device(self) -> bool:
        """
        Whether this path is a block device.
        """
        return False
        #raise NotImplementedError

    def is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        return False
        #raise NotImplementedError
        
    
    async def async_is_char_device(self) -> bool:
        """
        Whether this path is a character device.
        """
        return False

    def is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        return False
        

    async def async_is_fifo(self) -> bool:
        """
        Whether this path is a FIFO.
        """
        return False
        

    def is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        return False
        
    
    async def async_is_socket(self) -> bool:
        """
        Whether this path is a socket.
        """
        return False
        

    def expanduser(self) -> Type['PathzCFSPath']:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = self._flavour.gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self
    
    async def async_expanduser(self) -> Type['PathzCFSPath']:
        """ Return a new path with expanded ~ and ~user constructs
        (as returned by os.path.expanduser)
        """
        if (not self._drv and not self._root and self._parts and self._parts[0][:1] == '~'):
            homedir = await self._flavour.async_gethomedir(self._parts[0][1:])
            return self._from_parts([homedir] + self._parts[1:])
        return self

    def iterdir(self) -> Iterable[Type['PathzCFSPath']]:
        names = self._accessor.listdir(self)
        for name in names:
            if name in {'.', '..'}: continue
        yield self._make_child_relpath(name)
    
    async def async_iterdir(self) -> AsyncIterable[Type['PathzCFSPath']]:
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

        return Serialize.DefaultJson.loads(self.read_text(encoding = encoding), **kwargs)
        
    def read_jsonlines(self,  mode: str = 'r', skip_errors: bool = True, as_iterable: bool = True, **kwargs) -> Iterator[T]:
        """
        Reads JSON Lines
        """
        with self.open(mode=mode) as f:
            return Serialize.OrJson.readlines(f, as_iterable = as_iterable, skip_errors = skip_errors, **kwargs)
    
    def read_yaml(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        """
        Reads YAML
        """
        return Serialize.Yaml.loads(self.read_text(encoding = encoding), **kwargs)
    
    def read_pickle(self, mode: str = 'rb', **kwargs):
        """
        Reads Pickle File
        """
        return Serialize.Pkl.loads(self.read_bytes(mode = mode), **kwargs)

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
            f.write(Serialize.SimdJson.dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
    
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
        t = await self.async_read_text(encoding = encoding)
        return await Serialize.DefaultJson.async_loads(t, **kwargs)
        
    async def async_read_jsonlines(self, mode: str = 'r', ignore_errors: bool = True, as_iterable: bool = True, **kwargs) -> Iterator[T]:
        """
        Reads JSON Lines Asyncronously
        """
        async with self.async_open(mode=mode) as f:
            return await Serialize.OrJson.async_readlines(f, as_iterable = as_iterable, ignore_errors = ignore_errors, **kwargs)
    
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
        #self._accessor.filesys.start_transaction()

        async with self.async_open('a', encoding = encoding, consistency = 'crc32c') as f:
            #print(type(f))
            #print(type(f._fp))
            async for i in Serialize.Json.async_yield_jsonlines(data = data, ignore_errors = ignore_errors, log_errors = log_errors, **kwargs):
                await f.write(i)
                #print(i)
                await f.write(newline)
            #print(type(f))
            #await f.close()
            #f._fp.commit()
            #await Serialize.Json.async_write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)
            #await f.flush()
            #await f.aclose()

        #await self.async_cloze()
        #self._accessor.filesys.end_transaction()

    async def async_write_json(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, ensure_ascii: bool = False, indent: int = 2, **kwargs) -> None:
        """
        Writes JSON to File Asyncronously
        """
        async with self.async_open('w', encoding = encoding) as f:
            await f.write(await Serialize.SimdJson.async_dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
    
    async def async_write_jsonlines(self, data: List[JsonType], append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        """
        Writes JSON Lines to File Asyncronously
        """
        if ensure_file_exists and not await self.async_exists_: await self.async_touch()
        mode = 'a' if (append and await self.async_exists_) or ensure_file_exists else 'w'
        async with self.async_open(mode=mode, encoding=encoding) as f:
            await Serialize.Json.async_write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)
            await f.flush()
            await f.aclose()

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
    
    """
    Other Methods
    """
    def url(self, **kwargs):
        return self._accessor.url(self._cloudpath, **kwargs)
    
    async def async_url(self, **kwargs):
        return await self._accessor.async_url(self._cloudpath, **kwargs)
    
    def cloze(self, **kwargs):
        if self._fileio: 
            self._fileio.commit()
        return self._accessor.invalidate_cache(self._cloudpath)
    
    async def async_cloze(self, **kwargs):
        #if self._fileio:
            #self._fileio.flush(True)
            #self._fileio.commit()
        #_f = self._accessor.open(self._cloudpath)
        return await self._accessor.async_invalidate_cache(self._cloudpath)


class PathzCFSPosixPath(PosixPath, PathzCFSPath, PurePathzCFSPosixPath):
    __slots__ = ()


class PathzCFSWindowsPath(WindowsPath, PathzCFSPath, PurePathzCFSWindowsPath):
    __slots__ = ()

    def is_mount(self) -> int:
        raise NotImplementedError("PathzCFSPath.is_mount() is unsupported on this system")

    async def async_is_mount(self) -> int:
        raise NotImplementedError("PathzCFSPath.async_is_mount() is unsupported on this system")


os.PathLike.register(PathzCFSPurePath)
os.PathLike.register(PathzCFSPath)
os.PathLike.register(PurePathzCFSPosixPath)
os.PathLike.register(PathzCFSWindowsPath)
os.PathLike.register(PathzCFSPosixPath)
os.PathLike.register(PurePathzCFSWindowsPath)

def register_pathlike(pathz: List[Union[PosixPath, PathzCFSPath, WindowsPath, PathzCFSWindowsPath, PathzCFSPosixPath, PurePathzCFSWindowsPath, Any]]):
    for p in pathz:
        os.PathLike.register(p)

__all__ = (
    'ClassVar',
    'AccessorLike',
    'CFSLike',
    'get_accessor',
    'get_cloud_filesystem',
    'PathzCFSPurePath',
    'PurePath',
    'PurePathzCFSPosixPath',
    'PurePathzCFSWindowsPath',
    'PathzCFSPath',
    'Path',
    '_pathz_windows_flavour',
    '_pathz_posix_flavour',
    'PathzCFSPosixPath',
    'PathzCFSWindowsPath',
    'register_pathlike'

)

