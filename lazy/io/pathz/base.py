import abc
import os
import typing

from lazy.serialize import Serialize
from .types import *

from logz import get_logger
logger = get_logger('lazyio')

__all__ = (
    'DEFAULT_ENCODING',
    'PurePath',
    'ReadOnlyPath',
    'ReadWritePath'
    'Serialize',
    'logger'
)

DEFAULT_ENCODING = 'utf-8'


class PurePath(Protocol):
    """Protocol for pathlib.PurePath-like API."""
    parts: Tuple[str, ...]
    drive: str
    root: str
    anchor: str
    name: str
    suffix: str
    suffixes: List[str]
    stem: str

    def __new__(cls: Type[T], *args: PathLike) -> T:
        raise NotImplementedError

    def __fspath__(self) -> str:
        raise NotImplementedError

    def __hash__(self) -> int:
        raise NotImplementedError

    def __lt__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __le__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __gt__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __ge__(self, other: 'PurePath') -> bool:
        raise NotImplementedError

    def __truediv__(self: T, key: PathLike) -> T:
        raise NotImplementedError

    def __rtruediv__(self: T, key: PathLike) -> T:
        raise NotImplementedError

    def __bytes__(self) -> bytes:
        raise NotImplementedError

    def as_posix(self) -> str:
        raise NotImplementedError

    def as_uri(self) -> str:
        raise NotImplementedError

    def is_absolute(self) -> bool:
        raise NotImplementedError

    def is_reserved(self) -> bool:
        raise NotImplementedError

    def match(self, path_pattern: str) -> bool:
        raise NotImplementedError

    def relative_to(self: T, *other: PathLike) -> T:
        raise NotImplementedError

    def with_name(self: T, name: str) -> T:
        raise NotImplementedError

    def with_suffix(self: T, suffix: str) -> T:
        raise NotImplementedError

    def joinpath(self: T, *other: PathLike) -> T:
        raise NotImplementedError
    
    def get_blob(self: T) -> T:
        raise NotImplementedError

    @property
    def parents(self: T) -> Sequence[T]:
        raise NotImplementedError

    @property
    def parent(self: T) -> T:
        raise NotImplementedError
    
    @property
    def absolute_parent(self: T) -> T:
        raise NotImplementedError

    @property
    def is_cloud(self: T) -> bool:
        raise NotImplementedError
    
    @property
    def cloud_provider(self: T) -> T:
        raise NotImplementedError

    @property
    def is_gs(self: T) -> bool:
        raise NotImplementedError
    
    @property
    def is_s3(self: T) -> bool:
        raise NotImplementedError
     
    @property
    def is_minio(self: T) -> bool:
        raise NotImplementedError
    
    @property
    def string(self: T) -> str:
        return self.as_posix()
    
    @property
    def file_ext(self: T) -> T:
        return self.suffix[1:]

    @property
    def extension(self: T) -> T:
        return self.suffix[1:]

    @property
    def bucket(self: T) -> Optional[str]:
        raise NotImplementedError

    @property
    def bucket_path(self) -> Optional[str]:
        raise NotImplementedError
    
    @property
    def _path_str(self) -> str:
        """
        Returns the `__fspath__` string representation.
        """
        raise NotImplementedError
    
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
        raise NotImplementedError

    # py3.9 backport of PurePath.is_relative_to.
    def is_relative_to(self, *other: PathLike) -> bool:
        """Return True if the path is relative to another path or False."""
        try:
            self.relative_to(*other)
            return True
        except ValueError: return False


class ReadOnlyPath(PurePath, Protocol):
    """Protocol for read-only methods of pathlib.Path-like API.
    See [pathlib.Path](https://docs.python.org/3/library/pathlib.html)
    documentation.
    """

    def __new__(cls: Type[T], *args: PathLike) -> T:
        if cls not in (ReadOnlyPath, ReadWritePath):
            return super().__new__(cls, *args)
        from .generic import as_path
        return as_path(*args)

    @abc.abstractmethod
    def exists(self) -> bool:
        """Returns True if self exists."""

    @abc.abstractmethod
    def is_dir(self) -> bool:
        """Returns True if self is a dir."""

    def is_file(self) -> bool:
        """Returns True if self is a file."""
        return not self.is_dir()

    @abc.abstractmethod
    def iterdir(self: T) -> Iterator[T]:
        """Iterates over the directory."""

    @abc.abstractmethod
    def glob(self: T, pattern: str) -> Iterator[T]:
        """Yielding all matching files (of any kind)."""
        # Might be able to implement using `iterdir` (recursivelly for `rglob`).
    
    def get_files(self: T, pattern: str) -> List[T]:
        """returns all found results from glob"""
        it = self.glob(pattern)
        return [i for i in it if i.is_file()]

    def rglob(self: T, pattern: str) -> Iterator[T]:
        """Yielding all matching files recursivelly (of any kind)."""
        return self.glob(f'**/{pattern}')

    def expanduser(self: T) -> T:
        """Returns a new path with expanded `~` and `~user` constructs."""
        if '~' not in self.parts:    # pytype: disable=attribute-error
            return self
        raise NotImplementedError

    @abc.abstractmethod
    def resolve(self: T, strict: bool = False) -> T:
        """Returns the absolute path."""

    @abc.abstractmethod
    def open(
            self,
            mode: str = 'r',
            encoding: Optional[str] = DEFAULT_ENCODING,
            errors: Optional[str] = None,
            **kwargs: Any,
    ) -> typing.IO[AnyStr]:
        """Opens the file."""
    
    def as_reader(self, mode='r', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in r """
        return self.open(mode=mode, encoding=encoding, **kwargs)
    
    def reader(self, mode='r', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in r """
        return self.open(mode=mode, encoding=encoding, **kwargs)
    
    def read(self, mode='r', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> str:
        """ Reads the File in r """
        with self.open(mode=mode, encoding=encoding, **kwargs) as f:
            return f.read()
    
    def read_b(self, mode='rb', **kwargs) -> bytes:
        """ Reads the File in rb """
        with self.open(mode=mode) as f:
            return f.read()
    
    def readlines(self, **kwargs) -> List[str]:
        with self.open('r', **kwargs) as f:
            return f.readlines()

    def read_bytes(self) -> bytes:
        """Reads contents of self as bytes."""
        with self.open('rb') as f:
            return f.read()

    def read_text(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> str:
        """Reads contents of self as bytes."""
        with self.open('r', encoding=encoding, **kwargs) as f:
            return f.read()

    def format(self: T, *args: Any, **kwargs: Any) -> T:
        """Apply `str.format()` to the path."""
        return type(self)(os.fspath(self).format(*args, **kwargs))    # pytype: disable=not-instantiable

    
    """ Serializer Specific Methods """

    def read_json(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        #with self.open('r', encoding=encoding) as f:
        return Serialize.Json.loads(self.read_text(encoding = encoding), **kwargs)
        
    def read_jsonlines(self,  mode: str = 'r', skip_errors: bool = True, as_iterable: bool = True, **kwargs) -> Iterator[T]:
        with self.open(mode=mode) as f:
            return Serialize.Json.readlines(f, as_iterable = as_iterable, skip_errors = skip_errors, **kwargs)
    
    def read_yaml(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        #with self.open('r', encoding=encoding) as f:
        return Serialize.Yaml.loads(self.read_text(encoding = encoding), **kwargs)
    
    def read_pickle(self, mode: str = 'rb', **kwargs):
        with self.open(mode=mode) as f:
            return Serialize.Pkl.loads(f.read(), **kwargs)


    @abc.abstractmethod
    async def async_open(
            self,
            mode: str = 'r',
            encoding: Optional[str] = DEFAULT_ENCODING,
            errors: Optional[str] = None,
            **kwargs: Any,
    ) -> typing.IO[AnyStr]:
        """Asyncronously Opens the file."""
    
    async def async_exists(self) -> bool:
        return self.exists()

    async def async_as_reader(self, mode='r', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in r """
        return await self.async_open(mode=mode, encoding=encoding, **kwargs)
    
    async def async_reader(self, mode='r', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in r """
        return await self.async_open(mode=mode, encoding=encoding, **kwargs)

    async def async_read(self, mode='r', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> str:
        """ Reads the File in r """
        #with await self.async_open(mode=mode, encoding=encoding, **kwargs) as f:
        async with self.async_open(mode=mode, encoding=encoding, **kwargs) as f:
            reader = getattr(f, '_read', getattr(f, 'read'))
            return await reader()
    
    async def async_read_b(self, mode='rb', **kwargs) -> bytes:
        """ Reads the File in rb """
        async with self.async_open(mode=mode) as f:
            reader = getattr(f, '_read', getattr(f, 'read'))
            return await reader()
    
    async def async_readlines(self, **kwargs) -> List[str]:
        async with self.async_open('r', **kwargs) as f:
            reader = getattr(f, '_readlines', getattr(f, 'readlines'))
            return await reader()

    async def async_read_bytes(self) -> bytes:
        """Reads contents of self as bytes."""
        async with self.async_open('rb') as f:
            reader = getattr(f, '_read', getattr(f, 'read'))
            return await reader()

    async def async_read_text(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> str:
        """Reads contents of self as bytes."""
        async with self.async_open('r', encoding=encoding, **kwargs) as f:
            reader = getattr(f, '_read', getattr(f, 'read'))
            return await reader()
    
    """ Async Serializer Specific Methods """

    async def async_read_json(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        #with self.open('r', encoding=encoding) as f:
        return await Serialize.Json.async_loads(await self.async_read_text(encoding = encoding), **kwargs)
        
    async def async_read_jsonlines(self,  mode: str = 'r', skip_errors: bool = True, as_iterable: bool = True, **kwargs) -> Iterator[T]:
        async with self.async_open(mode=mode) as f:
            return await Serialize.Json.async_readlines(f, as_iterable = as_iterable, skip_errors = skip_errors, **kwargs)
    
    async def async_read_yaml(self, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> JsonType:
        #with self.open('r', encoding=encoding) as f:
        return await Serialize.Yaml.async_loads(await self.async_read_text(encoding = encoding), **kwargs)
    
    async def async_read_pickle(self, mode: str = 'rb', **kwargs):
        async with self.async_open(mode=mode) as f:
            reader = getattr(f, '_read', getattr(f, 'read'))
            return await Serialize.Pkl.async_loads(await reader(), **kwargs)
    


class ReadWritePath(ReadOnlyPath, Protocol):
    """Protocol for pathlib.Path-like API.
    See [pathlib.Path](https://docs.python.org/3/library/pathlib.html)
    documentation.
    """

    @abc.abstractmethod
    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a new directory at this given path."""

    @abc.abstractmethod
    def rmdir(self) -> None:
        """Remove the empty directory at this given path."""

    @abc.abstractmethod
    def rmtree(self) -> None:
        """Remove the directory, including all sub-files."""

    @abc.abstractmethod
    def unlink(self, missing_ok: bool = False) -> None:
        """Remove this file or symbolic link."""
    
    def ensure_dir(self: T, mode: int = 0o777, parents: bool = True, exist_ok: bool = True):
        """Ensures the parent directory exists, creates if not"""
        return self.absolute_parent.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create a file at this given path."""
        del mode    # Unused
        if self.exists():
            if exist_ok: return
            else: raise FileExistsError(f'{self} already exists.')
        self.write_text('', mode='w')

    @abc.abstractmethod
    def rename(self: T, target: PathLike) -> T:
        """Renames the path."""

    @abc.abstractmethod
    def replace(self: T, target: PathLike) -> T:
        """Overwrites the destination path."""

    @abc.abstractmethod
    def copy(self: T, dst: PathLike, overwrite: bool = False) -> T:
        """Copy the current file to the given destination."""
    
    def as_writer(self, mode='w', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in w"""
        return self.open(mode=mode, encoding=encoding, **kwargs)
    
    def writer(self, mode='w', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in w """
        return self.open(mode=mode, encoding=encoding, **kwargs)

    async def async_as_writer(self, mode='w', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in w"""
        return await self.async_open(mode=mode, encoding=encoding, **kwargs)
    
    async def async_writer(self, mode='w', encoding: Optional[str] = DEFAULT_ENCODING, **kwargs):
        """ Reads the File in w """
        return await self.async_open(mode=mode, encoding=encoding, **kwargs)
    
    @property
    def absolute_parent(self: T) -> T:
        uri_scheme = self._uri_scheme
        if uri_scheme: return self._new(self._PATH.join(f'{uri_scheme}://', '/'.join(self.parts[2:-1])))
        p = self.resolve()
        if p.is_dir:  return p
        return p.parent


    def copydir(self: T, dst: PathLike, ignore=['.git'], overwrite: bool = False, dryrun: bool = False, **kwargs) -> List[T]:
        """Copies the Current Top Level Parent Dir to the Dst Dir without recursion"""
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


    def copydirs(self: T, dst: PathLike, mode: str = 'shallow', pattern='*', ignore=['.git'], overwrite: bool = False, levels: int = 2, dryrun: bool = False, **kwargs) -> List[T]:
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

    def write_bytes(self, data: bytes) -> None:
        """Writes content as bytes."""
        with self.open('wb') as f:
            return f.write(data)

    def write_text(self, data: str, append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = None) -> None:
        """Writes content as str."""
        mode = 'a' if append and self.exists() else 'w'
        with self.open(mode, encoding=encoding, errors=errors) as f:
            if 'a' in mode:
                f.write('\n')
            return f.write(data)
    

    """ Serializer Specific Methods """

    def write_pickle(self, obj: Any, **kwargs) -> None:
        data = Serialize.Pkl.dumps(obj, **kwargs)
        return self.write_bytes(data)

    def write_yaml(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> None:
        """Writes Json to File"""
        with self.open('w', encoding = encoding) as f:
            f.write(Serialize.Yaml.dumps(data, **kwargs))

    def write_json(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, ensure_ascii: bool = False, indent: int = 2, **kwargs) -> None:
        """Writes Json to File"""
        with self.open('w', encoding = encoding) as f:
            f.write(Serialize.Json.dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
    
    def write_jsonlines(self, data: List[JsonType], append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        if ensure_file_exists and not self.exists(): self.touch()
        mode = 'a' if (append and self.exists()) or ensure_file_exists else 'w'
        with self.open(mode=mode, encoding=encoding) as f:
            Serialize.Json.write_jsonlines(f, data = data, newline = newline, ignore_errors = ignore_errors, flush_every = flush_every, log_errors = log_errors, **kwargs)
            

    """
    Async Methods
    """

    async def async_touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        """Create a file at this given path."""
        del mode    # Unused
        if await self.async_exists():
            if exist_ok: return
            else: raise FileExistsError(f'{self} already exists.')
        await self.async_write_text('', mode='w')

    async def async_write_bytes(self, data: bytes) -> None:
        """Writes content as bytes."""
        async with self.open('wb') as f:
            return await f.write(data)

    async def async_write_text(self, data: str, append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, errors: Optional[str] = None) -> None:
        """Writes content as str."""
        mode = 'a' if append and self.exists() else 'w'
        async with self.async_open(mode, encoding=encoding, errors=errors) as f:
            writer = getattr(f, '_write', getattr(f, 'write'))
            if 'a' in mode:
                await writer('\n')
            return await writer(data)
    

    """ Serializer Specific Methods """

    async def async_write_pickle(self, obj: Any, **kwargs) -> None:
        data = await Serialize.Pkl.async_dumps(obj, **kwargs)
        return await self.async_write_bytes(data)

    async def async_write_yaml(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, **kwargs) -> None:
        """Writes Json to File"""
        async with self.async_open('w', encoding = encoding) as f:
            writer = getattr(f, '_write', getattr(f, 'write'))
            await writer(await Serialize.Yaml.async_dumps(data, **kwargs))
            #await f.write(await Serialize.Yaml.async_dumps(data, **kwargs))

    async def async_write_json(self, data: JsonType, encoding: Optional[str] = DEFAULT_ENCODING, ensure_ascii: bool = False, indent: int = 2, **kwargs) -> None:
        """Writes Json to File"""
        async with self.async_open('w', encoding = encoding) as f:
            writer = getattr(f, '_write', getattr(f, 'write'))
            await writer(await Serialize.Json.async_dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
            #await f.write(await Serialize.Json.async_dumps(data, ensure_ascii=ensure_ascii, indent=indent, **kwargs))
    
    async def async_write_jsonlines(self, data: List[JsonType], append: bool = False, encoding: Optional[str] = DEFAULT_ENCODING, newline: str = '\n', ignore_errors: bool = True, ensure_file_exists: bool = True, flush_every: int = 0, log_errors: bool = False, **kwargs):
        if ensure_file_exists and not await self.async_exists(): await self.async_touch()
        mode = 'a' if (append and await self.async_exists()) or ensure_file_exists else 'w'
        async with self.async_open(mode=mode, encoding=encoding) as f:
            writer = getattr(f, '_write', getattr(f, 'write'))
            flusher = getattr(f, '_flush', getattr(f, 'flush'))
            for n, i in enumerate(data):
                try:
                    d = await Serialize.Json.async_dumps(i, **kwargs)
                    await writer(d + newline)
                    if flush_every and n + 1 % flush_every == 0:
                        await flusher()
                except (StopIteration, KeyboardInterrupt, GeneratorExit): break
                except ValueError as e:
                    if log_errors: logger.error(f'Value Error on idx {n}:\nError: {e}\nItem: {i}')
                    if ignore_errors: continue
                    raise e
                except Exception as e:
                    if log_errors: logger.error(f'Error on idx {n}:\nError: {e}\nItem: {i}')
                    if ignore_errors: continue
                    raise e
    
