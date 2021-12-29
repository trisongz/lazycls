"""
Wrapper class around pathlib.Path
with enhancements.
This takes several of the functions found in
lazycls.utils and builds it directly into Path.

Will deprecate features in .utils in a few versions.
"""
import os
import aiofiles
import tempfile
from pathlib import Path as _Path
from typing import TypeVar, Union, Type, List, Any, Optional, Callable
from lazycls.prop import classproperty
from lazycls.serializers import Json, OrJson, Yaml, Pkl, Base

from errno import EINVAL, ENOENT, ENOTDIR, EBADF, ELOOP
from stat import S_ISDIR, S_ISLNK, S_ISREG, S_ISSOCK, S_ISBLK, S_ISCHR, S_ISFIFO

try: import httpx as _req
except ImportError: _req = None

if _req is None:
    try: import requests as _req
    except ImportError: _req = None



_IGNORED_ERROS = (ENOENT, ENOTDIR, EBADF, ELOOP)
_IGNORED_WINERRORS = (
    21,  # ERROR_NOT_READY - drive exists but is not accessible
    123, # ERROR_INVALID_NAME - fix for bpo-35306
    1921,  # ERROR_CANT_RESOLVE_FILENAME - fix for broken symlink pointing to itself
)

def _ignore_error(exception):
    return (getattr(exception, 'errno', None) in _IGNORED_ERROS or getattr(exception, 'winerror', None) in _IGNORED_WINERRORS)

_LOADERS = {
    '.json': OrJson,
    '.yml': Yaml,
    '.yaml': Yaml,
    '.pkl': Pkl,
    '.pickle': Pkl,
    '.pb': Pkl,
    'json': OrJson,
    'yaml': Yaml,
    'yml': Yaml,
    'pkl': Pkl,
    'pickle': Pkl
}

# https://stackoverflow.com/questions/29850801/subclass-pathlib-path-fails

class Path(_Path):
    _flavour = type(_Path())._flavour

    def __new__(cls, *args):
        return super(Path, cls).__new__(cls, *args)

    def __init__(self, *args):
        super().__init__()
    
    @classproperty
    def async_os(cls):
        """ Aliases for aiofiles.os"""
        return aiofiles.os
        
    @classproperty
    def working_dir(cls) -> Type[_Path]:
        return cls(os.getcwd())
    
    @classproperty
    def userhome(cls) -> Type[_Path]:
        """Return a new path pointing to the user's home directory (as
        returned by os.path.expanduser('~')).
        """
        return cls(cls()._flavour.gethomedir(None))

    @staticmethod
    def get_path(path: Union[str, _Path], resolve: bool = True, ensure_exists: bool = False, mode=0o777, file_mode=0o666, parents=False, exist_ok: bool = True) -> Type[_Path]:
        if isinstance(path, str): path = Path(path)
        if resolve: path.resolve()
        if ensure_exists and path.is_dir(): path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        elif ensure_exists: path.touch(mode=file_mode, exist_ok=exist_ok)
        return path

    @staticmethod
    def get_user_path(path: Union[str, _Path], resolve: bool = False) -> Type[_Path]:
        if isinstance(path, str): path = Path(path)
        path = path.expanduser()
        if resolve: path.resolve()
        return path
    
    @classmethod
    def from_env(cls, key: str, default: Union[str, _Path], **kwargs) -> Type[_Path]:
        """ gets the path from env value, returns default if none"""
        val = os.getenv(key, None)
        if val is None: return cls.get_path(default, **kwargs)
        return cls.get_path(val, **kwargs)
    
    @property
    def _exists(self): 
        return self.exists()

    @classmethod
    def from_url(cls, url: str, path: Union[str, _Path], overwrite: bool = False, **kwargs) -> Type[_Path]:
        """ Downloads a file from url and saves to  file """
        p = cls.get_path(path, resolve=True)
        if p.exists() and not overwrite: return p
        assert _req is not None, 'Requests and/or httpx is not available'
        r = _req.get(url=url, **kwargs)
        if r.status_code >= 400: raise Exception(f'url = {url} returned status_code = {r.status_code}')
        p.write_bytes(data=r.content)
        return p
    
    @staticmethod
    def get_parent_path(path: Union[str, _Path]) -> Type[_Path]:
        p = Path.get_path(path)
        return p.parent

    @staticmethod
    def get_cwd(*paths, posix: bool = True) -> Union[str, Type[_Path]]:
        if not paths:
            if posix: return Path.cwd.string
            return Path.cwd
        if posix: return Path.cwd.joinpath(*paths).string
        return Path.cwd.joinpath(*paths)

    @property
    def string(self) -> str:
        return self.as_posix()

    @property
    def extension(self):
        """ alias for .suffix or the file extension"""
        return self.suffix
    

    def reader(self, mode: str = 'r', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
        """ Alias for open(r)"""
        return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

    def binary_reader(self, mode: str = 'rb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
        """ Alias for open(rb)"""
        return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)
    
    def appender(self, mode: str = 'a', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
        """ Alias for open(a)"""
        return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)
    
    def binary_appender(self, mode: str = 'ab', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
        """ Alias for open(ab)"""
        return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)
    
    def writer(self, mode: str = 'w', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
        """ Alias for open(w)"""
        return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)
    
    def binary_writer(self, mode: str = 'wb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
        """ Alias for open(wb)"""
        return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)


    async def async_open(self, mode: str = 'r', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(r)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    async def async_reader(self, mode: str = 'r', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(r)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    async def async_reader_binary(self, mode: str = 'rb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(rb)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    async def async_appender(self, mode: str = 'a', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(a)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    async def async_appender_binary(self, mode: str = 'ab', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(ab)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    async def async_writer(self, mode: str = 'w', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(w)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    async def async_writer_binary(self, mode: str = 'wb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
        """ Alias for aiofiles.open(wb)"""
        return await aiofiles.open(self.string, mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)
    
    @staticmethod
    def get_tempfile(mode: str = 'w', encoding: str = 'utf-8', dir: Any = None, buffering: int = -1, newline: str = None, delete: bool = False, **kwargs):
        return tempfile.NamedTemporaryFile(mode=mode, encoding=encoding, buffering=buffering, newline=newline, delete=delete, dir=dir, **kwargs)

    @staticmethod
    def get_tempdir(dir: Any = None, **kwargs):
        return tempfile.TemporaryDirectory(dir=dir, **kwargs)

    @staticmethod
    async def async_get_tempfile(mode: str = 'w', encoding: str = 'utf-8', dir: Any = None, buffering: int = -1, newline: str = None, delete: bool = False, **kwargs):
        return await aiofiles.tempfile.NamedTemporaryFile(mode=mode, encoding=encoding, buffering=buffering, newline=newline, delete=delete, dir=dir, **kwargs)

    @staticmethod
    async def async_get_tempdir(dir: Any = None, **kwargs):
        return await aiofiles.tempfile.TemporaryDirectory(dir=dir, **kwargs)

    def to_base64(self, **kwargs):
        """ Reads the file as text, then turns to base64"""
        data = self.read_text(**kwargs)
        return Base.b64_encode(data)
    
    def to_gzip_base64(self, **kwargs):
        """ Reads the file as text, then turns to gzip+base64"""
        data = self.read_text(**kwargs)
        return Base.b64_gzip_encode(data)
        
    @classmethod
    def from_base64(cls, data: Union[str, bytes], path: Union[str, _Path] = None, encoding: str = 'utf-8', errors: str = None, **kwargs):
        d = Base.b64_decode(data)
        if not path: return d
        p = cls.get_path(path, **kwargs)
        p.write_text(data=d, encoding=encoding, errors=errors)
        return p

    @classmethod
    def from_gzip_base64(cls, data: Union[str, bytes], path: Union[str, _Path] = None, encoding: str = 'utf-8', errors: str = None, **kwargs):
        d = Base.b64_gzip_decode(data)
        if not path: return d
        p = cls.get_path(path, **kwargs)
        p.write_text(data=d, encoding=encoding, errors=errors)
        return p

    def read_bytes(self):
        """
        Open the file in bytes mode (rb), read it, and close the file.
        """
        with self.open(mode='rb') as f:
            return f.read()

    def read_text(self, encoding: str = 'utf-8', errors: str = None):
        """
        Open the file in text mode (r), read it, and close the file.
        """
        with self.open(mode='r', encoding=encoding, errors=errors) as f:
            return f.read()

    def append_bytes(self, data):
        """
        Open the file in bytes mode (ab), append to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode='ab') as f:
            return f.write(view)

    def append_text(self, data: str, encoding: str ='utf-8', errors: str = None, insert_newline: bool = True):
        """
        Open the file in text mode (a), write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError('data must be str, not %s' % data.__class__.__name__)
        with self.open(mode='a', encoding=encoding, errors=errors) as f:
            f.write(data)
            if insert_newline: f.write('\n')
            return f

    def write_bytes(self, data):
        """
        Open the file in bytes mode (wb), write to it, and close the file.
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode='wb') as f:
            return f.write(view)

    def write_text(self, data: str, encoding: str ='utf-8', errors: str = None):
        """
        Open the file in text mode (w), write to it, and close the file.
        """
        if not isinstance(data, str): raise TypeError('data must be str, not %s' % data.__class__.__name__)
        with self.open(mode='w', encoding=encoding, errors=errors) as f:
            return f.write(data)
    
    async def async_read_bytes(self, **kwargs):
        """
        Open the file in bytes mode (rb), read it, and close the file.
        """
        async with self.async_open(mode='rb', **kwargs) as f:
            return await f.read()

    async def async_read_text(self, encoding: str = 'utf-8', errors: str = None, **kwargs):
        """
        Open the file in text mode (r), read it, and close the file.
        """
        async with self.async_open(mode='r', encoding=encoding, errors=errors, **kwargs) as f:
            return await f.read()

    def get_files(self, pattern: str = '*') -> List[Type[_Path]]:
        """
        Uses the .glob(pattern) to construct List[Type[Path]] instead of returning an iterator
        """
        p_iter = self.glob(pattern)
        return [p for p in p_iter if p]

    def get_loader(self, loader: str = None):
        """ If the file is supported (pkl, yaml, json) - then return the loader"""
        if loader: return _LOADERS.get(loader, _LOADERS.get(self.extension, None))
        return _LOADERS.get(self.extension, None)

    def loads(self, encoding: str ='utf-8', errors: str = None, loader: str = None, binary: bool = False, **kwargs) -> Any:
        """ If the file is supported (pkl, yaml, json) - then return the loader.loads(**kwargs) method.
            else:
                - if binary: returns .read_bytes()
                    else: returns .read_text()
        """
        l = self.get_loader(loader=loader)
        if l: return l.loads(self.read_text(encoding=encoding, errors=errors), **kwargs)
        if binary: return self.read_bytes()
        return self.read_text(encoding=encoding, errors=errors)

    def dumps(self, data: Any, encoding: str ='utf-8', errors: str = None, loader: str = None, **kwargs) -> Any:
        """ If the file is supported (pkl, yaml, json) - then perform the loader.dumps(data, **kwargs) method.
            else:
                - if binary: performs .write_bytes(data)
                    else: performs .write_text(data)
            returns: Path
        """
        l = self.get_loader(loader=loader)
        if l:
            d = l.dumps(data, **kwargs)
            if isinstance(d, bytes): self.write_bytes(d)
            else: self.write_text(d, encoding=encoding, errors=errors)
        elif isinstance(data, bytes): self.write_bytes(data)
        else: self.write_text(data, encoding=encoding, errors=errors)
        return self


    """ Modifying the below to be properties rather than callables"""

    @property
    def _is_dir(self):
        """
        Whether this path is a directory.
        """
        try: return S_ISDIR(self.stat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    @property
    def _is_file(self):
        """
        Whether this path is a regular file (also True for symlinks pointing
        to regular files).
        """
        try: return S_ISREG(self.stat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    @property
    def _is_mount(self):
        """
        Check if this path is a POSIX mount point
        """
        # Need to exist and be a dir
        if not self.exists() or not self.is_dir(): return False
        try: parent_dev = self.parent.stat().st_dev
        except OSError: return False
        dev = self.stat().st_dev
        if dev != parent_dev: return True
        ino = self.stat().st_ino
        parent_ino = self.parent.stat().st_ino
        return ino == parent_ino

    @property
    def _is_symlink(self):
        """
        Whether this path is a symbolic link.
        """
        try: return S_ISLNK(self.lstat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    @property
    def _is_block_device(self):
        """
        Whether this path is a block device.
        """
        try: return S_ISBLK(self.stat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    @property
    def _is_char_device(self):
        """
        Whether this path is a character device.
        """
        try: return S_ISCHR(self.stat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    @property
    def _is_fifo(self):
        """
        Whether this path is a FIFO.
        """
        try: return S_ISFIFO(self.stat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False

    @property
    def _is_socket(self):
        """
        Whether this path is a socket.
        """
        try: return S_ISSOCK(self.stat().st_mode)
        except OSError as e:
            if not _ignore_error(e): raise
            return False
        except ValueError: return False


PathLike = TypeVar('PathLike', type(_Path), Path, _Path, os.PathLike, str, Union[str, _Path], Union[str, Path], Union[str, os.PathLike])

__all__ = [
    'Path',
    'PathLike'
]