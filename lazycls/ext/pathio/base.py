"""
Patch Functions to modify pathlib.Path

borrowed some functions from https://github.com/KenKundert/extended_pathlib/blob/master/extended_pathlib.py
"""

import os
import sys
import codecs
import aiofiles
import tempfile
from pathlib import Path, PosixPath
from lazycls.prop import classproperty
from typing import TypeVar, Union, Type, List, Any, Optional, Callable, Dict


PathLike = TypeVar('PathLike', type(Path), Path, PosixPath, os.PathLike, str, Union[str, Path], Union[str, PosixPath], Union[str, os.PathLike])


def _is_readable(path):
    """
    Tests whether path exists and is readable.
    >>> from extended_pathlib import Path
    >>> Path('/usr/bin/python').is_readable()
    True
    """
    return os.access(str(path), os.R_OK)


PosixPath.is_readable = _is_readable


# is_writable {{{1
def _is_writable(path):
    """
    Tests whether path exists and is writable.
    >>> Path('/usr/bin/python').is_writable()
    False
    """
    return os.access(str(path), os.W_OK)


PosixPath.is_writable = _is_writable


# is_executable {{{1
def _is_executable(path):
    """
    Tests whether path exists and is executable.
    >>> Path('/usr/bin/python').is_executable()
    True
    """
    return os.access(str(path), os.X_OK)


PosixPath.is_executable = _is_executable


# is_hidden {{{1
def _is_hidden(path):
    """
    Tests whether path exists and is hidden.
    >>> Path('/usr/bin/python').is_hidden()
    False
    """
    return path.exists() and path.name.startswith(".")


PosixPath.is_hidden = _is_hidden


# is_newer {{{1
def _is_newer(path, ref):
    """
    Tests whether path is newer than ref where ref is either another path or a
    date.
    >>> Path('/usr/bin/python').is_newer(0)
    True
    """
    mtime = path.stat().st_mtime
    try: return mtime > ref
    except TypeError:
        try: return mtime > ref.timestamp
        except AttributeError: return mtime > ref.stat().st_mtime


PosixPath.is_newer = _is_newer


# path_from {{{1
def _path_from(path, start):
    """
    Returns relative path from start as a path object.
    This differs from Path.relative_to() in that relative_to() will not return a
    path that starts with '..'.
    >>> Path('.').path_from('..')
    PosixPath('tests')
    """
    return Path(os.path.relpath(str(path), str(start)))


PosixPath.path_from = _path_from


# sans_ext {{{1
def _sans_ext(path):
    """
    Removes the file extension.
    This differs from Path.stem, which returns the final path component
    stripped of its extension. This returns the full path stripped of its
    extension.
    >>> Path('a/b.c').sans_ext()
    PosixPath('a/b')
    """
    return path.parent / path.stem


PosixPath.sans_ext = _sans_ext

"""
Pulled from lazycls.io
"""

def _get_cwd() -> str:
    return os.getcwd()

def _get_user_home_dir() -> str:
    return os.path.expanduser('~')

@classproperty
def _working_dir(cls) -> PathLike:
    return cls(_get_cwd())

Path.working_dir = _working_dir

@classproperty
def _userhome(cls) -> PathLike:
    """Return a new path pointing to the user's home directory (as
    returned by os.path.expanduser('~')).
    """
    return cls(cls()._flavour.gethomedir(None))

Path.userhome = _userhome


@property
def _string(self) -> str:
    """Returns string representation of Path.as_posix()"""
    return self.as_posix()

Path.string = _string

@property
def _extension(self):
    """ alias for Path.suffix or the file extension"""
    return self.suffix

Path.extension = _extension

@property
def _exist(self) -> bool:
    """property value of path.exists()"""
    return self.exists()

Path.exist = _exist

def _to_pathlike(path: PathLike) -> PathLike:
    if isinstance(path, str): path = Path(path)
    return path

Path.to_pathlike = _to_pathlike

def _to_str_from_path(path: PathLike) -> PathLike:
    if not isinstance(path, str): 
        try: return path.string
        except: return path.as_posix()
    return path

Path.to_pathstr = _to_str_from_path

def _resolve_rel_str_path(path: PathLike) -> str:
    pathstr = _to_str_from_path(path)
    if pathstr.startswith('~'): pathstr = pathstr.replace('~', _get_user_home_dir(), 1)
    #elif pathstr.startswith('../'): pathstr = _get_cwd() + pathstr
    elif pathstr.startswith('../'): pathstr = pathstr.replace('..', _get_cwd(), 1)
    #elif pathstr.startswith('..'): pathstr = _get_cwd() + '/' + pathstr
    #elif pathstr.startswith('..'): pathstr = pathstr.replace('..', Path(_get_cwd()).parent.parent.as_posix() + '/', 1)
    elif pathstr.startswith('..'): pathstr = pathstr.replace('..', Path(_get_cwd()).parent.parent.as_posix() + '/', 1)
    elif pathstr.startswith('./'): pathstr = pathstr.replace('.', _get_cwd(), 1)
    #elif pathstr.startswith('.'): pathstr = pathstr.replace('.', _get_cwd() + '/', 1)
    elif pathstr.startswith('.'): pathstr = pathstr.replace('.', Path(_get_cwd()).parent.as_posix() + '/', 1)
    return pathstr

Path.resolve_to_pathstr = _resolve_rel_str_path

def _resolve_rel_path(path: PathLike) -> PathLike:
    pathstr = _resolve_rel_str_path(path)
    return Path(pathstr)


Path.resolve_to_path = _resolve_rel_path

def _get_path(path: PathLike, resolve: bool = True, ensure_exists: bool = False, mode=0o777, file_mode=0o666, parents=False, exist_ok: bool = True) -> PathLike:
    path = _resolve_rel_path(path)
    #if isinstance(path, str): path = Path(path)
    if resolve: path.resolve()
    if ensure_exists and path.is_dir(): path.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
    elif ensure_exists: path.touch(mode=file_mode, exist_ok=exist_ok)
    return path

Path.get_path = _get_path

def _get_user_path(path: PathLike, resolve: bool = False) -> PathLike:
    if isinstance(path, str): path = Path(path)
    path = path.expanduser()
    if resolve: path.resolve()
    return path

Path.get_user_path = _get_user_path

def _from_env(key: str, default: PathLike, **kwargs) -> PathLike:
    """ gets the path from env value, returns default if none"""
    val = os.getenv(key, None)
    if val is None: return _get_path(default, **kwargs)
    return _get_path(val, **kwargs)

Path.from_env = _from_env

@staticmethod
def _get_parent_path(path: PathLike, *args, **kwargs) -> PathLike:
    p = _get_path(path, *args, **kwargs)
    return p.parent

Path.get_parent_path = _get_parent_path

@staticmethod
def _get_lib_path(posix: bool = False) -> PathLike:
    p = _get_cwd()
    if posix: return p
    return Path(p)

Path.get_lib_path = _get_lib_path

@staticmethod
def _get_cwd_paths(*paths, posix: bool = True) -> PathLike:
    p = _get_cwd()
    if not paths:
        if posix: return p
        return Path(p)
    if posix: return Path(p).joinpath(*paths).as_posix()
    return Path(p).joinpath(*paths)

Path.get_cwd_paths = _get_cwd_paths

"""
Open extensions
"""

def _reader(self, mode: str = 'r', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
    """ Alias for open(r)"""
    return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

Path.reader = _reader
Path.read_mode = _reader
Path.in_reader = _reader

def _readerb(self, mode: str = 'rb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
    """ Alias for open(rb)"""
    return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

Path.readerb = _readerb
Path.reader_b = _readerb
Path.read_mode_b = _readerb
Path.binary_reader = _readerb
Path.b_reader = _readerb
Path.reader_binary = _readerb
Path.read_mode_binary = _readerb
Path.read_binary_mode = _readerb
Path.in_reader_b = _readerb

def _appender(self, mode: str = 'a', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
    """ Alias for open(a)"""
    return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

Path.appender = _appender
Path.append_mode = _appender
Path.in_appender = _appender

def _appenderb(self, mode: str = 'ab', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
    """ Alias for open(ab)"""
    return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

Path.appenderb = _appenderb
Path.appender_b = _appenderb
Path.append_mode_b = _appenderb
Path.binary_appender = _appenderb
Path.b_appender = _appenderb
Path.appender_binary = _appenderb
Path.append_mode_binary = _appenderb
Path.append_binary_mode = _appenderb
Path.in_appender_b = _appenderb

def _writer(self, mode: str = 'w', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
    """ Alias for open(w)"""
    return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

Path.writer = _writer
Path.write_mode = _writer
Path.in_writer = _writer

def _writerb(self, mode: str = 'wb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None):
    """ Alias for open(wb)"""
    return self.open(mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline)

Path.writerb = _writerb
Path.writer_b = _writerb
Path.write_mode_b = _writerb
Path.binary_writer = _writerb
Path.b_writer = _writerb
Path.writer_binary = _writerb
Path.write_mode_binary = _writerb
Path.write_binary_mode = _writerb
Path.in_writer_b = _writerb

from lazycls.serializers import Base, OrJson, Yaml, Pkl

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

def _to_base64(self, **kwargs):
    """ Reads the file as text, then turns to base64"""
    data = self.read_text(**kwargs)
    return Base.b64_encode(data)

Path.to_base64 = _to_base64
Path.to_b64 = _to_base64

def _to_gzip_base64(self, **kwargs):
    """ Reads the file as text, then turns to gzip+base64"""
    data = self.read_text(**kwargs)
    return Base.b64_gzip_encode(data)
    
Path.to_gzip_base64 = _to_gzip_base64
Path.to_bgz = _to_gzip_base64

@staticmethod
def _from_base64(data: Union[str, bytes], path: PathLike = None, encoding: str = 'utf-8', errors: str = None, **kwargs) -> PathLike:
    d = Base.b64_decode(data)
    if not path: return d
    p = _get_path(path, **kwargs)
    p.write_text(data=d, encoding=encoding, errors=errors)
    return p

Path.from_base64 = _from_base64
Path.from_b64 = _from_base64

@staticmethod
def _from_gzip_base64(data: Union[str, bytes], path: PathLike = None, encoding: str = 'utf-8', errors: str = None, **kwargs) -> PathLike:
    d = Base.b64_gzip_decode(data)
    if not path: return d
    p = _get_path(path, **kwargs)
    p.write_text(data=d, encoding=encoding, errors=errors)
    return p

Path.from_gzip_base64 = _from_gzip_base64
Path.from_bgz = _from_gzip_base64

def _read_bytes(self):
    """
    Open the file in bytes mode (rb), read it, and close the file.
    """
    with self.open(mode='rb') as f:
        return f.read()

Path.read_bytes = _read_bytes

def _read_text(self, encoding: str = 'utf-8', errors: str = None):
    """
    Open the file in text mode (r), read it, and close the file.
    """
    with self.open(mode='r', encoding=encoding, errors=errors) as f:
        return f.read()

Path.read_text = _read_text

def _read_text_to_lines(self, encoding: str = 'utf-8', errors: str = None, newlines: str = '\n', striplines: bool = True):
    data = self.read_text(encoding=encoding, errors=errors)
    lines = data.split(newlines)
    if striplines: lines = [l.strip() for l in lines]
    return lines

Path.read_textlines = _read_text_to_lines

def _read_json(self, encoding: str = 'utf-8', errors: str = None, **kwargs):
    data = self.read_text(encoding=encoding, errors=errors)
    return OrJson.loads(data, **kwargs)
    
Path.read_json = _read_json

def _read_yaml(self, encoding: str = 'utf-8', errors: str = None, **kwargs):
    data = self.read_text(encoding=encoding, errors=errors)
    return Yaml.loads(data, **kwargs)

Path.read_yaml = _read_yaml

def _read_pickle(self, encoding: str = 'utf-8', errors: str = None, **kwargs):
    data = self.read_bytes(encoding=encoding, errors=errors)
    return Pkl.loads(data, **kwargs)

Path.read_pickle = _read_pickle
Path.read_pkl = _read_pickle


def _append_bytes(self, data):
    """
    Open the file in bytes mode (ab), append to it, and close the file.
    """
    # type-check for the buffer interface before truncating the file
    view = memoryview(data)
    with self.open(mode='ab') as f:
        return f.write(view)

Path.append_bytes = _append_bytes

def _append_text(self, data: str, encoding: str ='utf-8', errors: str = None, insert_newline: bool = True):
    """
    Open the file in text mode (a), write to it, and close the file.
    """
    if not isinstance(data, str): raise TypeError('data must be str, not %s' % data.__class__.__name__)
    with self.open(mode='a', encoding=encoding, errors=errors) as f:
        f.write(data)
        if insert_newline: f.write('\n')
        return f

Path.append_text = _append_text


def _write_text(self, data: str, encoding: str ='utf-8', errors: str = None):
    """
    Open the file in text mode (w), write to it, and close the file.
    """
    if not isinstance(data, str): raise TypeError('data must be str, not %s' % data.__class__.__name__)
    with self.open(mode='w', encoding=encoding, errors=errors) as f:
        return f.write(data)

Path.write_text = _write_text

def _write_bytes(self, data):
    """
    Open the file in bytes mode (wb), write to it, and close the file.
    """
    # type-check for the buffer interface before truncating the file
    view = memoryview(data)
    with self.open(mode='wb') as f:
        return f.write(view)

Path.write_bytes = _write_bytes

def _write_json(self, data: Any, encoding: str = 'utf-8', errors: str = None, **kwargs):
    return self.write_text(OrJson.dumps(data, **kwargs), encoding=encoding, errors=errors)

Path.write_json = _write_json

def _write_yaml(self, data: Any, encoding: str = 'utf-8', errors: str = None, **kwargs):
    return self.write_text(Yaml.dumps(data, **kwargs), encoding=encoding, errors=errors)

Path.write_yaml = _write_yaml

def _write_pickle(self, data: Any, **kwargs):
    return self.write_bytes(Pkl.dumps(data, **kwargs))

Path.write_pickle = _write_pickle
Path.write_pkl = _write_pickle

def _get_loader(self, loader: str = None):
    """ If the file is supported (pkl, yaml, json) - then return the loader"""
    if loader: return _LOADERS.get(loader, _LOADERS.get(self.suffix, None))
    return _LOADERS.get(self.extension, None)

Path.get_loader = _get_loader

def _loads(self, encoding: str ='utf-8', errors: str = None, loader: str = None, binary: bool = False, **kwargs) -> Any:
    """ If the file is supported (pkl, yaml, json) - then return the loader.loads(**kwargs) method.
        else:
            - if binary: returns .read_bytes()
                else: returns .read_text()
    """
    l = self.get_loader(loader=loader)
    if l: return l.loads(self.read_text(encoding=encoding, errors=errors), **kwargs)
    if binary: return self.read_bytes()
    return self.read_text(encoding=encoding, errors=errors)

Path.loads = _loads

def _dumps(self, data: Any, encoding: str ='utf-8', errors: str = None, loader: str = None, **kwargs) -> Any:
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

Path.dumps = _dumps

def _get_files(self, pattern: str = '*') -> List[PathLike]:
    """
    Uses the .glob(pattern) to construct List[PathLike] instead of returning an iterator
    """
    p_iter = self.glob(pattern)
    return [p for p in p_iter if p.is_file]

Path.get_files = _get_files

@staticmethod
def _get_tempfile(mode: str = 'w', encoding: str = 'utf-8', dir: Any = None, buffering: int = -1, newline: str = None, delete: bool = False, **kwargs):
    return tempfile.NamedTemporaryFile(mode=mode, encoding=encoding, buffering=buffering, newline=newline, delete=delete, dir=dir, **kwargs)

Path.get_tempfile = _get_tempfile

@staticmethod
def _get_tempdir(dir: Any = None, **kwargs):
    return tempfile.TemporaryDirectory(dir=dir, **kwargs)

Path.get_tempdir = _get_tempdir

"""
Async Functions and Extensions
"""

from lazycls.ext._imports import LazyLib

@classproperty
def _async_os(cls):
    """ Aliases for aiofiles.os"""
    return aiofiles.os

Path.async_os = _async_os

async def _async_open(self, mode: str = 'r', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(r)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_open = _async_open


async def _async_read(self, mode: str = 'r', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(r)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_reader = _async_read
Path.async_read_mode = _async_read

async def _async_read_b(self, mode: str = 'rb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(rb)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_reader_b = _async_read_b
Path.async_read_mode_b = _async_read_b

async def _async_append(self, mode: str = 'a', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(a)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_appender = _async_append
Path.async_append_mode = _async_append

async def _async_append_b(self, mode: str = 'ab', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(ab)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_appender_b = _async_append_b
Path.async_append_mode_b = _async_append_b

async def _async_write(self, mode: str = 'w', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(w)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_writer = _async_write
Path.async_write_mode = _async_write

async def _async_write_b(self, mode: str = 'wb', encoding: str = 'utf-8', errors: str = None, buffering: int = -1, newline: str = None, closefd: bool = True, opener: Optional[Callable[[str, int], int]] = None, **kwargs):
    """ Alias for aiofiles.open(wb)"""
    return await aiofiles.open(self.as_posix(), mode=mode, encoding=encoding, errors=errors, buffering=buffering, newline=newline, closefd=closefd, opener=opener, **kwargs)

Path.async_writer_b = _async_write_b
Path.async_write_mode_b = _async_write_b

async def _async_read_bytes(self, **kwargs):
    """
    Open the file in bytes mode (rb), read it, and close the file.
    """
    async with self.async_open(mode='rb', **kwargs) as f:
        return await f.read()

Path.async_read_bytes = _async_read_bytes

async def _async_read_text(self, encoding: str = 'utf-8', errors: str = None, **kwargs):
    """
    Open the file in text mode (r), read it, and close the file.
    """
    async with self.async_open(mode='r', encoding=encoding, errors=errors, **kwargs) as f:
        return await f.read()

Path.async_read_text = _async_read_text

@staticmethod
async def _async_get_tempfile(mode: str = 'w', encoding: str = 'utf-8', dir: Any = None, buffering: int = -1, newline: str = None, delete: bool = False, **kwargs):
    return await aiofiles.tempfile.NamedTemporaryFile(mode=mode, encoding=encoding, buffering=buffering, newline=newline, delete=delete, dir=dir, **kwargs)

Path.async_get_tempfile = _async_get_tempfile

@staticmethod
async def _async_get_tempdir(dir: Any = None, **kwargs):
    return await aiofiles.tempfile.TemporaryDirectory(dir=dir, **kwargs)

Path.async_get_tempdir = _async_get_tempdir

if LazyLib.is_avail_requests:
    _reqs = LazyLib.requests
    
    @staticmethod
    def _from_url(url: str, path: PathLike, overwrite: bool = False, **kwargs) -> PathLike:
        """ Downloads a file from url and saves to  file """
        p = _get_path(path, resolve=True)
        if p.exists() and not overwrite: return p
        r = _reqs.get(url=url, **kwargs)
        if r.status_code >= 400: raise Exception(f'url = {url} returned status_code = {r.status_code}')
        p.write_bytes(data=r.content)
        return p
    
    Path.from_url = _from_url   

if LazyLib.is_avail_httpx:
    _httpx = LazyLib.httpx
    
    @staticmethod
    async def _async_from_url(url: str, path: PathLike, overwrite: bool = False, client_args: Dict[str, Any] = {}, **kwargs) -> PathLike:
        """ Downloads a file from url and saves to  file """
        p = _get_path(path, resolve=True)
        if p.exists() and not overwrite: return p
        async with _httpx.AsyncClient(url, **client_args) as c:
            r = await c.get(url=url, **kwargs)
        if r.status_code >= 400: raise Exception(f'url = {url} returned status_code = {r.status_code}')
        p.write_bytes(data=r.content)
        return p

    Path.async_from_url = _async_from_url

