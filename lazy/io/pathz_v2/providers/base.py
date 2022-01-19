from __future__ import annotations

import os
import io
import inspect
import ntpath
import pathlib
import posixpath
from pathlib import PosixPath, WindowsPath, Path, PurePath
from pathlib import _NormalAccessor as NormalAccessor
from typing import Optional, List, Union, AsyncIterable, Iterable, IO, TYPE_CHECKING, AsyncContextManager, cast, Callable
from os import stat_result, PathLike
from contextlib import asynccontextmanager

from anyio import open_file
#from aiopath.wrap import coro_as_method_coro, func_as_method_coro, to_thread, method_as_method_coro, func_to_async_func
#from aiopath.handle import IterableAIOFile, get_handle
#from aiopath.types import Final, Literal, FileMode

from ..aiopathz.wrap import coro_as_method_coro, func_as_method_coro, to_thread, method_as_method_coro, func_to_async_func
from ..aiopathz.handle import IterableAIOFile, get_handle
from ..aiopathz.types import Final, Literal, FileMode

from fsspec.asyn import AsyncFileSystem
from anyio import AsyncFile, open_file
from lazy.serialize import Serialize
from lazy.io.pathz_v2.types import *
#from ..types import *
from .cloud_static import _ASYNC_SYNTAX_MAPPING

if TYPE_CHECKING:  # keep mypy quiet
    from lazy.io.pathz_v2.base import PathzPath

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

DEFAULT_ENCODING: Final[str] = 'utf-8'
ON_ERRORS: Final[str] = 'ignore'
NEWLINE: Final[str] = '\n'

BEGINNING: Final[int] = 0
CHUNK_SIZE: Final[int] = 4 * 1_024

SEP: Final[str] = '\n'
ENCODING: Final[str] = 'utf-8'
ERRORS: Final[str] = 'replace'


Paths = Union['PathzPath', Path, str]
FileData = Union[bytes, str]


def iscoroutinefunction(obj):
    if inspect.iscoroutinefunction(obj): return True
    if hasattr(obj, '__call__') and inspect.iscoroutinefunction(obj.__call__): return True
    return False

"""
Lol why so inconsistent.

"""

def rewrite_async_syntax(obj, provider: str = 's3'):
    """
    Basically - we're rewriting all the fsspec's async method
    from _method to async_method for syntax
    """
    _names = _ASYNC_SYNTAX_MAPPING[provider]
    for attr in dir(obj):
        if attr.startswith('_') and not attr.startswith('__'):
            attr_val = getattr(obj, attr)
            if iscoroutinefunction(attr_val) and _names.get(attr):
                setattr(obj, _names[attr], attr_val)
    return obj

Paths = Union[Path, PathLike, str]
Handle = AsyncFile


@asynccontextmanager
async def get_cloud_handle(name: Paths, mode: FileMode = 'r', buffering: int = -1, encoding: str | None = ENCODING, errors: str | None = ERRORS, newline: str | None = SEP) -> AsyncContextManager[Handle]:
    file: AsyncFile
    #if not isinstance(name, str): name = cast(IO[Union[str, bytes]], name)
    if 'b' in mode: file = await open_file(name, mode)
    else: file = await open_file(name, mode, encoding=encoding, errors=errors, newline=newline)
    yield file
    await file.aclose()


@asynccontextmanager
async def get_cloud_file(filelike: Paths) -> AsyncContextManager[Handle]:
    file: AsyncFile
    filelike = cast(IO[Union[str, bytes, os.PathLike]], filelike)
    file = AsyncFile(filelike)
    yield file
    await file.aclose()
