
import os
import typing
from typing import Callable, Dict, Tuple, Type, Union, TypeVar, List

from . import core
from . import cloud

PathLike = core.PathLike
ReadOnlyPath = core.ReadOnlyPath
ReadWritePath = core.ReadWritePath
PathLikeCls = Union[Type[ReadOnlyPath], Type[ReadWritePath]]

T = TypeVar('T')

_PATHLIKE_CLS: Tuple[PathLikeCls, ...] = (
    core.PosixFSxPath,
    core.WindowsFSxPath,
    cloud.PosixGCSPath,
    cloud.PosixS3Path,
    cloud.PosixMinioPath,
    cloud.PosixS3CompatPath,
    cloud.WindowsGCSPath,
    cloud.WindowsS3Path,
    cloud.WindowsMinioPath,
    cloud.WindowsS3CompatPath,
)

_URI_PREFIXES_TO_CLS: Dict[str, PathLikeCls] = {
    'gs://': cloud.PosixGCSPath,
    's3://': cloud.PosixS3Path,
    'minio://': cloud.PosixMinioPath,
    's3compat://': cloud.PosixS3CompatPath,
}


@typing.overload
def register_pathlike_cls(path_cls_or_uri_prefix: str) -> Callable[[T], T]:
    ...


@typing.overload
def register_pathlike_cls(path_cls_or_uri_prefix: T) -> T:
    ...


def register_pathlike_cls(path_cls_or_uri_prefix):
    global _PATHLIKE_CLS
    if isinstance(path_cls_or_uri_prefix, str):

        def register_pathlike_decorator(cls: T) -> T:
            _URI_PREFIXES_TO_CLS[path_cls_or_uri_prefix] = cls
            return register_pathlike_cls(cls)

        return register_pathlike_decorator
    else:
        _PATHLIKE_CLS = _PATHLIKE_CLS + (path_cls_or_uri_prefix,)
        return path_cls_or_uri_prefix


def as_path(path: PathLike) -> ReadWritePath:
    """Create a generic `pathlib.Path`-like abstraction.
    Depending on the input (e.g. `gs://`, `github://`, `ResourcePath`,...), the
    system (Windows, Linux,...), the function will create the right pathlib-like
    abstraction.
    Args:
        path: Pathlike object.
    Returns:
        path: The `pathlib.Path`-like abstraction.
    """
    is_windows = os.name == 'nt'
    if isinstance(path, str):
        uri_splits = path.split('://', maxsplit=1)
        if len(uri_splits) > 1:    # str is URI (e.g. `gs://`, `github://`,...)
            # On windows, `PosixGCSPath` is created for `gs://` paths
            return _URI_PREFIXES_TO_CLS[uri_splits[0] + '://'](path)
        elif is_windows: return cloud.WindowsGCSPath(path)
        else: return core.PosixFSxPath(path)
    elif isinstance(path, _PATHLIKE_CLS):
        return path
    elif isinstance(path, os.PathLike): # Other `os.fspath` compatible objects
        path_cls = core.WindowsFSxPath if is_windows else core.PosixFSxPath
        return path_cls(path)
    else: raise TypeError(f'Invalid path type: {path!r}')

def get_path(filepath: Union[str, PathLike]) -> _PATHLIKE_CLS:
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath

def get_pathlike(filepath: Union[str, PathLike]) -> _PATHLIKE_CLS:
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath