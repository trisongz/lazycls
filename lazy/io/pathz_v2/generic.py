import os
import pathlib
from .base import *
from .providers.gs_gcp import *
from .providers.s3_aws import *
from .providers.s3_minio import *

from typing import List, Dict, Union, Type, Tuple

PathzLike = Union[
    Type[PathzPurePath],
    Type[PathzPath],
    Type[PurePathzPosixPath],
    Type[PathzWindowsPath],
    Type[PathzPosixPath],
    Type[PurePathzWindowsPath],
    Type[PathzGSPurePath],
    Type[PathzGSPath],
    Type[PurePathzGSPosixPath],
    Type[PathzGSWindowsPath],
    Type[PathzGSPosixPath],
    Type[PurePathzGSWindowsPath],
    Type[PathzS3PurePath],
    Type[PathzS3Path],
    Type[PurePathzS3PosixPath],
    Type[PathzS3WindowsPath],
    Type[PathzS3PosixPath],
    Type[PurePathzS3WindowsPath],
    Type[PathzMinioPurePath], Type[PathzMinioPath], Type[PurePathzMinioPosixPath], Type[PathzMinioWindowsPath], Type[PathzMinioPosixPath], Type[PurePathzMinioWindowsPath]

]

_PATHLIKE_CLS: Tuple[PathzLike, ...] = (
    PathzPurePath,
    PathzPath,
    PurePathzPosixPath,
    PathzWindowsPath,
    PathzPosixPath,
    PurePathzWindowsPath,
    PathzGSPurePath,
    PathzGSPath,
    PurePathzGSPosixPath,
    PathzGSWindowsPath,
    PathzGSPosixPath,
    PurePathzGSWindowsPath,
    PathzS3PurePath,
    PathzS3Path,
    PurePathzS3PosixPath,
    PathzS3WindowsPath,
    PathzS3PosixPath,
    PurePathzS3WindowsPath,
    PathzMinioPurePath, PathzMinioPath, PurePathzMinioPosixPath, PathzMinioWindowsPath, PathzMinioPosixPath, PurePathzMinioWindowsPath
)

FileSysLike = Union[
    Type[AWSFileSystem],
    Type[GCPFileSystem],
    Type[MinioFileSystem],
]

PathLike = Union[str, os.PathLike, PathzLike]

_PREFIXES_TO_CLS: Dict[str, PathzLike] = {
    'gs://': PathzGSPath,
    's3://': PathzS3Path,
    'minio://': PathzMinioPath,
    #'minio://': cloud.PosixMinioPath,
    #'s3compat://': cloud.PosixS3CompatPath,
}


def as_path(path: PathLike) -> PathzLike:
    """Create a generic `pathlib.Path`-like abstraction.
    Depending on the input (e.g. `gs://`, `github://`, `ResourcePath`,...), the
    system (Windows, Linux,...), the function will create the right pathlib-like
    abstraction.
    Args:
        path: Pathlike object.
    Returns:
        path: The `pathlib.Path`-like abstraction.
    """
    if isinstance(path, str):
        uri_splits = path.split('://', maxsplit=1)
        if len(uri_splits) > 1:    
            # str is URI (e.g. `gs://`, `github://`,...)
            return _PREFIXES_TO_CLS[uri_splits[0] + '://'](path)
        return PathzPath(path)
    elif isinstance(path, _PATHLIKE_CLS):
        return path
    elif isinstance(path, os.PathLike):
        return PathzPath(path)
    else: raise TypeError(f'Invalid path type: {path!r}')


def get_userhome(as_pathz: bool = True):
    h = os.path.expanduser('~')
    if as_pathz: return as_path(h)
    return h

def get_cwd():
    return os.getcwd()


def resolve_relative(filepath: PathLike) -> str:
    if not isinstance(filepath, str): filepath = filepath.as_posix()
    if '://' in filepath: return filepath
    if filepath.startswith('~'): filepath = filepath.replace('~', get_userhome(), 1)
    elif filepath.startswith('../'): filepath = filepath.replace('..', get_cwd(), 1)
    elif filepath.startswith('..'): filepath = filepath.replace('..', pathlib.Path(get_cwd()).parent.parent.as_posix() + '/', 1)
    elif filepath.startswith('./'): filepath = filepath.replace('.', get_cwd(), 1)
    elif filepath.startswith('.'): filepath = filepath.replace('.', pathlib.Path(get_cwd()).parent.as_posix() + '/', 1)
    return filepath

def get_path(filepath: PathLike, resolve: bool = False) -> PathzLike:
    if resolve: filepath = resolve_relative(filepath)
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath

def get_pathlike(filepath: PathLike, resolve: bool = False) -> PathzLike:
    if resolve: filepath = resolve_relative(filepath)
    if isinstance(filepath, str): filepath = as_path(filepath)
    return filepath

def get_lazydir(mkdir: bool = False):
    """
    returns resolved '~/.lazy'
    """
    p = get_userhome()
    p = p.joinpath('.lazy')
    if mkdir: p.mkdir(exist_ok=True, parents=True)
    return p

to_path = get_path
__all__ = (
    'get_path', 'as_path', 'PathLike', 'PathzPath', 'PathzS3Path', 'PathzGSPath', 'PathzLike', 'get_lazydir',
)