from typing import Dict, Any, Union, Type

from .core import run_fuze, BaseFuzerCls, MountPoint
from .cloud import GCSFuze, S3Fuze, MinioFuze, S3CompatFuze

FuzeTypes = Union[type(BaseFuzerCls), GCSFuze, S3Fuze, MinioFuze, S3CompatFuze]

_URI_PREFIXES_TO_CLS: Dict[str, FuzeTypes] = {
    'gs://': GCSFuze,
    's3://': S3Fuze,
    'minio://': MinioFuze,
    's3compat://': S3CompatFuze,
}

def get_fuze(source: str) -> FuzeTypes:
    uri_splits = source.split('://', maxsplit=1)
    if len(uri_splits) > 1:
        return _URI_PREFIXES_TO_CLS[uri_splits[0] + '://']
    return BaseFuzerCls


def autofuze_mount(source: str, mount_path: str, ready_file: bool = True, foreground: bool = False, threads: bool = False, fs_args: Dict[str, Any] = {}, cleanup: bool = True, *args, **kwargs) -> FuzeTypes:
    """ 
    Mounts a source path/bucket/etc with this FuzeCls at mount_path

    source: Bucket/path that will be the source of the mount
    mount_path: the directory that will be mounted to
    foreground: return a thread blocking function, otherwise will be a background thread that will need to be killed explicitly.
    fs_args: Dict[str, Any] that will be passed when initializing the FileSystem (such as auth)
    cleanup: removes the mount_path on unmount
    """
    uri_splits = source.split('://', maxsplit=1)
    if len(uri_splits) > 1:
        fuzecls = _URI_PREFIXES_TO_CLS[uri_splits[0] + '://']
        source_path = uri_splits[-1]
    else:
        fuzecls = BaseFuzerCls
        source_path = source
    fuzecls.mount(source_path, mount_path, ready_file = ready_file, foreground = foreground, threads = threads, fs_args = fs_args, cleanup = cleanup, *args, **kwargs)
    return fuzecls


def autofuze_unmount(source: str, mount_path: str, timeout: int = 5, force: bool = False, *args, **kwargs) -> FuzeTypes:
    uri_splits = source.split('://', maxsplit=1)
    if len(uri_splits) > 1:
        fuzecls = _URI_PREFIXES_TO_CLS[uri_splits[0] + '://']
    else:
        fuzecls = BaseFuzerCls
    fuzecls.unmount(mount_path, timeout = timeout, force = force)
    return fuzecls