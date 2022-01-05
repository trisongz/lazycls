import os
import ntpath
import pathlib
import posixpath

from .core import PosixFSxPath

"""
Type Checking
"""

try: import s3fs
except ImportError: s3fs = object

try: import gcsfs
except ImportError: gcsfs = object

try: from lazy import CloudAuthz
except ImportError: CloudAuthz = object

_authz: CloudAuthz = None

def get_cloudauthz():
    global _authz
    if _authz is None:
        from lazy import CloudAuthz
        _authz = CloudAuthz
    return _authz


class PosixS3Path(PosixFSxPath, pathlib.PurePosixPath):
    """
    Pathlib-like API around `fsspec.s3fs` providing Async Capabilities
    """
    _PATH = posixpath
    _FSX: 's3fs.S3FileSystem' = None
    _SYNC_FS: 's3fs.S3FileSystem' = None
    _ASYNC_FS: 's3fs.S3FileSystem' = None
    _FSX_LIB: str = 's3fs'
    _FSX_MODULE: str = 'S3FileSystem'
    _AUTHZ: 'CloudAuthz' = None

    @property
    def async_fs(self) -> 's3fs.S3FileSystem':
        if not self._ASYNC_FS: self.get_filesystem(is_async=True)
        return self._ASYNC_FS
    
    @property
    def sync_fs(self) -> 's3fs.S3FileSystem':
        if not self._SYNC_FS: self.get_filesystem()
        return self._SYNC_FS
    
    @classmethod
    def get_authz(cls, reload: bool = False, **config):
        if cls._AUTHZ and not reload: return cls._AUTHZ
        cls._AUTHZ = get_cloudauthz()
        if config: cls._AUTHZ.update_authz(**config)
        return cls._AUTHZ

    @classmethod
    def get_auth_config(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.aws_access_key_id:
            _config['key'] = authz.aws_access_key_id
            _config['secret'] = authz.aws_secret_access_key
        elif authz.aws_access_token:
            _config['token'] = authz.aws_access_token
        elif not authz.boto_config:
            _config['anon'] = True
        return _config
    

class PosixGCSPath(PosixFSxPath, pathlib.PurePosixPath):
    """Pathlib-like API around `fsspec.gcsfs` providing Async Capabilities"""
    _PATH = posixpath
    _FSX: 'gcsfs.GCSFileSystem' = None
    _SYNC_FS: 'gcsfs.GCSFileSystem' = None
    _ASYNC_FS: 'gcsfs.GCSFileSystem' = None
    _FSX_LIB: str = 'gcsfs'
    _FSX_MODULE: str = 'GCSFileSystem'
    _AUTHZ: 'CloudAuthz' = None

    @classmethod
    def get_authz(cls, reload: bool = False, **config):
        if cls._AUTHZ and not reload: return cls._AUTHZ
        cls._AUTHZ = get_cloudauthz()
        if config: cls._AUTHZ.update_authz(**config)
        return cls._AUTHZ

    @classmethod
    def get_auth_config(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.gauth: _config['token'] = authz.gauth
        if authz.gcloud_project or authz.google_cloud_project: _config['project'] = authz.gcloud_project or authz.google_cloud_project
        return _config

    @property
    def async_fs(self) -> 'gcsfs.GCSFileSystem':
        if not self._ASYNC_FS: self.get_filesystem(is_async=True)
        return self._ASYNC_FS
    
    @property
    def sync_fs(self) -> 'gcsfs.GCSFileSystem':
        if not self._SYNC_FS: self.get_filesystem()
        return self._SYNC_FS


class WindowsGCSPath(PosixGCSPath, pathlib.PureWindowsPath):
    _PATH = ntpath

class WindowsS3Path(PosixS3Path, pathlib.PureWindowsPath):
    _PATH = ntpath


os.PathLike.register(PosixGCSPath)
os.PathLike.register(PosixS3Path)

os.PathLike.register(WindowsGCSPath)
os.PathLike.register(WindowsS3Path)
