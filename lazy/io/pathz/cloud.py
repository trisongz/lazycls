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
    AWS S3 Pathlib-like API around `fsspec.s3fs` providing Async Capabilities
    """
    _PATH = posixpath
    _FSX: 's3fs.S3FileSystem' = None
    _SYNC_FS: 's3fs.S3FileSystem' = None
    _ASYNC_FS: 's3fs.S3FileSystem' = None
    _FSX_LIB: str = 's3fs'
    _FSX_CLS: str = 'S3FileSystem'
    _FSX_MODULE = None
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
    def get_configz(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.aws_access_key_id:
            _config['key'] = authz.aws_access_key_id
            _config['secret'] = authz.aws_secret_access_key
        elif authz.aws_access_token: _config['token'] = authz.aws_access_token
        elif not authz.boto_config: _config['anon'] = True
        if authz.s3_config: _config['config_kwargs'] = authz.s3_config
        return _config
    

class PosixGCSPath(PosixFSxPath, pathlib.PurePosixPath):
    """
    Google Cloud Storage Pathlib-like API around `fsspec.gcsfs` providing Async Capabilities
    """
    _PATH = posixpath
    _FSX: 'gcsfs.GCSFileSystem' = None
    _SYNC_FS: 'gcsfs.GCSFileSystem' = None
    _ASYNC_FS: 'gcsfs.GCSFileSystem' = None
    _FSX_LIB: str = 'gcsfs'
    _FSX_CLS: str = 'GCSFileSystem'
    _FSX_MODULE = None
    _AUTHZ: 'CloudAuthz' = None

    @classmethod
    def get_authz(cls, reload: bool = False, **config):
        if cls._AUTHZ and not reload: return cls._AUTHZ
        cls._AUTHZ = get_cloudauthz()
        if config: cls._AUTHZ.update_authz(**config)
        return cls._AUTHZ

    @classmethod
    def get_configz(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.gcp_auth: _config['token'] = authz.gcp_auth
        if authz.gcloud_project or authz.google_cloud_project: _config['project'] = authz.gcloud_project or authz.google_cloud_project
        if authz.gcs_client_config: _config['client_kwargs'] = authz.gcs_client_config
        if authz.gcs_config: _config['config_kwargs'] = authz.gcs_config
        return _config

    @property
    def async_fs(self) -> 'gcsfs.GCSFileSystem':
        if not self._ASYNC_FS: self.get_filesystem(is_async=True)
        return self._ASYNC_FS
    
    @property
    def sync_fs(self) -> 'gcsfs.GCSFileSystem':
        if not self._SYNC_FS: self.get_filesystem()
        return self._SYNC_FS


class PosixMinioPath(PosixS3Path, pathlib.PurePosixPath):
    """
    Minio-S3 Pathlib-like API around `fsspec.s3fs` providing Async Capabilities
    """
    _PATH = posixpath
    _FSX: 's3fs.S3FileSystem' = None
    _SYNC_FS: 's3fs.S3FileSystem' = None
    _ASYNC_FS: 's3fs.S3FileSystem' = None
    _FSX_LIB: str = 's3fs'
    _FSX_CLS: str = 'S3FileSystem'
    _FSX_MODULE = None
    _AUTHZ: 'CloudAuthz' = None

    @classmethod
    def get_configz(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.minio_secret_key:
            _config['key'] = authz.minio_access_key
            _config['secret'] = authz.minio_secret_key
        elif authz.minio_access_token: _config['token'] = authz.minio_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.minio_endpoint}
        if authz.minio_config: _config['config_kwargs'] = authz.minio_config
        return _config
    

class PosixS3CompatPath(PosixS3Path, pathlib.PurePosixPath):
    """
    S3-Compatible Pathlib-like API around `fsspec.s3fs` providing Async Capabilities
    """
    _PATH = posixpath
    _FSX: 's3fs.S3FileSystem' = None
    _SYNC_FS: 's3fs.S3FileSystem' = None
    _ASYNC_FS: 's3fs.S3FileSystem' = None
    _FSX_LIB: str = 's3fs'
    _FSX_CLS: str = 'S3FileSystem'
    _FSX_MODULE = None
    _AUTHZ: 'CloudAuthz' = None

    @classmethod
    def get_configz(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.s3compat_secret_key:
            _config['key'] = authz.s3compat_access_key
            _config['secret'] = authz.s3compat_secret_key
        elif authz.s3compat_access_token: _config['token'] = authz.s3compat_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.s3compat_endpoint}
        if authz.s3compat_region: _config['client_kwargs']['region_name'] = authz.s3compat_region
        if authz.s3compat_config: _config['config_kwargs'] = authz.s3compat_config
        return _config



class WindowsGCSPath(PosixGCSPath, pathlib.PureWindowsPath):
    _PATH = ntpath

class WindowsS3Path(PosixS3Path, pathlib.PureWindowsPath):
    _PATH = ntpath

class WindowsMinioPath(PosixMinioPath, pathlib.PureWindowsPath):
    _PATH = ntpath

class WindowsS3CompatPath(PosixS3CompatPath, pathlib.PureWindowsPath):
    _PATH = ntpath


os.PathLike.register(PosixGCSPath)
os.PathLike.register(PosixS3Path)
os.PathLike.register(PosixMinioPath)
os.PathLike.register(PosixS3CompatPath)

os.PathLike.register(WindowsGCSPath)
os.PathLike.register(WindowsS3Path)
os.PathLike.register(WindowsMinioPath)
os.PathLike.register(WindowsS3CompatPath)

