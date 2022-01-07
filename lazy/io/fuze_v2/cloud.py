

"""
Cloud Fuze Implementations

libfuse-dev
libfuse3-dev
fuse3

# -Displaymode 0
"""


from typing import Optional
from .core import BaseFuzerCls


class GCSFuze(BaseFuzerCls):
    """
    GCS Fuze
    """
    _FSX_LIB: str = 'gcsfs'
    _FSX_MODULE: Optional[str] = None
    _FSX_CLS: str = 'GCSFileSystem'

    @classmethod
    def get_configz(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.gcp_auth: _config['token'] = authz.gcp_auth
        if authz.gcloud_project or authz.google_cloud_project: _config['project'] = authz.gcloud_project or authz.google_cloud_project
        if authz.gcs_client_config: _config['client_kwargs'] = authz.gcs_client_config
        if authz.gcs_config: _config['config_kwargs'] = authz.gcs_config
        return _config

class S3Fuze(BaseFuzerCls):
    """
    S3 Fuze
    """
    _FSX_LIB: str = 's3fs'
    _FSX_MODULE: Optional[str] = None
    _FSX_CLS: str = 'S3FileSystem'

    @classmethod
    def get_configz(cls, reload: bool = False, **config):
        authz = cls.get_authz(reload=reload, **config)
        _config = {}
        if authz.aws_access_key_id:
            _config['key'] = authz.aws_access_key_id
            _config['secret'] = authz.aws_secret_access_key
        
        elif authz.aws_access_token: _config['token'] = authz.aws_access_token
        elif not authz.boto_config: _config['anon'] = True
        if authz.aws_region: _config['client_kwargs'] = {'endpoint_url': authz.get_s3_endpoint(), 'region_name': authz.aws_region}
        if authz.s3_config: _config['config_kwargs'] = authz.s3_config
        return _config


class MinioFuze(BaseFuzerCls):
    """
    Minio Fuze based on S3
    """
    _FSX_LIB: str = 's3fs'
    _FSX_MODULE: Optional[str] = None
    _FSX_CLS: str = 'S3FileSystem'

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

    

class S3CompatFuze(BaseFuzerCls):
    """
    S3Compatible Generic Fuze based for providers like digitalocean, etc.
    """
    _FSX_LIB: str = 's3fs'
    _FSX_MODULE: Optional[str] = None
    _FSX_CLS: str = 'S3FileSystem'

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
