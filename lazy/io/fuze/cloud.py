

"""
Cloud Fuze Implementations
"""


from typing import Optional
from .core import BaseFuzerCls


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
        elif authz.aws_access_token:
            _config['token'] = authz.aws_access_token
        elif not authz.boto_config:
            _config['anon'] = True
        return _config
    
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
        if authz.gauth: _config['token'] = authz.gauth
        if authz.gcloud_project or authz.google_cloud_project: _config['project'] = authz.gcloud_project or authz.google_cloud_project
        return _config
    
    