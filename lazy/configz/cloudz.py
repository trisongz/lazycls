

"""
Cloud Provider Configs
"""
from typing import Optional
from lazy.types import pyd
from .core import ConfigCls
from .types import *


## not sure if needed
from logz import get_logger
logger = get_logger('cloudauthz')

__all__ = ('CloudAuthz')

class CloudAuthz(ConfigCls):
    authz_dir: PathStr = "./authz"
    boto_config: Optional[PathStr] = ""
    
    """ 
    AWS Specific 
    """
    aws_access_token: Optional[str] = ""
    aws_access_key_id: Optional[str] = ""
    aws_secret_access_key: Optional[str] = ""
    aws_region: Optional[str] = "us-east-1"
    set_s3_endpoint: Optional[bool] = True
    s3_config: Optional[pyd.Json] = None

    """ 
    GCP Specific 
    """
    
    gcloud_project: Optional[str] = ""
    google_cloud_project: Optional[str] = ""
    gauth: Optional[GoogleAuthBGZ] = "" # Kept for compatability
    gcp_auth: GoogleAuthBGZ = ""
    gcp_auth: GoogleAuthBGZ = ""
    gcs_client_config: Optional[pyd.Json] = None
    gcs_config: Optional[pyd.Json] = None

    """
    Minio Specific
    """
    minio_endpoint: Optional[str] = ""
    minio_access_key: Optional[str] = ""
    minio_secret_key: Optional[str] = ""
    minio_access_token: Optional[str] = ""
    minio_config: Optional[pyd.Json] = None

    """
    S3-Compatiable Generic
    """
    s3compat_endpoint: Optional[str] = ""
    s3compat_region: Optional[str] = ""
    s3compat_access_key: Optional[str] = ""
    s3compat_secret_key: Optional[str] = ""
    s3compat_access_token: Optional[str] = ""
    s3compat_config: Optional[pyd.Json] = None

    class Config:
        auto_init = True

    @classmethod
    def get(cls, value: str, default=None):
        return cls.dict().get(value, default)

    @classmethod
    def get_s3_endpoint(cls):
        #_authz = cls()
        return f'https://s3.{cls.aws_region}.amazonaws.com'

    @classmethod
    def get_boto_path(cls):
        if cls.boto_config and cls.boto_config.exists(): return cls.boto_config
        cls.authz_dir.mkdir(parents=True, exist=True)
        return cls.authz_dir.cast().joinpath('.boto')
        #_authz = cls()
        #if _authz.boto_config and _authz.boto_config.exists(): return _authz.boto_config
        #_authz.authz_dir.mkdir(parents=True, exist=True)
        #return _authz.authz_dir.cast().joinpath('.boto')
    
    @classmethod
    def should_write_boto(cls):
        return not bool(cls.boto_config and cls.boto_config.exists())
        #_authz = cls()
        #return not bool(_authz.boto_config and _authz.boto_config.exists())

    @classmethod
    def get_boto_values(cls):
        t = "[Credentials]\n"
        if cls.get('aws_access_key_id'):
            t += f"aws_access_key_id = {cls.get('aws_access_key_id')}\n"
            t += f"aws_secret_access_key = {cls.get('aws_secret_access_key')}\n"
        if cls.get('gauth') and cls.get('gauth').to_env_path.exists():
            t += f"gs_service_key_file = {cls.get('gauth')}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        if cls.get('gcloud_project') or cls.get('google_cloud_project'):
            t += f"default_project_id = {cls.get('gcloud_project') or cls.get('google_cloud_project')}\n"
        return t

    @classmethod
    def _get_boto_values(cls):
        _authz = cls()
        t = "[Credentials]\n"
        if _authz.aws_access_key_id:
            t += f"aws_access_key_id = {_authz.aws_access_key_id}\n"
            t += f"aws_secret_access_key = {_authz.aws_secret_access_key}\n"
        if _authz.gauth.to_env_path.exists():
            t += f"gs_service_key_file = {_authz.gauth}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        if _authz.gcloud_project or _authz.google_cloud_project:
            t += f"default_project_id = {_authz.gcloud_project or _authz.google_cloud_project}\n"
        return t

    @classmethod
    def write_botofile(cls, overwrite: bool = False, **kwargs):
        #_authz = cls()
        if cls.should_write_boto:
            p = cls.get_boto_path()
            if not p.exists() or overwrite:
                logger.info(f'Writing Botofile to {p.as_posix()}')
                p.write_text(cls.get_boto_values())
            else: 
                logger.error(f'Botofile {p.as_posix()} exists and overwrite is False. Not overwriting')
            return p
        else: 
            logger.warning(f'Skipping writing Botofile as BotoConfig = {cls.get("boto_config").as_posix()} exists')
            return cls.boto_config


    @classmethod
    def set_authz_env(cls):
        from lazy.cmd.contrib import export
        #_authz = cls()
        if cls.get('gcp_auth'): export(GOOGLE_APPLICATION_CREDENTIALS=cls.gcp_auth)
        elif cls.get('gauth'): export(GOOGLE_APPLICATION_CREDENTIALS=cls.gauth)

        if cls.gcloud_project: export(GOOGLE_CLOUD_PROJECT=cls.gcloud_project or cls.google_cloud_project)
        botopath = cls.get_boto_path()
        ## We know this is our custom botofile
        if botopath.exists() and cls.should_write_boto():
            export(BOTO_PATH=botopath.as_posix())
            export(BOTO_CONFIG=botopath.as_posix())
        if cls.aws_access_key_id:
            export(AWS_ACCESS_KEY_ID=cls.aws_access_key_id)
            export(AWS_SECRET_ACCESS_KEY=cls.aws_secret_access_key)
        if cls.set_s3_endpoint:
            export(S3_ENDPOINT=cls.get_s3_endpoint())
        if cls.minio_access_key:
            export(MINIO_ACCESS_KEY=cls.minio_access_key)
            export(MINIO_SECRET_KEY=cls.minio_secret_key)
        if cls.minio_endpoint:
            export(MINIO_ENDPOINT=cls.minio_endpoint)
    
    @classmethod
    def update_authz(cls, **config):
        cls().update_config(**config)
        cls.set_authz_env()
        
        


        