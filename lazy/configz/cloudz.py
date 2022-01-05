

"""
Cloud Provider Configs
"""
from typing import Optional
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
    aws_region: str = "us-east-1"
    set_s3_endpoint: Optional[bool] = True

    """ 
    GCP Specific 
    """
    gauth: GoogleAuthBGZ = ""
    gcloud_project: Optional[str] = ""
    google_cloud_project: Optional[str] = ""

    """
    Minio Specific
    """
    minio_endpoint: Optional[str] = ""
    minio_access_key: Optional[str] = ""
    minio_secret_key: Optional[str] = ""


    @classmethod
    def get_s3_endpoint(cls):
        return f'https://s3.{cls.aws_region}.amazonaws.com'

    @classmethod
    def get_boto_path(cls):
        if cls.boto_config and cls.boto_config.exists(): return cls.boto_config
        cls.authz_dir.mkdir(parents=True, exist=True)
        return cls.authz_dir.joinpath('.boto')
    
    @classmethod
    def should_write_boto(cls): 
        return not bool(cls.boto_config and cls.boto_config.exists())

    @classmethod
    def get_boto_values(cls):
        t = "[Credentials]\n"
        if cls.aws_access_key_id:
            t += f"aws_access_key_id = {cls.aws_access_key_id}\n"
            t += f"aws_secret_access_key = {cls.aws_secret_access_key}\n"
        if cls.gauth.to_env_path.exists():
            t += f"gs_service_key_file = {cls.gauth}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        if cls.gcloud_project or cls.google_cloud_project:
            t += f"default_project_id = {cls.gcloud_project or cls.google_cloud_project}\n"
        return t

    @classmethod
    def write_botofile(cls, overwrite: bool = False, **kwargs):
        if cls.should_write_boto:
            p = cls.get_boto_path()
            if not p.exists() or overwrite:
                logger.info(f'Writing Botofile to {p.as_posix()}')
                p.write_text(cls.get_boto_values())
            else: 
                logger.error(f'Botofile {p.as_posix()} exists and overwrite is False. Not overwriting')
            return p
        else: 
            logger.warning(f'Skipping writing Botofile as BotoConfig = {cls.boto_config.as_posix()} exists')
            return cls.boto_config


    @classmethod
    def set_authz_env(cls):
        from lazy.cmd.contrib import export
        if cls.gauth: export(GOOGLE_APPLICATION_CREDENTIALS=cls.gauth)
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
        new_cls = cls().update_config(**config)
        cls = new_cls
        cls.set_authz_env()
        cls.reload()

        


        