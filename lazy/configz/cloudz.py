

"""
Cloud Provider Configs
"""
from typing import Optional, TYPE_CHECKING

from lazy.types import pyd
from .core import ConfigCls

### need to refactor later since there's circular dependencies
### with PathStr

from .types import *
from logz import get_logger
logger = get_logger('cloudauthz')

__all__ = ('CloudAuthz')

class CloudAuthz(ConfigCls):
    authz_dir: PathStr = "./authz"
    boto_config: Optional[PathStr] = "~/.boto"
    
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
    
    gcloud_project: str = ""
    google_cloud_project: str = ""
    gauth: GoogleAuthBGZ = "" # Kept for compatability
    gcp_auth: GoogleAuthJsonStr = ""
    gcp_authb64: GoogleAuthB64 = ""
    gcp_authbgz: GoogleAuthBGZ = ""

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

    def get_s3_endpoint(self):
        return f'https://s3.{self.aws_region}.amazonaws.com'

    def get_boto_path(self):
        if self.boto_config and self.boto_config.exists(): return self.boto_config
        if not self.authz_dir.exist: self.authz_dir.mkdir(create_parents=True, exist_ok=True)
        return self.authz_dir.joinpath('.boto')
    
    def should_write_boto(self):
        return not bool(self.boto_config and self.boto_config.exists())
    
    def get_boto_values(self):
        t = "[Credentials]\n"
        if self.aws_access_key_id:
            t += f"aws_access_key_id = {self.aws_access_key_id}\n"
            t += f"aws_secret_access_key = {self.aws_secret_access_key}\n"
        if self.gauth and self.gauth.to_env_path.exists():
            t += f"gs_service_key_file = {self.gauth}\n"
        elif self.gcp_auth and self.gcp_auth.to_env_path.exists():
            t += f"gs_service_key_file = {self.gcp_auth}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        if self.gcloud_project or self.google_cloud_project:
            t += f"default_project_id = {self.gcloud_project or self.google_cloud_project}\n"
        return t


    def write_botofile(self, overwrite: bool = False, **kwargs):
        if self.should_write_boto:
            p = self.get_boto_path()
            if not p.exists() or overwrite:
                logger.info(f'Writing Botofile to {p.as_posix()}')
                p.write_text(self.get_boto_values())
            else: 
                logger.error(f'Botofile {p.as_posix()} exists and overwrite is False. Not overwriting')
            return p
        else: 
            logger.warning(f'Skipping writing Botofile as BotoConfig = {self.boto_config.as_posix()} exists')
            return self.boto_config


    def set_authz_env(self):
        from lazy.cmd.contrib import export
        if self.gcp_auth: export(GOOGLE_APPLICATION_CREDENTIALS=self.gcp_auth)
        elif self.gauth: export(GOOGLE_APPLICATION_CREDENTIALS=self.gauth)

        if self.gcloud_project: export(GOOGLE_CLOUD_PROJECT=self.gcloud_project or self.google_cloud_project)
        botopath = self.get_boto_path()
        ## We know this is our custom botofile
        if botopath.exists() and self.should_write_boto():
            export(BOTO_PATH=botopath.as_posix())
            export(BOTO_CONFIG=botopath.as_posix())
        if self.aws_access_key_id:
            export(AWS_ACCESS_KEY_ID=self.aws_access_key_id)
            export(AWS_SECRET_ACCESS_KEY=self.aws_secret_access_key)
        if self.set_s3_endpoint:
            export(S3_ENDPOINT=self.get_s3_endpoint())
        if self.minio_access_key:
            export(MINIO_ACCESS_KEY=self.minio_access_key)
            export(MINIO_SECRET_KEY=self.minio_secret_key)
        if self.minio_endpoint:
            export(MINIO_ENDPOINT=self.minio_endpoint)

    def update_authz(self, **config):
        self.update_config(**config)
        self.set_authz_env()
        
        


        