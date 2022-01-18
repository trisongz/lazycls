

"""
Cloud Provider Configs
"""
import pathlib
from typing import Optional, TYPE_CHECKING

from lazy.types import pyd
from .core import ConfigCls

### need to refactor later since there's circular dependencies
### with PathStr
from .types import *
from logz import get_logger
logger = get_logger('cloudauthz')

__all__ = ('CloudAuthz')

try:
    from google.colab import drive
    _is_colab = True
except ImportError: _is_colab = False

class CloudAuthz(ConfigCls):
    authz_dir: Optional['AuthzDir'] = "~/.authz"
    boto_config: Optional['PathStr'] = "~/.boto"
    
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

    gcp_project: str = ""
    gcloud_project: str = ""
    google_cloud_project: str = ""
    
    gauth: 'GoogleAuthBGZ' = "" # Kept for compatability
    gcp_auth: 'GoogleAuthJsonStr' = ""
    gcp_authb64: 'GoogleAuthB64' = ""
    gcp_authbgz: 'GoogleAuthBGZ' = ""

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

    def get_gcp_project(self):
        for v in [self.gcp_project, self.gcloud_project, self.google_cloud_project]:
            if v: return v

    def get_gcp_auth(self):
        for v in [self.gauth, self.gcp_auth, self.gcp_authb64, self.gcp_authbgz]:
            if v: 
                if isinstance(v, str): v = pathlib.Path(v)
                if v.exists(): return v

    def get_authz_path(self):
        if not self.authz_dir:
            self.authz_dir = pathlib.Path('/content/authz') if _is_colab else pathlib.Path('~/.authz').resolve(True)
        return self.authz_dir

    def get_boto_config_path(self):
        if not self.boto_config:
            self.boto_config = pathlib.Path('/root/.boto') if _is_colab else pathlib.Path('~/.boto').resolve(True)
        return self.boto_config


    def get_s3_endpoint(self):
        return f'https://s3.{self.aws_region}.amazonaws.com'

    def get_boto_path(self):
        boto_config = self.get_boto_config_path()
        if boto_config.exists(): return boto_config
        authz_dir = self.get_authz_path()
        authz_dir.mkdir(create_parents=True, exist_ok=True)
        return authz_dir.joinpath('.boto')
    
    def should_write_boto(self):
        boto_config = self.get_boto_config_path()
        return not boto_config.exists()
 
    def get_boto_values(self):
        t = "[Credentials]\n"
        if self.aws_access_key_id:
            t += f"aws_access_key_id = {self.aws_access_key_id}\n"
            t += f"aws_secret_access_key = {self.aws_secret_access_key}\n"
        #if self.gauth and self.gauth.to_env_path.exists():
        gcp_auth = self.get_gcp_auth()
        if gcp_auth.exists():
            t += f"gs_service_key_file = {gcp_auth.as_posix()}\n"
        t += "\n[Boto]\n"
        t += "https_validate_certificates = True\n"
        t += "\n[GSUtil]\n"
        t += "content_language = en\n"
        t += "default_api_version = 2\n"
        gcp_project = self.get_gcp_project()
        if gcp_project:
            t += f"default_project_id = {gcp_project}\n"
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
        gcp_auth = self.get_gcp_auth()
        if gcp_auth and gcp_auth.exists(): export(GOOGLE_APPLICATION_CREDENTIALS=gcp_auth)
        gcp_project = self.get_gcp_project()
        if gcp_project: export(GOOGLE_CLOUD_PROJECT=gcp_project)

        try:
            botopath = self.get_boto_path()
            ## We know this is our custom botofile
            if botopath.exists() and self.should_write_boto():
                export(BOTO_PATH=botopath.as_posix())
                export(BOTO_CONFIG=botopath.as_posix())
        except: pass
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
        
        


        