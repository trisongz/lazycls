import re
from pydantic import Field
from lazy.types.base import *
from lazy.types.pyd import *

from .core import ConfigCls
from .types import *

__all__ = (
    'DBBaseConfigCls',
    'PostgresConfigz',
    'MysqlConfigz',
    'RedisConfigz',
    'ElasticsearchConfigz',
    'SMTPEmailBaseConfigCls',
    'EmailBaseConfigCls',
    'AppBaseConfigCls',
    'FastAPIBaseConfigCls',
)


"""
Database Configs
"""
class DBBaseConfigCls(ConfigCls):
    host: Optional[str] = "127.0.0.1"
    port: Optional[int] = 5432
    username: Optional[str] = None
    password: Optional[str] = None
    config: Optional[DictStr] = None
    database: Optional[str] = None

class PostgresConfigz(DBBaseConfigCls):
    username: Optional[str] = 'postgres'
    database: Optional[str] = 'postgres'

class MysqlConfigz(DBBaseConfigCls):
    port: Optional[int] = 3306
    username: Optional[str] = 'mysql'
    database: Optional[str] = 'mysql'
    
class RedisConfigz(DBBaseConfigCls):
    port: Optional[int] = 6379
    database: Optional[int] = 0

class ElasticsearchConfigz(DBBaseConfigCls):
    port: Optional[int] = 9200


"""
Commonly Used App Configs

To use for your specific app, you can subclass AppBaseConfigCls

MyAppConfigz(AppBaseConfigCls): pass

all env variables would be translated be prefixed with MYAPP_
title       -> MYAPP_TITLE
description -> MYAPP_DESCRIPTION

"""
class SMTPEmailBaseConfigCls(ConfigCls):
    tls: Optional[bool] = True
    port: Optional[int] = 587
    host: Optional[str] = 'smtp.gmail.com'
    user: Optional[str] = 'user'
    password: Optional[str] = 'password'

    class Config:
        env_prefix = "SMTP_"

class EmailBaseConfigCls(ConfigCls):
    from_email: Optional[str] = None
    from_name: Optional[str] = None
    reset_token_expire_hrs: Optional[int] = 48
    templates_dir: Optional[PathStr] = './email-templates'
    enabled: Optional[bool] = False
    smtp_config: Optional[SMTPEmailBaseConfigCls] = Field(default_factory = SMTPEmailBaseConfigCls)

    class Config:
        env_prefix = "EMAILS_"


class AppBaseConfigCls(ConfigCls):
    title: Optional[str] = 'Lazy App'
    project: Optional[str] = 'Lazy Project'
    description: Optional[str] = 'Just a super secret app'
    version: Optional[str] = '0.0.1-alpha'
    domain: Optional[str] = None
    host: Optional[str] = '0.0.0.0'
    port: Optional[int] = 8080
    log_level: Optional[str] = 'info'
    config_dir: Optional[PathStr] = None

    author: Optional[str] = None
    maintainer: Optional[str] = None

    ### Auth Configs ###
    authuser: Optional[str] = None
    authpass: Optional[str] = None
    apikey: Optional[str] = None
    access_token: Optional[str] = None
    token: Optional[str] = None
    jwt_token: Optional[str] = None
    token_expire_mins: Optional[int] = 60 * 24 # 60 minutes * 24 hours

    authorized_users: Optional[ListStr] = []
    allowed_origins: Optional[ListStr] = []
    cors_origins: Optional[ListStr] = []

    open_registration: Optional[bool] = False

    ### Initialization ###
    first_superuser: Optional[str] = None
    first_superuser_role: Optional[str] = None
    first_superuser_pw: Optional[str] = None # Don't use this unless necessary
    first_superuser_pw_b64: Optional[Base64] = None
    first_superuser_pw_bgz: Optional[Base64Gzip] = None
    init_seed_data: Optional[Json] = {} # Json format
    init_seed_data_b64: Optional[JsonB64Str] = {} # Json in Base64 format
    init_seed_data_bgz: Optional[JsonB64GZipStr] = {} # Json in Base64 GZIP format    

    class Config:
        env_prefix = "APP_"

_path_prefix_pattern = re.compile(r'(/[\w.-]+)+')

try: from fastapi import FastAPI
except: FastAPI = object

class FastAPIBaseConfigCls(ConfigCls):
    root_path: Optional[str] = None
    api_prefix: Optional[str] = '/api/v1'
    xapi_prefix: Optional[str] = '/xapi'
    graphql_prefix: Optional[str] = '/graphql'
    openapi_prefix: Optional[str] = ''
    openapi_url: Optional[str] = '/openapi.json'
    docs_url: Optional[str] = '/docs'
    redoc_url: Optional[str] = '/redoc'
    version: Optional[str] = '0.0.1'
    root_path_in_servers: Optional[bool] = False
    allow_credentials: Optional[bool] = True
    allowed_hosts: Optional[ListStr] = []
    allowed_origins: Optional[ListStr] = ["*"]
    allowed_methods: Optional[ListStr] = ["*"]
    allowed_headers: Optional[ListStr] = ["*"]

    app_config: Optional[AppBaseConfigCls] = Field(default_factory = AppBaseConfigCls)

    class Config:
        env_prefix = "FASTAPI_"
    
    @property
    def app_prefix(self) -> str:
        """
        root_path takes precendence over api_prefix
        """
        if self.root_path: return self.root_path
        return self.api_prefix

    @property
    def openapi_path(self):
        """ 
        Prefixes openapi_url with openapi_prefix/app_prefix 
        with openapi_prefix taking precedence
        """
        p: str = (self.openapi_prefix or self.app_prefix) + self.openapi_url
        ## Fix any double slashes when combining path
        p = p.replace('//', '/')
        return p
    
    def get_openapi_prefix(self):
        """ 
        since openapi_url should either be a fixed path, or have prefix
        if openapi_prefix is present, this should be None
        """
        if self.openapi_prefix in self.openapi_path: return ''
        return self.openapi_prefix

    def get_fastapi_config(self, app_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Returns Dict for FastAPI(**config)
        """
        if app_config: self.app_config.update_config(**app_config) # self.app_config = self.app_config.update_config(**app_config)
        return {
            'title': self.app_config.title,
            'version': self.app_config.version or self.version,
            'root_path': self.app_prefix,
            'openapi_prefix': self.get_openapi_prefix(),
            'openapi_url': self.openapi_path,
            'root_path_in_servers': self.root_path_in_servers,
        }
    
    def get_fastapi_app(self, app_config: Dict[str, Any] = None, **kwargs) -> 'FastAPI':
        """
        Initializes a new FastAPI App using Config Settings

        returns fastapi.FastAPI
        """
        from lazy import Lib
        fastapi = Lib.import_lib('fastapi')
        fastapi_cls = getattr(fastapi, 'FastAPI')
        config = self.get_fastapi_config(app_config)
        if kwargs: config.update(kwargs)
        return fastapi_cls(**config)
    
    def update_fastapi_middleware(self, app: 'FastAPI', auth_config: Dict[str, Any] = None):
        """
        Updates an existing FastAPI instance with middlewares
        Assumes fastapi is installed since it would be required to pass the app

        returns the same app
        """
        if auth_config: self.update_config(**auth_config) #self = self.update_config(**auth_config)
        from starlette.middleware.cors import CORSMiddleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=self.allowed_origins,
            allow_credentials=self.allow_credentials,
            allow_methods=self.allowed_methods,
            allow_headers=self.allowed_headers,
        )
        if self.allowed_hosts:
            from fastapi.middleware.trustedhost import TrustedHostMiddleware
            app.add_middleware(TrustedHostMiddleware, allowed_hosts=self.allowed_hosts)
        return app



class FuzeConfigz(ConfigCls):
    cache_dir: PathStr = '~/.cachez'

    



