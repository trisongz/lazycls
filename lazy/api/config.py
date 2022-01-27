from __future__ import annotations
import json
from lazy.types import *
from lazy.types.pyd import Json
from lazy.models import Field
from lazy.configz import ConfigCls
from lazy.configz.common import AppBaseConfigCls, PostgresConfigz, RedisConfigz, ElasticsearchConfigz, MysqlConfigz

from logz import get_cls_logger
from .base_imports import httpx, fastapi, FastAPI, _ensure_api_reqs, Lib
if TYPE_CHECKING:
    import httpx
    import fastapi
    from fastapi import FastAPI

get_logger = get_cls_logger('lazy:api')
logger = get_logger()

DefaultHeaders = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

class TimeZoneConfigz(ConfigCls):
    desired: str = 'America/Chicago'
    tz_format: str = '%Y-%m-%dT%H:%M:%SZ'

class HttpConfigz(ConfigCls):
    base_url: str = ""
    timeout: float = 30.0
    keep_alive: int = 50
    max_connect: int = 200
    default_headers: Json = json.dumps(DefaultHeaders)
    module_name: str = 'lazy'
    
    @property
    def httpx_timeout(self):
        return httpx.Timeout(self.timeout, connect=self.timeout)

    @property
    def httpx_limits(self):
        return httpx.Limits(max_keepalive_connections=self.keep_alive, max_connections=self.max_connect)
    
    @property
    def httpx_config(self):
        return {'timeout': self.httpx_timeout, 'limits': self.httpx_limits, 'headers': self.default_headers}


class AsyncHttpConfigz(HttpConfigz):
    pass

class AppConfigz(AppBaseConfigCls):
    pass

## Basically a copy paste from configz.common but more explicit

class FastAPIConfigz(ConfigCls):
    root_path: Optional[str] = None
    api_prefix: Optional[str] = ''
    xapi_prefix: Optional[str] = '/xapi'
    graphql_prefix: Optional[str] = '/graphql'
    openapi_prefix: Optional[str] = ''
    openapi_url: Optional[str] = '/openapi.json'
    docs_url: Optional[str] = '/docs'
    redoc_url: Optional[str] = '/redoc'
    version: Optional[str] = '0.0.1'
    root_path_in_servers: Optional[bool] = False
    include_middleware: Optional[bool] = True
    allow_credentials: Optional[bool] = True
    allow_hosts: Optional[List[str]] = []
    allow_origins: Optional[List[str]] = ["*"]
    allow_methods: Optional[List[str]] = ["*"]
    allow_headers: Optional[List[str]] = ["*"]
    app_configz: Optional[Json]

    class Config:
        env_prefix = "FASTAPI_"

    @property
    def app_config(self):
        if not self.app_configz:
            self.app_configz = AppConfigz()
        elif isinstance(self.app_configz, dict):
            configz = AppConfigz()
            configz.update_config(**self.app_configz)
            self.app_configz = configz
        return self.app_configz
    
    @property
    def app_prefix(self) -> str:
        """
        root_path takes precendence over api_prefix
        """
        return self.root_path or self.api_prefix

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

    def get_fastapi_config(self, app_config: Union[AppConfigz, Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Returns Dict for FastAPI(**config)
        """
        if app_config:
            if isinstance(app_config, type(AppConfigz)):
                self.app_configz = app_config
            else:
                self.app_config.update_config(**app_config) # self.app_config = self.app_config.update_config(**app_config)
        return {
            'title': self.app_config.title,
            'version': self.app_config.version or self.version,
            'root_path': self.app_prefix,
            'openapi_prefix': self.get_openapi_prefix(),
            'openapi_url': self.openapi_path,
            'root_path_in_servers': self.root_path_in_servers,
        }
            
    def get_fastapi_app(self, app_config: Union[AppConfigz, Dict[str, Any]] = None, **kwargs) -> 'FastAPI':
        """
        Initializes a new FastAPI App using Config Settings

        returns fastapi.FastAPI
        """
        _ensure_api_reqs()
        config = self.get_fastapi_config(app_config)
        if kwargs: config.update(kwargs)
        return FastAPI(**config)
    
    def update_fastapi_middleware(self, app: 'FastAPI', auth_config: Dict[str, Any] = None):
        """
        Updates an existing FastAPI instance with middlewares
        Assumes fastapi is installed since it would be required to pass the app

        returns the same app
        """
        _ensure_api_reqs()
        if auth_config: self.update_config(**auth_config) #self = self.update_config(**auth_config)
        from starlette.middleware.cors import CORSMiddleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=self.allow_origins,
            allow_credentials=self.allow_credentials,
            allow_methods=self.allow_methods,
            allow_headers=self.allow_headers,
        )
        if self.allow_hosts:
            from fastapi.middleware.trustedhost import TrustedHostMiddleware
            app.add_middleware(TrustedHostMiddleware, allowed_hosts=self.allow_hosts)
        return app


__all__ = [
    'logger',
    'DefaultHeaders',
    'TimeZoneConfigz',

    'HttpConfigz',
    'AsyncHttpConfigz',
    
    'AppConfigz',
    'FastAPIConfigz',

    'PostgresConfigz', 
    'RedisConfigz', 
    'ElasticsearchConfigz', 
    'MysqlConfigz'

]
