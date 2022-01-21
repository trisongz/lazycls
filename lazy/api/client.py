from __future__ import annotations

from lazy.types import *
from lazy.models import BaseCls

from .config import *
from .types import *
from .utils import convert_to_cls
from .base_imports import _httpx_available, _ensure_api_reqs

if _httpx_available:
    from httpx import Client as _Client
    from httpx import AsyncClient as _AsyncClient
    from httpx import Response as HttpResponse
else:
    _Client, _AsyncClient, HttpResponse = object, object, object


class Client:
    _web: _Client = None
    _async: _AsyncClient = None 
    
    @classmethod
    def create_client(cls, base_url: str = "", config: Dict[str, Any] = None, **kwargs) -> Type[_Client]:
        """Creates a Sync httpx Client"""
        _ensure_api_reqs()
        configz = HttpConfigz()
        if config: configz.update_config(**config)
        client_config = configz.httpx_config
        if 'headers' in kwargs:
            headers = kwargs.pop('headers')
            if headers: client_config['headers'] = headers
        return _Client(base_url = base_url, **client_config, **kwargs)
    
    @classmethod
    def create_async_client(cls, base_url: str = "", config: Dict[str, Any] = None, **kwargs) -> Type[_AsyncClient]:
        """ Creates an async httpx Client"""
        _ensure_api_reqs()
        configz = AsyncHttpConfigz()
        if config: configz.update_config(**config)
        client_config = configz.httpx_config
        if 'headers' in kwargs:
            headers = kwargs.pop('headers')
            if headers: client_config['headers'] = headers
        return _AsyncClient(base_url = base_url, **client_config, **kwargs)

    @classproperty
    def client(cls) -> Type[_Client]:
        if not cls._web: cls._web = cls.create_client()
        return cls._web
    
    @classproperty
    def async_client(cls) -> Type[_AsyncClient]:
        if not cls._async: cls._async = cls.create_async_client()
        return cls._async



class ApiClient:
    def __init__(self, base_url: str = HttpConfigz.base_url or AsyncHttpConfigz.base_url, headers: DictAny = {}, config: DictAny = None, async_config: DictAny = None, module_name: str = HttpConfigz.module_name or AsyncHttpConfigz.module_name, default_resp: bool = False, **kwargs):
        _ensure_api_reqs()
        self.base_url = ""
        self.headers = {}
        self.config = None
        self.async_config = None
        self._module_name = None
        self._kwargs = {}
        self._web = None
        self._async = None
        self._default_mode = False
        self.set_configs(base_url = base_url, headers = headers, config = config, async_config = async_config, module_name = module_name, default_resp = default_resp, **kwargs)

    def set_configs(self, base_url: str = HttpConfigz.base_url or AsyncHttpConfigz.base_url, headers: DictAny = {}, config: DictAny = None, async_config: DictAny = None, module_name: str = HttpConfigz.module_name or AsyncHttpConfigz.module_name, default_resp: bool = False,  **kwargs):
        self.base_url = base_url or self.base_url
        self.headers = headers or self.headers
        self.config = config or self.config
        self.async_config = async_config or self.async_config
        self._module_name = module_name or self._module_name
        self._default_mode = default_resp or self._default_mode
        self._kwargs = kwargs or self._kwargs

    def reset_clients(self, base_url: str = HttpConfigz.base_url or AsyncHttpConfigz.base_url, headers: DictAny = {}, config: DictAny = None, async_config: DictAny = None, module_name: str = HttpConfigz.module_name or AsyncHttpConfigz.module_name, default_resp: bool = False, **kwargs):
        self.set_configs(base_url = base_url, headers = headers, config = config, async_config = async_config, module_name = module_name, default_resp = default_resp, **kwargs)
        self._web = None
        self._async = None
    
    @property
    def client(self):
        if not self._web: self._web = Client.create_client(base_url=self.base_url, config=self.config, headers=self.headers, **self._kwargs)
        return self._web
    
    @property
    def aclient(self):
        if not self._async: self._async = Client.create_async_client(base_url=self.base_url, config=self.async_config, headers=self.headers, **self._kwargs)
        return self._async
    

    #############################################################################
    #                             Base REST APIs                                #
    #############################################################################
    
    def delete(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = self.client.delete(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'sync', method = 'delete')

    def get(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = self.client.get(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'sync', method = 'get')

    def head(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = self.client.head(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'sync', method = 'head')

    def patch(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = self.client.patch(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'sync', method = 'patch')

    def put(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = self.client.put(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'sync', method = 'put')
    
    def post(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = self.client.post(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'sync', method = 'post')

    #############################################################################
    #                          Async REST Methods                               #
    #############################################################################
    
    async def async_delete(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = await self.aclient.delete(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'async', method = 'delete')

    async def async_get(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = await self.aclient.get(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'async', method = 'get')
    
    async def async_head(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = await self.aclient.head(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'async', method = 'head')

    async def async_patch(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = await self.aclient.patch(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'async', method = 'patch')

    async def async_put(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = await self.aclient.put(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'async', method = 'put')
    
    async def async_post(self, path: str, **kwargs) -> Union[Response, HttpResponse]:
        resp = await self.aclient.post(url=path, **kwargs)
        if self._default_mode: return resp
        return Response(resp = resp, client_type = 'async', method = 'post')


    #############################################################################
    #                       Supplementary Helpful Callers                       #
    #############################################################################

    def ping(self, path: str, max_status_code: int = 300, min_status_code: int = None, **kwargs) -> bool:
        """ Returns a bool of whether response code is great/within range/less than an int
            Can be used as a health check """
        res = self.get(url=path, **kwargs)
        if min_status_code and max_status_code:
            return bool(res.status_code in range(min_status_code, max_status_code))
        if min_status_code:
            return bool(res.status_code > min_status_code)
        return bool(res.status_code < max_status_code)
    
    def get_data(self, path: str, key: str = 'data', **kwargs) -> DataType:
        """ Expects to get data in JSON. If does not get the key, returns None. """
        resp = self.get(url=path, **kwargs)
        return resp.data.get(key, None)
    
    def get_lazycls(self, path: str, key: str = 'data', **kwargs) -> Type[BaseCls]:
        """
        Expects to get data in JSON. If does not get the key, returns None.
        Returns the data from a GET request to Path as a LazyCls
        """
        data = self.get_data(path=path, key=key, **kwargs)
        if not data: return None
        return convert_to_cls(resp=data, module_name=self._module_name, base_key=key)


    #############################################################################
    #                  Async Supplementary Helpful Callers                      #
    #############################################################################

    async def async_ping(self, path: str, max_status_code: int = 300, min_status_code: int = None, **kwargs) -> bool:
        """ Returns a bool of whether response code is great/within range/less than an int
            Can be used as a health check """
        res = await self.async_get(url=path, **kwargs)
        if min_status_code and max_status_code:
            return bool(res.status_code in range(min_status_code, max_status_code))
        if min_status_code:
            return bool(res.status_code > min_status_code)
        return bool(res.status_code < max_status_code)
    
    async def async_get_data(self, path: str, key: str = 'data', **kwargs) -> DataType:
        """ Expects to get data in JSON. If does not get the key, returns None. """
        resp = await self.async_get(url=path, **kwargs)
        return resp.data.get(key, None)
    
    async def async_get_lazycls(self, path: str, key: str = 'data', **kwargs) -> Type[BaseCls]:
        """
        Expects to get data in JSON. If does not get the key, returns None.
        Returns the data from a GET request to Path as a LazyCls
        """
        data = await self.async_get_data(path=path, key=key, **kwargs)
        if not data: return None
        return convert_to_cls(resp=data, module_name=self._module_name, base_key=key)
    

        
APIClient = ApiClient

__all__ = [
    'Client',
    'HttpResponse',
    'ApiClient',
    'APIClient',
    '_Client',
    '_AsyncClient'
]
