from lazy.types import *
from lazy.models.base import BaseCls, Field, OrJson, to_camelcase
from lazy.serialize import Serializer
from lazy.serialize._pysimd import SimdArray, SimdObject
from lazy.utils.wrapz.utils import timed_cache


from typing import overload
from .base_imports import *
from .utils import convert_to_cls
from .timez import get_timestamp_utc

if _httpx_available:
    from httpx import Response as HttpReponse
    from httpx import Request as HttpRequest
else:
    HttpReponse, HttpRequest = object, object

if _starlette_available:
    from starlette.requests import Request as _Request
else:
    _Request = object

RequestType = TypeVar('RequestType', Dict[Any, Any], HttpRequest, _Request)
JsonResponse = TypeVar('JsonResponse', HttpReponse, Union[None, int], None, Union[HttpReponse, int], Union[Dict[Any, Any], int], Dict[Any, Any], Dict[str, Any], SimdObject)
TextResponse = TypeVar('TextResponse', HttpReponse, Union[None, int], None, Union[HttpReponse, int], Union[str, int], str)
ContentResponse = TypeVar('ContentResponse', HttpReponse, Union[None, int], None, Union[HttpReponse, int], Union[bytes, int], Union[str, int], bytes, str)
AnyResponse = TypeVar('AnyResponse', HttpReponse, Union[None, int], None, Union[HttpReponse, int], Union[Any, int], Any, bytes, str, SimdObject, SimdArray)
BoolResponse = TypeVar('BoolResponse', HttpReponse, Union[bool, int], bool, Union[HttpReponse, int], Union[bool, int], bool)

ContentType = TypeVar('ContentType', str, bytes)
DataType = TypeVar('DataType', Dict[str, Any], Dict[Any, Any], List[Any], str)
DataObjType = TypeVar('DataObjType', SimdArray, SimdObject, Dict[str, Any], Dict[Any, Any], List[Any])


class Response(BaseCls):
    resp: HttpReponse
    client_type: str = 'sync'
    method: str = 'get'
    timestamp: str = Field(default_factory=get_timestamp_utc)

    class Config:
        arbitrary_types_allowed = True
        extra = 'allow'
        alias_generator = to_camelcase
        json_loads = OrJson.loads
        json_dumps = OrJson.dumps
    
    @overload
    def __getattr__(self, name, default = None):
        if not hasattr(self, name, None):
            return getattr(self.resp, name, default)
        return getattr(self, name, default)


    @property
    def status_code(self): return self.resp.status_code
    @property
    def text(self) -> str: return self.resp.text
    @property
    def content(self) -> ContentType: return self.resp.content
    @property
    @timed_cache(10)
    def data(self) -> DataType: return self.resp.json()
    @property
    def data_obj(self) -> DataObjType: return Serializer.SimdJson.parse(self.resp)
    @property
    def url(self): return self.resp.url
    @property
    def is_async(self): return bool(self.client_type == 'async')
    @property
    def is_sync(self): return bool(self.client_type == 'sync')
    @property
    def is_error(self): return bool(self.status_code >= 400)
    @property
    def is_success(self): return bool(self.status_code < 300)
    @property
    def is_redirect(self): return bool(self.status_code in range(300, 399))
    @property
    def status(self): return self.resp.status_code
    
    @property
    @timed_cache(10)
    def _valid_data(self):
        try: return self.data
        except: return None

    @property
    def data_cls(self) -> Type[BaseCls]:
        """Attempts to turn the response (if valid json/dict) to lazycls object. Returns None if not valid"""
        if not self._valid_data: return None
        return convert_to_cls(resp=self._valid_data)
    
    @property
    def data_yaml(self) -> Optional[Union[List[Any], Dict[Any, Any]]]:
        """ Attempts to transform into valid YAML """
        return Serializer.Yaml.loads(self.text)

    @property
    def data_b64(self) -> str:
        """ Attempts to Serialize the Data into Base64(str)"""
        return Serializer.Base.b64_encode(self.text)

    @property
    def data_bgz(self) -> str:
        """ Attempts to Serialize the Data into Base64(Gzip(str))"""
        return Serializer.Base.b64_gzip_encode(self.text)
    
    @property
    def data_pickle(self) -> bytes:
        """ Attempts to Serialize the Data into Picke(content)"""
        return Serializer.Pkl.dumps(self.content)
    
    @property
    def data_b64_decode(self) -> str:
        """ Attempts to Deserialize the Data from Base64(bytes)"""
        return Serializer.Base.b64_decode(self.content)

    @property
    def data_bgz_decode(self) -> str:
        """ Attempts to Deserialize the Data from Base64(Gzip(str))"""
        return Serializer.Base.b64_gzip_decode(self.content)
    
    @property
    def data_picke_decode(self) -> Any:
        """ Attempts to Deserialize the Data from Picke(content)"""
        return Serializer.Pkl.loads(self.content)
    



__all__ = (
    'Response',
    'HttpResponse',
    'HttpRequest',
    'RequestType',
    'JsonResponse',
    'TextResponse',
    'ContentResponse',
    'AnyResponse',
    'BoolResponse',
    'HttpResponse',
    'ContentType',
    'DataType',
    'DataObjType'
)

    



