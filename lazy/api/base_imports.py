
from types import ModuleType
from typing import TYPE_CHECKING

try: 
    import fastapi
    from fastapi import FastAPI
    _fastapi_available = True

except ImportError: 
    fastapi: ModuleType = None
    FastAPI: object = None
    _fastapi_available = False

try: 
    import dateparser
    _dateparser_available = True
except ImportError: 
    dateparser: ModuleType = None
    _dateparser_available = False

try: 
    import pytz
    _pytz_available = True
except ImportError: 
    pytz: ModuleType = None
    _pytz_available = False

try: 
    import starlette
    from starlette.requests import Request as _Request
    _starlette_available = True
except ImportError: 
    starlette: ModuleType = None
    _Request: object = None
    _starlette_available = False

try: 
    import httpx
    from httpx import Response as HttpResponse
    from httpx import Request as HttpRequest
    _httpx_available = True
except ImportError: 
    httpx: ModuleType = None
    HttpRequest: object = None
    HttpResponse: object = None
    _httpx_available = False

from lazy.libz import Lib

_LAZYAPI_CHECKED = False


def _ensure_api_reqs():
    """
    Checks to ensure that all 3rd party libs required by this library
    are met. 
    """
    global _LAZYAPI_CHECKED
    if _LAZYAPI_CHECKED: return
    global fastapi, starlette, httpx, dateparser, pytz
    global _Request, HttpRequest, HttpResponse, FastAPI
    
    if fastapi is None:
        fastapi = Lib.import_lib('fastapi', 'fastapi[all]')
        Lib.reload_module(fastapi)
        FastAPI = fastapi.FastAPI
    if starlette is None:
        starlette = Lib.import_lib('starlette')
        Lib.reload_module(starlette)
        _Request = starlette.requests.Request
    if httpx is None:
        httpx = Lib.import_lib('httpx')
        Lib.reload_module(httpx)
        HttpResponse = httpx.Response
        HttpRequest = httpx.Request
    if dateparser is None:
        dateparser = Lib.import_lib('httpx')
        Lib.reload_module(dateparser)
    if pytz is None:
        pytz = Lib.import_lib('pytz')
        Lib.reload_module(pytz)
    _LAZYAPI_CHECKED = True

__all__ = (
    'fastapi',
    'starlette',
    'httpx',
    'pytz',
    'dateparser',
    '_ensure_api_reqs',
    '_Request', 
    'HttpRequest', 
    'HttpResponse',
    'TYPE_CHECKING',
    'Lib',
    '_starlette_available',
    '_fastapi_available',
    '_httpx_available',
    '_pytz_available',
    '_dateparser_available',
)

    