from __future__ import annotations

from lazy.types import *
from .base_imports import *
from .config import FastAPIConfigz, AppConfigz

if _fastapi_available:
    from fastapi import Header, Depends, Body, FastAPI, APIRouter, HTTPException, BackgroundTasks, status
    from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse, FileResponse, RedirectResponse, ORJSONResponse, StreamingResponse
    from fastapi import WebSocket, WebSocketDisconnect
else:
    Header, Depends, Body, FastAPI, APIRouter, HTTPException, BackgroundTasks, status = Callable, Callable, Callable, object, object, object, object, object
    JSONResponse, PlainTextResponse, HTMLResponse, FileResponse, RedirectResponse, ORJSONResponse, StreamingResponse = object, object, object, object, object, object, object
    WebSocket, WebSocketDisconnect = object, object

if _starlette_available:
    from starlette.requests import Request
else: Request = object


if TYPE_CHECKING:
    from fastapi import FastAPI, Header, Depends, Body, HTTPException, status, BackgroundTasks
    from fastapi.responses import JSONResponse, PlainTextResponse
    from starlette.requests import Request

def create_fastapi(
        app_name: str = None, 
        title: str = AppConfigz.title, 
        description: str = AppConfigz.description, 
        version: str = AppConfigz.version, 
        include_middleware: bool = FastAPIConfigz.include_middleware, 
        allow_credentials: bool = FastAPIConfigz.allow_credentials, 
        allow_origins: List[str] = FastAPIConfigz.allow_origins, 
        allow_methods: List[str] = FastAPIConfigz.allow_methods, 
        allow_hosts: List[str] = FastAPIConfigz.allow_hosts, 
        allow_headers: List[str] = FastAPIConfigz.allow_headers, 
        auth_config: Dict[str, Any] = None,
        logger: Optional[Any] = None,
        **kwargs
    ) -> Type['FastAPI']:
    """
    This method will dynamically create a new FastAPI App instance using 
    the base Environment Variables in FastAPIConfigz

    In certain use cases (such as submounts), you can leave the app_name blank for the primary app,
    and define the app_name for all subapps.
    """
    if app_name: title += ': ' + app_name
    app_config = AppConfigz()
    app_config.update_config(title = title, description = description, version = version)
    fast_config = FastAPIConfigz()
    fast_config.update_config(include_middleware = include_middleware, allow_credentials = allow_credentials, allow_origins = allow_origins, allow_methods = allow_methods, allow_hosts = allow_hosts, allow_headers = allow_headers)
    new_fastapi_app = fast_config.get_fastapi_app(app_config = app_config, **kwargs)
    if include_middleware: fast_config.update_fastapi_middleware(new_fastapi_app, auth_config)
    if logger: new_fastapi_app.logger = logger
    return new_fastapi_app
    


class FastAPIValidator:
    header: 'Header' = None
    def __init__(self, key: str, alias: str = 'token'):
        self._key = key
        self._alias = alias
        self.header = Header(..., alias=self._alias)

def create_validator(key: str, alias: str = 'token') -> Type[Callable]:
    async def verify_token(token: str = Header(..., alias=alias)):
        if token == key: return True
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials", headers={"WWW-Authenticate": "Bearer"})
    return verify_token


def create_multi_validator(keys: List[str], alias: str = 'token') -> Type[Callable]:
    async def verify_token(token: str = Header(..., alias=alias)):
        if token in keys: return True
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials", headers={"WWW-Authenticate": "Bearer"})
    return verify_token


def create_func_multi_validator_body(func: Callable, keys: List[str], alias: str = 'user_id', data_key: str = None, **funcKwargs) -> Type[Callable]:
    datakey = data_key or alias
    async def verify_data(data: Dict[str, Any] = Body(..., alias=alias)):
        if data.get(datakey) and func(data[datakey], **funcKwargs) in keys: return True
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials", headers={"WWW-Authenticate": "Bearer"})
    return verify_data


def create_func_multi_validator_request(func: Callable, keys: List[str], alias: str = 'user_id', data_key: str = None, reqType: str = 'form', **funcKwargs) -> Type[Callable]:
    datakey = data_key or alias
    async def verify_data(req: 'Request' = Body(..., alias=alias)):
        reqMethod = getattr(req, reqType)
        req = await reqMethod()
        if req.get(datakey) and func(req[datakey], **funcKwargs) in keys: return True
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authentication credentials", headers={"WWW-Authenticate": "Bearer"})
    return verify_data


## Directly implemented from
## https://fastapi.tiangolo.com/advanced/websockets/

class WebsocketManager:
    def __init__(self):
        _ensure_api_reqs()
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


__all__ = [
    ## FastAPI Base Imports
    'Header', 
    'Depends', 
    'Body', 
    'FastAPI', 
    'APIRouter',
    'HTTPException', 
    'BackgroundTasks', 
    'status',
    ## Responses
    'JSONResponse', 
    'PlainTextResponse', 
    'HTMLResponse', 
    'FileResponse', 
    'RedirectResponse', 
    'ORJSONResponse', 
    'StreamingResponse',
    ## Starlette Request
    'Request',
    ## Websocket
    'WebSocket', 
    'WebSocketDisconnect',
    'WebsocketManager',
    ## Custom Classes / Funcs
    'create_validator',
    'create_multi_validator',
    'create_func_multi_validator_body',
    'create_func_multi_validator_request',
    'AppConfigz',
    'FastAPIConfigz',
    'create_fastapi',
]