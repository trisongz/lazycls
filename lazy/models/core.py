
__all__ = (
    'LazyCls',
    'create_lazycls',
    'clear_lazy_models',
    'set_modulename'
)

from pydantic import create_model
from typing import Dict, Any, Type
from lazy.types import classproperty
from .base import BaseLazy, BaseModel, BaseCls

_GeneratedLazyModels = {}
_BaseModuleName = 'lazycls'

def clear_lazy_models():
    """
    Function to Clear out any Generated Lazy Models
    """
    global _GeneratedLazyModels
    _GeneratedLazyModels = {}

def set_modulename(name: str = 'lazycls'):
    """
    Function to Set the Module Name. Useful when using this within other projects.
    """
    global _BaseModuleName
    _BaseModuleName = name


def generate_modelcls(_clsname: str, data: Dict[str, Any], modulename: str = _BaseModuleName, basecls: Type[BaseModel] = BaseLazy):
    """
    Primary Function to Generate the Model Class Dynamically
    """
    global _GeneratedLazyModels
    if _clsname not in _GeneratedLazyModels:
        clsdata = {k: type(v) for k,v in data.items()}
        _GeneratedLazyModels[_clsname] = create_model(_clsname, __base__ = basecls, __module__ = modulename, **clsdata)
    return _GeneratedLazyModels[_clsname](**data)

def create_lazycls(clsname: str, data: Dict[str, Any], modulename: str = _BaseModuleName, basecls: Type[BaseModel] = BaseLazy) -> Type[BaseCls]:
    """
    Iterator Function to take data and recurisvely turn them into LazyCls Models
    """
    if clsname.endswith('s'): clsname = clsname[:-1]
    if isinstance(data, str): return data
    for k,v in data.items():
        subcls = f'{clsname}_{k}'
        if v and isinstance(v, list):
            data[k] = [generate_modelcls(_clsname=subcls, data=i, modulename=modulename, basecls=basecls) if isinstance(i, dict) else i  for i in v]
        elif isinstance(v, dict):
            for a,b in v.items():
                if isinstance(b, dict):
                    subsubcls = f'{clsname}_{k}_{a}'
                    v[a] = generate_modelcls(_clsname=subsubcls, data=b, modulename=modulename, basecls=basecls)
            data[k] = generate_modelcls(_clsname=subcls, data=v, modulename=modulename, basecls=basecls)
    return generate_modelcls(_clsname=clsname, data=data, modulename=modulename, basecls=basecls)


class LazyCls:

    @classmethod
    def create(cls, *args, **kwargs):
        return create_lazycls(*args, **kwargs)
    
    @classmethod
    def get(cls, name):
        return _GeneratedLazyModels.get(name)
    
    @classproperty
    def models(cls):
        return _GeneratedLazyModels

    @classproperty
    def modelNames(cls):
        return list(cls.models.keys())

    @classproperty
    def __len__(cls):
        return len(_GeneratedLazyModels)

    @classmethod
    def __getitem__(cls, name):
        return _GeneratedLazyModels.get(name)
    
    @classmethod
    def __call__(cls, name, data: Dict[str, Any], *args, **kwargs):
        return create_lazycls(clsname=name, data=data, *args, **kwargs)

