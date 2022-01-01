from .base import *
from ._json import JsonSrlzer, OrJson, SimdJson
from ._yaml import Yaml
from ._base import Base


class Srlz:
    Json: JsonSrlzer = OrJson
    Yaml: Yaml = Yaml
    Base: Base = Base
    
    
    