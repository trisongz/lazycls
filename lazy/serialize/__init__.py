

from .core import *
from ._json import JsonBase, OrJson, SimdJson, Json, DefaultJson
from ._yaml import Yaml
from ._base import Base
from ._secrets import Secret
from ._pickle import BasePickle, Pickle, Dill, Pkl, Compression
from ._multi import YamlBase64, YamlBGZ, JsonBase64, JsonBGZ


class Serialize:
    Json: JsonBase = DefaultJson
    JsonB64: JsonBase64 = JsonBase64
    JsonBGZ: JsonBGZ = JsonBGZ
    Yaml: Yaml = Yaml
    YamlB64: YamlBase64 = YamlBase64
    YamlBGZ: YamlBGZ = YamlBGZ
    Base: Base = Base
    Secret: Secret = Secret
    Pkl: BasePickle = Dill
    Compress = Compression
    SimdJson: SimdJson = SimdJson
    OrJson: OrJson = OrJson
    DefaultJson: DefaultJson = DefaultJson
    


    
__all__ = (
    'OrJson', 'SimdJson', 'Json', 'DefaultJson',
    'Yaml',
    'Base',
    'Secret',
    'Pickle', 'Dill', 'Pkl',
    'Compression',
    'Serialize',
    'YamlBase64', 'YamlBGZ', 'JsonBase64', 'JsonBGZ'
)