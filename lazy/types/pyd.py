
"""
Pydantic Types
"""

__all__ = (
    'NoneStr',
    'NoneBytes',
    'StrBytes',
    'NoneStrBytes',
    'StrictStr',
    'ConstrainedBytes',
    'conbytes',
    'ConstrainedList',
    'conlist',
    'ConstrainedSet',
    'conset',
    'ConstrainedStr',
    'constr',
    'PyObject',
    'ConstrainedInt',
    'conint',
    'PositiveInt',
    'NegativeInt',
    'NonNegativeInt',
    'NonPositiveInt',
    'ConstrainedFloat',
    'confloat',
    'PositiveFloat',
    'NegativeFloat',
    'NonNegativeFloat',
    'NonPositiveFloat',
    'ConstrainedDecimal',
    'condecimal',
    'UUID1',
    'UUID3',
    'UUID4',
    'UUID5',
    'FilePath',
    'DirectoryPath',
    'Json',
    'JsonWrapper',
    'Yaml',
    'YamlWrapper',
    'JsonOrYaml',
    'SecretStr',
    'SecretBytes',
    'StrictBool',
    'StrictBytes',
    'StrictInt',
    'StrictFloat',
    'PaymentCardNumber',
    'ByteSize'
)
#import yaml
from pydantic.types import *
from pydantic.types import _registered
from typing import TYPE_CHECKING, Type, Any, Dict, Union

class YamlWrapper:
    pass

class YamlMeta(type):
    def __getitem__(self, t: Type[Any]) -> Type[YamlWrapper]:
        return _registered(type('YamlWrapperValue', (YamlWrapper,), {'inner_type': t}))

#if TYPE_CHECKING:
#    Yaml = str
#else:

class Yaml(metaclass=YamlMeta):
    @classmethod
    def validate(cls, value: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(value, (dict, list)): return value
        if value and isinstance(value, str):
            import yaml
            value = yaml.load(value, Loader=yaml.SafeLoader)
        return value
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> None:
        field_schema.update(type='string', format='yaml-string')


class JsonWrapper:
    pass

class JsonMeta(type):
    def __getitem__(self, t: Type[Any]) -> Type[JsonWrapper]:
        return _registered(type('JsonWrapperValue', (JsonWrapper,), {'inner_type': t}))

#if TYPE_CHECKING:
#    Yaml = str
#else:

class Json(metaclass=JsonMeta):
    @classmethod
    def validate(cls, value: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(value, (dict, list)): return value
        if value and isinstance(value, str):
            import json
            value = json.loads(value)
        return value
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> None:
        field_schema.update(type='string', format='json-string')


class JsonOrYaml(metaclass=JsonMeta):
    @classmethod
    def validate(cls, value: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        # sourcery skip: raise-specific-error
        if isinstance(value, (dict, list)): return value
        if value and isinstance(value, str):
            try:
                import json
                value = json.loads(value)
            except:
                try:
                    import yaml
                    value = yaml.load(value, Loader=yaml.SafeLoader)
                except Exception as e:
                    raise Exception from e
        return value
    
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> None:
        field_schema.update(type='string', format='json-string')