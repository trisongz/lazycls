from __future__ import annotations

import simdjson as _simdjson
from typing import Dict, Union, Any, List, Optional, Type, Iterable, overload, Callable, Iterable, Iterator, TYPE_CHECKING
from typing_extensions import SupportsIndex
if TYPE_CHECKING:
    from _typeshed import SupportsLessThanT, SupportsLessThan

""" 
Extends the Object/Array C-types from pysimdjson
Makes them more python friendly, while keeping their performance benefits
"""

class SimdArray(object):
    def __init__(self, data: Union[_simdjson.Array, 'SimdArray', List[Any]]):
        self._obj = data
        self._data = []
    
    @property
    def data(self):
        if not self._data: 
            if not isinstance(self._obj, (_simdjson.Array, type(self))): 
                print(type(self._obj))
                return self._obj
            self._data = self._obj.as_list()
        return self._data

    def clear(self) -> None:
        self.data.clear()

    def copy(self) -> Type[List]:
        return self.data.copy()

    def append(self, __object: Any) -> None:
        self.data.append(__object)
    
    def extend(self, __iterable: Iterable[Any]) -> None:
        self.data.extend(__iterable)
    
    def pop(self, __index: SupportsIndex = ...) -> Any: 
        return self.data.pop(__index)

    def index(self, __value: Any, __start: SupportsIndex = ..., __stop: SupportsIndex = ...) -> int:
        return self.data.index(__value, __start, __stop)

    def count(self, __value: Any) -> int:
        return self.data.count(__value)

    def insert(self, __index: SupportsIndex, __object: Any) -> None:
        self.data.insert(__index, __object)

    def remove(self, __value: Any) -> None: 
        self.data.remove(__value)

    def reverse(self) -> None:
        self.data.reverse()

    @overload
    def sort(self, *, key: Callable[[Any], SupportsLessThan], reverse: bool = ...) -> None:
        self.data.sort(key, reverse)
    
    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[Any]:
        return self.data.__iter__

    def __str__(self) -> str:
        return self.data.__str__

    @overload
    def __getitem__(self, __i: SupportsIndex) -> Any:
        return self.data.__getitem__(__i)

    @overload
    def __getitem__(self, __s: slice) -> list[Any]:
        return self.data.__getitem__(__s)

    @overload
    def __setitem__(self, __i: SupportsIndex, __o: Any) -> None:
        self.data.__setitem__(__i, __o)

    @overload
    def __setitem__(self, __s: slice, __o: Iterable[Any]) -> None:
        self.data.__setitem__(__s, __o)

    def __delitem__(self, __i: Union[SupportsIndex, slice]) -> None:
        self.data.__delitem__(__i)

    def __add__(self, __x: list[Any]) -> list[Any]:
        return self.data.__add__(__x)

    def __iadd__(self, __x: Iterable[Any]) -> Any:
        return self.data.__iadd__(__x)

    def __mul__(self, __n: SupportsIndex) -> list[Any]:
        return self.data.__mul__(__n)

    def __rmul__(self, __n: SupportsIndex) -> list[Any]:
        return self.data.__rmul__(__n)

    def __imul__(self, __n: SupportsIndex) -> Any:
        return self.data.__imul__(__n)

    def __contains__(self, __o: object) -> bool:
        return self.data.__contains__(__o)

    def __reversed__(self) -> Iterator[Any]:
        return self.data.__reversed__()

    def __gt__(self, __x: list[Any]) -> bool:
        return self.data.__gt__(__x)

    def __ge__(self, __x: list[Any]) -> bool:
        return self.data.__ge__(__x)

    def __lt__(self, __x: list[Any]) -> bool:
        return self.data.__lt__(__x)

    def __le__(self, __x: list[Any]) -> bool:
        return self.data.__le__(__x)


class SimdObject(object):
    def __init__(self, data: Union[_simdjson.Object, 'SimdObject', Dict[Any, Any]]):
        self._obj = data
        self._data = {}

    @property
    def data(self) -> Dict[Any, Any]:
        if not self._data: 
            if not isinstance(self._obj, (_simdjson.Object, type(self))): 
                print(type(self._obj))
                return self._obj
            self._data = self._obj.as_dict()
        return self._data

    def dict(self) -> Dict[Any, Any]:
        return self.data

    @property
    def keys(self):
        return list(self.data.keys())

    @property
    def values(self):
        return list(self.data.values())
    
    @property
    def items(self):
        return dict(self.data.items())
    
    @property
    def json(self):
        return _simdjson.dumps(self.data)

    def update(self, **kwargs):
        self.data.update(**kwargs)

    def get(self, key: str, default: Any = None):
        if self._data.get(key): return self._data[key]
        return type(self)(self._obj.get(key, default))
    
    def pop(self, key: str, default: Any = None):
        return self.data.pop(key, default)
    
    def popitem(self): 
        return self.data.popitem()

    def copy(self):
        return self.data.copy()

    def __iter__(self):
        return self.data.__iter__

    def __delitem__(self, key: str):
        self.data.__delitem__(key)

    def __setitem__(self, key: str, value: Any = None):
        self.data.__setitem__(key, value)

    def __getitem__(self, key: str, default: Any = None) -> Any:
        if self._data.get(key): return self._data[key]
        d = type(self)(self._obj.get(key, default))
        return d.data

    def __len__(self):
        return self.data.__len__
    
    def __str__(self):
        return self.json

def create_simdobj(data: Union[_simdjson.Object, _simdjson.Array]):
    if isinstance(data, _simdjson.Object): return SimdObject(data)
    return SimdArray(data)