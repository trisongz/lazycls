from typing import List, Dict, Optional, Callable, TypeVar, Any, Union, Coroutine, Generator, Type, Tuple

Data = TypeVar('Data', str, List[str], Dict[str, Union[str, List[str]]])
AnyMany = TypeVar('AnyMany', Any, List[Any])

TextMany = TypeVar('TextMany', str, List[str])
TextList = List[str]

DictList = List[Dict[str, Any]]
DictMany = TypeVar('DictMany', Dict[str, Any], List[Dict[str, Any]])
DictAny = Dict[str, Any]
DictText = Dict[str, str]

DefaultHeaders = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}
