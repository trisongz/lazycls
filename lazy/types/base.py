"""
Base Types from Typing
"""

__all__ = (
    'PathLike',
    'TYPE_CHECKING',
    'List', 'Dict', 'AnyStr', 'Any',
    'Optional', 'Union', 'Tuple', 'Mapping', 'Sequence', 'TypeVar', 'Type',
    'Callable', 'Coroutine', 'Generator', 'IO', 'Iterable', 'Iterator', 'AsyncIterator',
    'cast', 'overload',
    'Final',
    'Data', 'AnyMany', 'TextMany', 'TextList',
    'DictList', 'DictMany', 'DictAny', 'DictAny',
)

import sys

from os import PathLike
from typing import TYPE_CHECKING
from typing import List, Dict, AnyStr, Any
from typing import Optional, Union, Tuple, Mapping, Sequence, TypeVar, Type
from typing import Callable, Coroutine, Generator, IO, Iterable, Iterator, AsyncIterator
from typing import cast, overload

if sys.version_info >= (3, 8):
    from typing import Final
else:
    from typing_extensions import Final

Data = TypeVar('Data', str, List[str], Dict[str, Union[str, List[str]]])
AnyMany = TypeVar('AnyMany', Any, List[Any])

TextMany = TypeVar('TextMany', str, List[str])
TextList = List[str]

DictList = List[Dict[str, Any]]
DictMany = TypeVar('DictMany', Dict[str, Any], List[Dict[str, Any]])
DictAny = Dict[str, Any]
DictText = Dict[str, str]
