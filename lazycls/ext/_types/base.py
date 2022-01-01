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
    'Final'
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

