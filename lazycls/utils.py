from pathlib import Path
from typing import Union
import logging

logger = logging.getLogger(name='lazycls')


def getParentPath(p: str) -> Path: return Path(p).parent


def toPath(path: Union[str, Path], resolve: bool = True) -> Path:
    if isinstance(path, str): path = Path(path)
    if resolve: path.resolve()
    return path

def to_camelcase(string: str) -> str:
    return ''.join(word.capitalize() for word in string.split('_'))


get_parent_path = getParentPath
to_path = toPath