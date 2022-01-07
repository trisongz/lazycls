from . import types
from . import base
from . import core
from . import cloud
from . import generic

from .generic import as_path, get_path, PathLike, register_pathlike_cls, get_userhome, get_lazydir

to_path = get_path

from .core import PosixFSxPath, WindowsFSxPath
from .cloud import PosixGCSPath, PosixS3Path, WindowsS3Path, WindowsGCSPath

import pathlib
PathzPath = (PosixFSxPath, WindowsFSxPath, PosixGCSPath, PosixS3Path, WindowsS3Path, WindowsGCSPath, pathlib.Path, pathlib.PosixPath, pathlib.PurePath, str)