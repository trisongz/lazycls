"""
Cloud Path extensions if libs are available
"""

from lazycls.ext._imports import LazyLib
from lazycls.ext.pathio.base import Path
from lazycls.ext.pathio.base import PathLike

if LazyLib.is_avail_smartopen:
    pass

if LazyLib.is_avail_tensorflow:
    pass

