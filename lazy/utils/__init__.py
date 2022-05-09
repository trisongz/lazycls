from . import helpers
from . import procs

from . import _async

from .helpers import get_logger, get_cls_logger, timer, is_coro_func
from . import wrapz

from .runtime import ProtectedRuntime
from .executor import Executor

logger = get_logger('lazy')
_get_logger = get_cls_logger('lazy')