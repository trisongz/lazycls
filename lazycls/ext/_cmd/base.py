import io
import inspect
import reprlib
import pathlib

import types

from copy import copy
from glob import glob
from lazycls.prop import classproperty
from lazycls.serializers import OrJson
from subprocess import PIPE, Popen, TimeoutExpired
from lazycls.ext.pathio import Path, PathLike

