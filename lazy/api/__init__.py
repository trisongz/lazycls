"""
Consolidating lazyapi into this.
"""
from . import types

from .config import (
    AppConfigz, FastAPIConfigz,
    TimeZoneConfigz, 
    HttpConfigz, AsyncHttpConfigz,
    PostgresConfigz, RedisConfigz, ElasticsearchConfigz, MysqlConfigz
)
from .types import *
from .client import *
from .fast import *
from .backends import RedisBackend