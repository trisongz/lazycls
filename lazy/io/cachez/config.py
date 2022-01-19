import dill

from lazy.types import *
from lazy.types.pyd import ByteSize
from lazy.models import BaseCls
from lazy.configz import ConfigCls


# https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
# https://blog.devart.com/increasing-sqlite-performance.html
# page_size: 4096 seems to be the fastest-ish

"""
You can reload the settings manually at runtime

from lazy.io.cachez import CachezConfigz

CachezConfigz.update_config(sqlmode = 'perf', serializer = 'pickle')
print(CachezConfigz.dict())

"""

class SqlConfig(BaseCls):
    mode: str = ''
    settings: Dict[str, Any] = {}
    policies: Dict[str, Any] = {}

class CachezConfigz(ConfigCls):
    dbname: str = 'cache.db'
    sqlmode: str = 'default' # default / standard / perf / optimized
    default_table: str = 'Cache'
    serializer: str = 'dill' # dill / pickle

    default_compression_level: int = 3
    standard_compression_level: int = 5
    optim_compression_level: int = 7
    perf_compression_level: int = 0 # Zero compression

    default_size_limit: ByteSize = '1 GiB'
    standard_size_limit: ByteSize = '5 GiB'
    optim_size_limit: ByteSize = '10 GiB'
    perf_size_limit: ByteSize = '20 GiB'

    default_disk_min_file_size: ByteSize = '32 KiB'
    standard_disk_min_file_size: ByteSize = '64 KiB'
    optim_disk_min_file_size: ByteSize = '128 KiB'
    perf_disk_min_file_size: ByteSize = '256 KiB'
    
    default_sqlite_mmap_size: ByteSize = '512 MiB'
    standard_sqlite_mmap_size: ByteSize = '1024 MiB'
    optim_sqlite_mmap_size: ByteSize = '1024 MiB'
    perf_sqlite_mmap_size: ByteSize = '2 GiB'
    
    default_sqlite_cache_size: int = '4096' # '8192' # 8,192 pages
    standard_sqlite_cache_size: int = '4096' # '16384' # 16,384 pages
    optim_sqlite_cache_size: int = '4096' # '8192' # 8,192 pages
    perf_sqlite_cache_size: int = '4096' # '32768' # 32,768 pages


    def get_default_sql_settings(self):
        return {
            u'statistics': 0,  # False
            u'tag_index': 0,  # False
            u'eviction_policy': u'least-recently-stored',
            u'size_limit': self.default_size_limit,
            u'cull_limit': 10,
            u'sqlite_auto_vacuum': 1,  # FULL
            u'sqlite_cache_size': self.default_sqlite_cache_size,
            u'sqlite_journal_mode': u'wal',
            u'sqlite_mmap_size': self.default_sqlite_mmap_size,
            u'sqlite_synchronous': 1,  # NORMAL
            u'disk_min_file_size': self.default_disk_min_file_size,  # 32kb
            u'disk_pickle_protocol': dill.DEFAULT_PROTOCOL,
        }
    
    def get_standard_sql_settings(self):
        return {
            u'statistics': 0,  # False
            u'tag_index': 0,  # False
            u'eviction_policy': u'least-recently-stored',
            u'size_limit': self.standard_size_limit,
            u'cull_limit': 10,
            u'sqlite_auto_vacuum': 1,  # FULL
            u'sqlite_cache_size': self.standard_sqlite_cache_size,
            u'sqlite_journal_mode': u'wal',
            u'sqlite_mmap_size': self.standard_sqlite_mmap_size,
            u'sqlite_synchronous': 1,  # NORMAL
            u'disk_min_file_size': self.standard_disk_min_file_size,  # 32kb
            u'disk_pickle_protocol': dill.HIGHEST_PROTOCOL,
        }
    
    def get_optim_sql_settings(self):
        return {
            u'statistics': 0,  # False
            u'tag_index': 0,  # False
            u'eviction_policy': u'least-recently-stored',
            u'size_limit': self.optim_size_limit,
            u'cull_limit': 10,
            u'sqlite_auto_vacuum': 1,  # FULL
            u'sqlite_cache_size': self.optim_sqlite_cache_size,
            u'sqlite_journal_mode': u'wal',
            u'sqlite_mmap_size': self.optim_sqlite_mmap_size,
            u'sqlite_synchronous': 1,  # NORMAL
            u'disk_min_file_size': self.optim_disk_min_file_size,  # 32kb
            u'disk_pickle_protocol': dill.HIGHEST_PROTOCOL,
        }

    def get_perf_sql_settings(self):
        return {
            u'statistics': 0,  # False
            u'tag_index': 0,  # False
            u'eviction_policy': u'least-frequently-used',
            u'size_limit': self.perf_size_limit,
            u'cull_limit': 10,
            u'sqlite_auto_vacuum': 1,  # FULL
            u'sqlite_cache_size': self.perf_sqlite_cache_size,
            u'sqlite_journal_mode': u'wal',
            u'sqlite_mmap_size': self.perf_sqlite_mmap_size,
            u'sqlite_synchronous': 0,  # OFF
            u'disk_min_file_size': self.perf_disk_min_file_size,  # 32kb
            u'disk_pickle_protocol': dill.HIGHEST_PROTOCOL,
        }

    @property
    def sql_settings(self):
        if self.sqlmode in {'perf', 'max', 'performance'}:
            return self.get_perf_sql_settings()
        if self.sqlmode in {'std', 'standard', 'normal'}:
            return self.get_standard_sql_settings()
        if self.sqlmode in {'opt', 'optimized', 'optim', 'optimize'}:
            return self.get_optim_sql_settings()
        return self.get_default_sql_settings()


    @property
    def compression_lvl(self):
        if self.sqlmode in {'perf', 'max', 'performance'}:
            return self.perf_compression_level
        if self.sqlmode in {'std', 'standard', 'normal'}:
            return self.standard_compression_level
        if self.sqlmode in {'opt', 'optimized', 'optim', 'optimize'}:
            return self.optim_compression_level
        return self.default_compression_level


    def get_eviction_policies(self, table_name: str = None):
        table_name = table_name or self.default_table
        return {
            'none': {
                'init': None,
                'get': None,
                'cull': None,
            },
            'least-recently-stored': {
                'init': (
                    f'CREATE INDEX IF NOT EXISTS {table_name}_store_time ON'
                    f' {table_name} (store_time)'
                ),
                'get': None,
                'cull': 'SELECT {fields} FROM ' + table_name + ' ORDER BY store_time LIMIT ?',
            },
            'least-recently-used': {
                'init': (
                    f'CREATE INDEX IF NOT EXISTS {table_name}_access_time ON'
                    f' {table_name} (access_time)'
                ),
                'get': 'access_time = {now}',
                'cull': 'SELECT {fields} FROM ' + table_name + ' ORDER BY access_time LIMIT ?',
            },
            'least-frequently-used': {
                'init': (
                    f'CREATE INDEX IF NOT EXISTS {table_name}_access_count ON'
                    f' {table_name} (access_count)'
                ),
                'get': 'access_count = access_count + 1',
                'cull': 'SELECT {fields} FROM ' + table_name + ' ORDER BY access_count LIMIT ?',
            },
        }


    def get_sql_settings(self, table_name: str = None, **config) -> SqlConfig:
        settings = self.sql_settings
        if config: settings.update(config)
        policies = self.get_eviction_policies(table_name = table_name)
        return SqlConfig(mode = self.sqlmode, settings = settings, policies = policies)


