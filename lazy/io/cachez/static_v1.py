__all__ = (
    'full_name',
    'WindowsExceptionError',
    'Constant',
    'DBNAME',
    'ENOVAL',
    'UNKNOWN',
    'MODE_NONE',
    'MODE_RAW',
    'MODE_BINARY',
    'MODE_TEXT',
    'MODE_PICKLE',
    'DEFAULT_SETTINGS',
    'METADATA',
    'EVICTION_POLICY'
)

import dill

def full_name(func):
    "Return full name of `func` by adding the module and function name."
    return func.__module__ + '.' + func.__qualname__



try: 
    class WindowsExceptionError(WindowsError):
        pass
except:
    class WindowsExceptionError(Exception):
        "Windows error place-holder on platforms without support."


#except NameError:




class Constant(tuple):
    "Pretty display of immutable constant."

    def __new__(cls, name):
        return tuple.__new__(cls, (name,))

    def __repr__(self):
        return '%s' % self[0]


DBNAME = 'cache.db'
ENOVAL = Constant('ENOVAL')
UNKNOWN = Constant('UNKNOWN')

MODE_NONE = 0
MODE_RAW = 1
MODE_BINARY = 2
MODE_TEXT = 3
MODE_PICKLE = 4

DEFAULT_SETTINGS = {
    u'statistics': 0,  # False
    u'tag_index': 0,  # False
    u'eviction_policy': u'least-recently-stored',
    #u'size_limit': 2 ** 30,  # 1gb
    u'size_limit': 2 ** 30 * 20,  # 20gb
    u'cull_limit': 10,
    u'sqlite_auto_vacuum': 1,  # FULL
    u'sqlite_cache_size': 2 ** 13,  # 8,192 pages
    u'sqlite_journal_mode': u'wal',
    #u'sqlite_mmap_size': 2 ** 26,  # 64mb
    u'sqlite_mmap_size': 536870912,  # 512mb
    u'sqlite_synchronous': 1,  # NORMAL
    u'disk_min_file_size': 2 ** 15,  # 32kb
    u'disk_pickle_protocol': dill.HIGHEST_PROTOCOL,
}

METADATA = {
    u'count': 0,
    u'size': 0,
    u'hits': 0,
    u'misses': 0,
}

EVICTION_POLICY = {
    'none': {
        'init': None,
        'get': None,
        'cull': None,
    },
    'least-recently-stored': {
        'init': (
            'CREATE INDEX IF NOT EXISTS Cache_store_time ON'
            ' Cache (store_time)'
        ),
        'get': None,
        'cull': 'SELECT {fields} FROM Cache ORDER BY store_time LIMIT ?',
    },
    'least-recently-used': {
        'init': (
            'CREATE INDEX IF NOT EXISTS Cache_access_time ON'
            ' Cache (access_time)'
        ),
        'get': 'access_time = {now}',
        'cull': 'SELECT {fields} FROM Cache ORDER BY access_time LIMIT ?',
    },
    'least-frequently-used': {
        'init': (
            'CREATE INDEX IF NOT EXISTS Cache_access_count ON'
            ' Cache (access_count)'
        ),
        'get': 'access_count = access_count + 1',
        'cull': 'SELECT {fields} FROM Cache ORDER BY access_count LIMIT ?',
    },
}

