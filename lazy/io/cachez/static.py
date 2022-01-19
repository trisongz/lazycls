__all__ = (
    'full_name',
    'WindowsExceptionError',
    'Constant',
    'ENOVAL',
    'UNKNOWN',
    'MODE_NONE',
    'MODE_RAW',
    'MODE_BINARY',
    'MODE_TEXT',
    'MODE_PICKLE',
    'METADATA',
)

def full_name(func):
    "Return full name of `func` by adding the module and function name."
    return func.__module__ + '.' + func.__qualname__


try: 
    class WindowsExceptionError(WindowsError):
        pass
except:
    class WindowsExceptionError(Exception):
        "Windows error place-holder on platforms without support."


class Constant(tuple):
    "Pretty display of immutable constant."

    def __new__(cls, name):
        return tuple.__new__(cls, (name,))

    def __repr__(self):
        return '%s' % self[0]

ENOVAL = Constant('ENOVAL')
UNKNOWN = Constant('UNKNOWN')

MODE_NONE = 0
MODE_RAW = 1
MODE_BINARY = 2
MODE_TEXT = 3
MODE_PICKLE = 4

METADATA = {
    u'count': 0,
    u'size': 0,
    u'hits': 0,
    u'misses': 0,
}
