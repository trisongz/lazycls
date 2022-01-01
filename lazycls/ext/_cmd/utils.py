from .base import *

def is_iterable(value):
    try: iter(value)
    except TypeError: return False
    return True



class ExecFile:
    """ Simple container for a filename. Mainly needed to be able to run
        `isinstance(..., ExecFile)`
        Extends with some utils from modified lazycls.ext.pathio
    """

    def __init__(self, filename):
        self.filename = filename
        self._path = None
    
    @property
    def path(self) -> PathLike:
        if not self._path: self._path = Path.get_path(self.filename, resolve=True)
        return self._path
    
    @property
    def is_file(self): return self.path.is_file()
    
    @property
    def is_dir(self): return self.path.is_dir()
    
    @property
    def exists(self): return self.path.exists()
    
    
    