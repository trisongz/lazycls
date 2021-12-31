"""
Handler to check Class Imports
"""
import os
import sys
import importlib
import subprocess
import pkg_resources


def get_variable_separator():
    """
    Returns the environment variable separator for the current platform.
    :return: Environment variable separator
    """
    if sys.platform.startswith('win'): return ';'
    return ':'



class LazyLibType(type):
    
    def get_requirement(cls, name: str, clean: bool = True) -> str:
        if not clean: return name.strip()
        return name.split('=')[0].replace('>','').replace('<','').strip()
    
    def install_library(cls, library: str, upgrade: bool = True):
        pip_exec = [sys.executable, '-m', 'pip', 'install']
        if '=' not in library or upgrade: pip_exec.append('--upgrade')
        pip_exec.append(library)
        return subprocess.check_call(pip_exec, stdout=subprocess.DEVNULL)
        
    def is_available(cls, library: str) -> bool:
        """ Checks whether a Python Library is available."""
        try:
            _ = pkg_resources.get_distribution(library)
            return True
        except pkg_resources.DistributionNotFound: return False
    
    def is_imported(cls, library: str) -> bool:
        """ Checks whether a Python Library is currently imported."""
        return library in sys.modules
    
    def _ensure_lib_imported(cls, library: str):
        clean_lib = cls.get_requirement(library, True)
        if not cls.is_imported(clean_lib): sys.modules[clean_lib] = importlib.import_module(clean_lib)
        #if clean_lib not in sys.modules: sys.modules[clean_lib] = importlib.import_module(clean_lib)    
        return sys.modules[clean_lib]
        
    
    def import_lib(cls, library: str, resolve_missing: bool = True, require: bool = False, upgrade: bool = False):
        """ Lazily resolves libs.
            if available, returns the sys.modules[library]
            if missing and resolve_missing = True, will lazily install
        else:
            if require: raise ImportError
            returns None
        """
        clean_lib = cls.get_requirement(library, True)
        if not cls.is_available(clean_lib):
            if require and not resolve_missing: raise ImportError(f"Required Lib {library} is not available.")
            if not resolve_missing: return None
            cls.install_library(library, upgrade=upgrade)
        return cls._ensure_lib_imported(library)
    
    def get_binary_path(cls, executable: str):
        """
        Searches for a binary named `executable` in the current PATH. If an executable is found, its absolute path is returned
        else None.
        :param executable: Name of the binary
        :return: Absolute path or None
        """
        if 'PATH' not in os.environ: return None
        for directory in os.environ['PATH'].split(get_variable_separator()):
            binary = os.path.abspath(os.path.join(directory, executable))
            if os.path.isfile(binary) and os.access(binary, os.X_OK): return binary
        return None
    
    def is_exec_available(cls, executable: str) -> bool:
        return bool(cls.get_binary_path(executable) is not None)

    def __getattr__(cls, key):
        """
            LazyLib.is_avail_tensorflow -> bool
            LazyLib.tensorflow -> sys.modules[tensorflow] or None
            
            LazyLib.is_avail_pydantic -> True (since its installed with this lib)
            LazyLib.is_imported_tensorflow 
            LazyLib.is_exec_avail_bash -> True
        """
        if key.startswith('is_avail_'):
            lib_name = key.split('is_avail_')[-1].strip()
            return cls.is_available(lib_name)
        
        if key.startswith('is_imported_'):
            lib_name = key.split('is_imported_')[-1].strip()
            return cls.is_imported(lib_name)
        
        if key.startswith('is_exec_avail_'):
            exec_name = key.split('is_exec_avail_')[-1].strip()
            return cls.is_exec_available(exec_name)
        
        return cls.import_lib(key, resolve_missing=False, require=False)
        
class LazyLib(metaclass=LazyLibType):
    pass