"""
Handler to check Class Imports
"""
import os
import sys
import importlib
import subprocess
import pkg_resources

from typing import List


def get_variable_separator():
    """
    Returns the environment variable separator for the current platform.
    :return: Environment variable separator
    """
    if sys.platform.startswith('win'): return ';'
    return ':'

class PkgInstall:
    win: str = 'choco install [flags]'
    mac: str = 'brew [flags] install '
    linux: str = 'apt-get -y [flags] install'

    @classmethod
    def get_args(cls, binary: str, flags: List[str] = None):
        """
        Builds the install args for the system
        """
        flag_str = ' '.join(flags) if flags else '' 
        if sys.platform.startswith('win'):
            b = cls.win + ' ' + binary
            b = b.replace('[flags]', flag_str)
        
        if sys.platform.startswith('linux'):
            b = cls.linux + ' ' + binary
            b = b.replace('[flags]', flag_str)

        if sys.platform.startswith('darwin'):
            b = cls.mac + ' ' + binary
            b = b.replace('[flags]', flag_str)
        return [i.strip() for i in b.split(' ') if i.strip()]
        


class LibType(type):
    
    def get_requirement(cls, name: str, clean: bool = True) -> str:
        # Replaces '-' with '_'
        # for any library such as tensorflow-text -> tensorflow_text
        name = name.replace('-', '_')
        if not clean: return name.strip()
        return name.split('=')[0].replace('>','').replace('<','').strip()
    
    def install_library(cls, library: str, upgrade: bool = True):
        pip_exec = [sys.executable, '-m', 'pip', 'install']
        if '=' not in library or upgrade: pip_exec.append('--upgrade')
        pip_exec.append(library)
        return subprocess.check_call(pip_exec, stdout=subprocess.DEVNULL)
    
    def install_binary(cls, binary: str, flags: List[str] = None):
        if cls.get_binary_path(binary): return
        args = PkgInstall.get_args(binary, flags)
        return subprocess.check_call(args, stdout=subprocess.DEVNULL)
        
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
        return sys.modules[clean_lib]
    
    def _ensure_lib_installed(cls, library: str, pip_name: str = None, upgrade: bool = False):
        clean_lib = cls.get_requirement(library, True)
        if not cls.is_available(clean_lib):
            cls.install_library(pip_name or library, upgrade=upgrade)

    def _ensure_binary_installed(cls, binary: str, flags: List[str] = None):
        return cls.install_binary(binary, flags)
    
    def import_lib(cls, library: str, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False):
        """ Lazily resolves libs.

            if pip_name is provided, will install using pip_name, otherwise will use libraryname
        
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
            cls.install_library(pip_name or library, upgrade=upgrade)
        return cls._ensure_lib_imported(library)
    
    def import_cmd(cls, binary: str, resolve_missing: bool = True, require: bool = False, flags: List[str] = None):
        """ Lazily builds a lazy.Cmd based on binary
        
            if available, returns the lazy.Cmd(binary)
            if missing and resolve_missing = True, will lazily install in host system
        else:
            if require: raise ImportError
            returns None
        """
        if not cls.is_exec_available(binary):
            if require and not resolve_missing: raise ImportError(f"Required Executable {binary} is not available.")
            if not resolve_missing: return None
            cls.install_binary(binary, flags=flags)
        from lazy.cmd import Cmd
        return Cmd(binary=binary)
    
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
            Lib.is_avail_tensorflow -> bool
            Lib.tensorflow -> sys.modules[tensorflow] or None
            
            Lib.is_avail_pydantic -> True (since its installed with this lib)
            Lib.is_imported_tensorflow 
            Lib.is_avail_bin_bash -> True
            Lib.is_avail_exec_bash -> True
        """
        if key.startswith('is_avail_bin_'):
            exec_name = key.split('is_avail_bin_')[-1].strip()
            return cls.is_exec_available(exec_name)
        
        if key.startswith('is_avail_exec_'):
            exec_name = key.split('is_avail_exec_')[-1].strip()
            return cls.is_exec_available(exec_name)
        
        if key.startswith('is_avail_lib_'):
            lib_name = key.split('is_avail_lib_')[-1].strip()
            return cls.is_available(lib_name)
        
        if key.startswith('is_avail_'):
            lib_name = key.split('is_avail_')[-1].strip()
            return cls.is_available(lib_name)
        
        if key.startswith('is_imported_'):
            lib_name = key.split('is_imported_')[-1].strip()
            return cls.is_imported(lib_name)
        
        if key.startswith('cmd_'):
            binary_name = key.split('cmd_')[-1].strip()
            return cls.import_cmd(binary=binary_name)
        
        return cls.import_lib(key, resolve_missing=False, require=False)
        
class Lib(metaclass=LibType):
    pass