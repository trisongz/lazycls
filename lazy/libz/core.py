"""
Handler to check Class Imports
"""
import os
import sys
import importlib
import subprocess
import pkg_resources
import pathlib

from typing import List, Type, Any, Union, Dict
from types import ModuleType

_root_path = pathlib.Path(__file__).parent
_scriptz_path = _root_path.joinpath('scriptz')
_bash_scriptz_path = _scriptz_path.joinpath('bash')

try:
    from google.colab import drive
    _is_colab = True
except ImportError: _is_colab = False

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
        if library == 'colab': return _is_colab
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
    
    def import_lib(cls, library: str, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False) -> ModuleType:
        """ Lazily resolves libs.

            if pip_name is provided, will install using pip_name, otherwise will use libraryname

            ie ->   Lib.import_lib('fuse', 'fusepy') # if fusepy is not expected to be available, and fusepy is the pip_name
                    Lib.import_lib('fuse') # if fusepy is expected to be available
            
            returns `fuse` as if you ran `import fuse`
        
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
    
    def import_module(cls, name: str, library: str = None, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False) -> ModuleType:
        """ Lazily resolves libs and imports the name, aliasing
            immportlib.import_module

            ie ->   Lib.import_module('tensorflow.io.gfile', 'tensorflow') # if tensorflow is not expected to be available
                    Lib.import_module('tensorflow.io.gfile') # if tensorflow is expected to be available
            returns tensorflow.io.gfile
        """
        if library:
            cls.import_lib(library, pip_name, resolve_missing, require, upgrade)
            return importlib.import_module(name, package=library)
        return importlib.import_module(name)

    def import_module_attr(cls, name: str, module_name: str, library: str = None, pip_name: str = None, resolve_missing: bool = True, require: bool = False, upgrade: bool = False) -> Any:
        """ Lazily resolves libs and imports the name, aliasing
            immportlib.import_module
            Returns an attribute from the module

            ie ->   Lib.import_module_attr('GFile', 'tensorflow.io.gfile', 'tensorflow') # if tensorflow is not expected to be available
                    Lib.import_module_attr('GFile', 'tensorflow.io.gfile') # if tensorflow is expected to be available
            returns GFile
        """
        mod = cls.import_module(name=module_name, library=library, pip_name=pip_name, resolve_missing = resolve_missing, require = require, upgrade = upgrade)
        return getattr(mod, name)
    
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
    
    def run_bash(cls, cmd: str = None, bash_name: str = 'bash', *args, **kwargs):
        """
        Utility to run bash command

        Lib.run_bash('/path/to/script.sh') -> bash /path/to/script.sh
        """

        from lazy.cmd import Cmd
        bash = Cmd(bash_name)
        return bash(cmd, *args, **kwargs).val
    
    def run_bash_script(cls, script_name: str, *args, **kwargs):
        if not script_name.endswith('.sh'): script_name += '.sh'
        scriptz = _bash_scriptz_path.joinpath(script_name)
        assert scriptz.exists(), f'Invalid Script: {script_name} does not exist.'
        cls.run_bash(cmd=scriptz.as_posix())
    
    def run_fusemount(cls, bucket: str, mountpoint: str, auth_config: Dict[Any, Any] = None, *cliargs, **kwargs):
        """
        Runs fuse_v1
        Base Args:
        BUCKET=$1
        MOUNT_PATH=$2

        For S3:
        S3_TYPE=$3
        """

        from lazy.configz import CloudAuthz
        from lazy.cmd import Cmd

        uris = bucket.split('://', maxsplit=1)
        provider, bucket_path = uris[0], uris[-1]
        if auth_config is not None: CloudAuthz.update_authz(**auth_config)
        bash = Cmd('bash')
        pathlib.Path(mountpoint).mkdir(exist_ok=True, parents=True)
        CloudAuthz.set_authz_env()
        if provider == 'gs':
            scriptz = _bash_scriptz_path.joinpath('gcsfuse.sh')
            return bash(scriptz.as_posix(), bucket_path, mountpoint, *cliargs, **kwargs).val
        
        scriptz = _bash_scriptz_path.joinpath('s3fuse.sh')
        if provider == 's3': return bash(scriptz.as_posix(), bucket_path, mountpoint, 'aws', *cliargs, **kwargs).val
        return bash(scriptz.as_posix(), bucket_path, mountpoint, provider, *cliargs, **kwargs).val

    @staticmethod
    def reload_module(module: ModuleType):
        return importlib.reload(module)

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
    
    def get(cls, name: str, attr_name: str = None, pip_name: str = None, resolve_missing: bool = True) -> Union[ModuleType, Any]:
        """
        Resolves the import based on params.

        Importing a module:
        Lib.get('tensorflow') -> tensorflow Module
        Lib.get('fuse', pip_name='fusepy') -> fuse Module
        
        Importing a submodule:
        Lib.get('tensorflow.io') -> io submodule

        Importing something from within a submodule:
        Lib.get('tensorflow.io.gfile', 'GFile') -> GFile class
        """
        if attr_name: 
            lib_name = name.split('.', 1)[0]
            return cls.import_module_attr(attr_name, module_name = name, library = lib_name, pip_name = pip_name, resolve_missing = resolve_missing)
        if '.' in name: 
            lib_name = name.split('.', 1)[0]
            return cls.import_module(name, library=lib_name, pip_name=pip_name, resolve_missing=resolve_missing)
        return cls.import_lib(name, pip_name=pip_name, resolve_missing=resolve_missing)
    
    def _parse_name(cls, name: str) -> Dict[str, str]:
        """
        Resolves the name into a dict = 
        {
            'library': str,
            'pip_name': Optional[str],
            'module_name': Optional[str],
            'attr_name': Optional[str]
        }
        
        - module.submodule:attribute
        - pip_name|module.submodule:attribute # if pip_name is not the same
        
        Lib['tensorflow']                   -> {'library': 'tensorflow'}
        Lib['fusepy|fuse']                  -> {'library': 'fuse', 'pip_name': 'fusepy'}
        Lib['tensorflow.io']                -> {'library': 'tensorflow', 'module_name': 'tensorflow.io'}
        Lib['tensorflow.io.gfile:GFile']    -> {'library': 'tensorflow', 'module_name': 'tensorflow.io.gfile', 'attr_name': GFile}
        """
        rez = {'library': '', 'pip_name': None, 'module_name': None, 'attr_name': None}
        _name = name.strip()
        if ':' in _name:
            _s = _name.split(':', 1)
            rez['attr_name'] = _s[-1]
            _name = _s[0]
        if '|' in _name:
            _s = _name.split('|', 1)
            rez['pip_name'] = _s[0]
            _name = _s[-1]
        if '.' in _name:
            _s = _name.split('.', 1)
            rez['module_name'] = _name
            rez['library'] = _s[0]
        else: 
            #rez['module_name'] = _name
            rez['library'] = _name
        return rez
    
    def __getitem__(cls, name: str) -> ModuleType:
        """
        Resolves the import based on params. 
        Will automatically assume as resolve_missing = True, require = True
        
        The naming scheme is resolved as
        
        - module.submodule:attribute
        - pip_name|module.submodule:attribute # if pip_name is not the same

        Importing a module:
        Lib['tensorflow'] -> tensorflow Module
        Lib['fusepy|fuse'] -> fuse Module
        
        Importing a submodule:
        Lib['tensorflow.io'] -> io submodule

        Importing something from within a submodule:
        Lib['tensorflow.io.gfile:GFile'] -> GFile class
        """
        r = cls._parse_name(name)
        if r.get('attr_name'): return cls.import_module_attr(r['attr_name'], module_name = r['module_name'], library = r['library'], pip_name = r['pip_name'], resolve_missing = True, require = True)
        if r.get('module_name'): return cls.import_module(name = r['module_name'], library = r['library'], pip_name = r['pip_name'], resolve_missing = True, require = True)
        return cls.import_lib(r['library'], pip_name = r['pip_name'], resolve_missing = True, require = True)



        
class Lib(metaclass=LibType):
    pass