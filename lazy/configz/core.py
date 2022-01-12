__all__ = (
    'NoPrefixNames',
    'ReplacePrefixNames',
    'ConfigClsMeta',
    'ConfigCls'
)

import os

from collections import namedtuple
from inspect import currentframe
from pydantic import BaseSettings
from pydantic.env_settings import SettingsSourceCallable
from typing import List, Tuple, Dict, Any, Type, TYPE_CHECKING

try:
    from pydantic.main import ModelMetaclass
except ImportError:  # pragma: no cover
    ModelMetaclass = type(BaseSettings)

from lazy.models.base import OrJson


NoPrefixNames: List[str] = ['base', 'authz']
ReplacePrefixNames: List[str] = ['Cfg', 'Configz', 'Config', 'Settings']

class ConfigClsMeta(ModelMetaclass):
    def __new__(mcs, name, bases, namespace):
        Config = namespace.setdefault("Config", type("Config", (), {}))
        if not hasattr(Config, "env_prefix"):
            _prefixed = False
            for n in NoPrefixNames:
                if n in name.lower(): 
                    Config.env_prefix = ""
                    _prefixed = True
                    break
            for n in ReplacePrefixNames:
                if _prefixed: continue
                if n in name:
                    Config.env_prefix = name.replace(n, "") + "_"
                    _prefixed = True
                    break
            if not _prefixed: Config.env_prefix = name + "_"

        cls = super().__new__(mcs, name, bases, namespace)
        if not getattr(Config, "auto_init", True): return cls
        else: return cls()


def dynamic_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """
    Settings source that loads variables from a .json/.yaml/.env/.txt file
    using cls.config.env_prefix.upper() + 'CONFIG_FILE' environment variable
    to determine the env file location.
    
    i.e. class CustomConfig(ConfigCls) = 'CUSTOM_CONFIG_FILE' -> /path/to/.env
    """
    encoding = settings.__config__.env_file_encoding
    config_file_env_name = settings.__config__.env_prefix.upper() + 'CONFIG_FILE'
    config_file_env_name = config_file_env_name.replace('CONFIG_CONFIG',' CONFIG_', 1) # Prevents duplicate config names 
    config_file_val = os.getenv(config_file_env_name)
    if not config_file_val: return {}

    ## Import lazy.io during runtime
    from lazy.io import get_path
    from lazy.serialize import Yaml, Json, Pkl

    p = get_path(config_file_val).resolve(True)
    if not p.exists(): return {}
    if p.extension in {'.yml', '.yaml'}: return Yaml.loads(p.read_text(encoding=encoding))
    if p.extension == '.json': return Json.loads(p.read_text(encoding=encoding))
    if p.extension in {'.pickle', '.pkl'}: return Pkl.loads(p.read_bytes())
    from dotenv import dotenv_values
    return dotenv_values(p.as_posix(), encoding=encoding)


class ConfigCls(BaseSettings, metaclass=ConfigClsMeta):

    class Config:
        @classmethod
        def customise_sources(cls, init_settings: SettingsSourceCallable, env_settings: SettingsSourceCallable, file_secret_settings: SettingsSourceCallable) -> Tuple[SettingsSourceCallable, ...]:
            return init_settings, dynamic_config_settings_source, env_settings, file_secret_settings
        
        auto_init = False
        env_prefix = ""
        arbitrary_types_allowed = True
        extra = 'allow'
        json_loads = OrJson.loads
        json_dumps = OrJson.dumps


    @property
    def env_values(self): 
        return {
            self.Config.env_prefix.upper() + k.upper(): v
            for k, v in self.dict().items()
        }

    def prefixed_dict(self, *args, **kwargs) -> Dict[str, Any]:
        """
        Similar to pydantic's `dict()`, but the `env_prefix` is included in the keys,
        so they match the original environment variable names.
        For example:
            class Database(ConfigCls):
                host = "localhost"
                username = "admin"
            assert Database.dict() == {"host": "localhost", "username": "admin"}
            assert Database.prefixed_dict() == {"DATABASE_HOST": "localhost", "DATABASE_USERNAME": "admin"}
        """
        return {
            self.Config.env_prefix.upper() + k.upper(): v
            for k, v in self.dict(*args, **kwargs).items()
        }

    def __call__(self, *args, **kwargs) -> "Config":
        """
        Instantiate this class as if you were calling the class itself.
        """
        return type(self)(*args, **kwargs)
    
    def reload(self, *args, **kwargs):
        """
        Reloads the class by reinitializing and reloading environment variables
        if they've changed.
        
        os.environ['DATABASE_HOST'] = 'localhost'
        assert Database.host == 'localhost'
        os.environ['DATABASE_HOST'] = '127.0.0.1'
        Database.reload()
        assert Database.host == '127.0.0.1'
        """
        self = self.__init__(*args, **kwargs)
    
    def update_config(self, **kwargs) -> Type["Config"]:
        if not kwargs: return
        self.reload(**kwargs)
    

def populate_globals(globs=None):
    """
    Search for instances of `ConfigCls` in the global variables in the calling context
    and then update the global variables with the `prefixed_dict()` of those `ConfigCls` isntances.
    For example, this code::
        class Database(ConfigCls):
            host = "localhost"
            username = "admin"
        populate_globals()
    will result in global variables `DATABASE_HOST` and `DATABASE_USERNAME`.
    This is useful in e.g. Django where settings need to be declared at the global level.
    You can pass your own dict for the function to use instead of the current global variables.
    """
    if globs is None:
        globs = currentframe().f_back.f_globals

    for cls in list(globs.values()):
        if isinstance(cls, ConfigCls):
            globs.update(cls.prefixed_dict())