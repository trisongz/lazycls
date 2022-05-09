import time

from lazy.types import *
from lazy.libz import Lib
from lazy.io import get_path
from .config import logger
from .config import RedisConfigz

from lazy.utils.procs import exec_daemon

import time

## port of internal lib used to start services
try: 
    from redis import Redis as RedisClient
except ImportError: 
    RedisClient: object = None

_HOST_OS = None
_REDIS_ENABLED = False
_REDIS_PY = False
_REDIS_INSTALL = None

def _set_host_os():
    global _HOST_OS
    if _HOST_OS is not None: return
    import platform
    _HOST_OS = platform.system().lower()

def _set_redis_install():
    global _REDIS_INSTALL
    if _REDIS_INSTALL is not None: return
    _set_host_os()
    if _HOST_OS == 'darwin':
        _REDIS_INSTALL = 'redis'
    elif _HOST_OS == 'windows':
        _REDIS_INSTALL = 'redis-64'
    else:
        _REDIS_INSTALL = 'redis-server'

def _get_redispy():
    global RedisClient, _REDIS_PY
    if _REDIS_PY and RedisClient: return
    if RedisClient is None:
        RedisClient = Lib['redis:Redis']
    _REDIS_PY = True


def require_redis():
    global RedisClient, _REDIS_ENABLED
    if _REDIS_ENABLED: return
    if not Lib.is_exec_available('redis-server'):
        _set_redis_install()
        Lib.install_binary(_REDIS_INSTALL)
    _get_redispy()    
    #if RedisClient is None: raise ImportError('Redis')
    #global RedisClient
    #if RedisClient is not None: return


# RedisBackend is a class that contains all the configuration for the Redis backend
class RedisBackend(RedisConfigz):
    cachedir: str = None
    sentinel: bool = False
    sentinel_master: str = None
    fallback_enabled: bool = True
    local_redis: bool = True
    is_deployment: bool = False
    startup_delay: int = 7
    startup_attempts: int = 10

    class Config:
        env_prefix = "REDIS_"

    def wait_for_redis_started(self):
        """
        Wait for the Redis server to start
        """
        r = RedisClient(host=self.host, port=self.port, db=self.database, password=self.password, socket_timeout = 5.0, socket_connect_timeout = 5.0, socket_keepalive = False)
        attempts, success = 0, False
        while attempts < self.startup_attempts:
            try:
                r.ping()
                success = True
                break            
            except:
                time.sleep(self.startup_delay)
        if not success: raise Exception
    
    def check_redis_connection(self):
        """Doing a quick ping and then timeout. If does not connect in 5 secs, then is not fast enough."""
        r = RedisClient(host=self.host, port=self.port, db=self.database, password=self.password, socket_timeout = 5.0, socket_connect_timeout = 5.0, socket_keepalive = False)
        try: return bool(r.ping())
        except: return False

    def set_to_default_redis_config(self):
        """ Changes Redis cfg to default"""
        self.host = '127.0.0.1'
        self.port = 6379
        self.password = None

    def get_cachedir(self):
        d = get_path(self.cachedir) if self.cachedir else Lib.get_cwd('db', string=False)
        d.mkdir(parents=True)
        return d.as_posix()

    def start_redis_local(self):
        """ Ensures that local redis is running if local_redis = true or manually called if not able to connect to external redis"""
        # Doing System Platform Check
        require_redis()
        rstatus = Lib.run_cmd('redis-cli ping', raise_error=False)
        if not rstatus:
            if self.is_deployment: exec_daemon('redis-server', cwd=self.get_cachedir(), set_proc_uid=False)
            else: exec_daemon('redis-server', cwd=self.get_cachedir())
            self.wait_for_redis_started()
        self.set_to_default_redis_config()

    def ensure_redis(self):
        """If external redis, checks connect.
            falls back to creating internal redis.
        """
        if not self.local_redis and self.check_redis_connection(): return logger.info('External Redis Connection Successful')
        if not self.fallback_enabled: raise RuntimeError('Unable to Establish Redis connection')
        if self.local_redis:
            logger.info('Setting up Local Redis Connection')
            self.start_redis_local()
            #if self.check_redis_connection(): 
            return logger.info('Local Redis Connection Successful')
        logger.info('Unable to Establish Redis Connection')
        raise

    @property
    def redis_config(self):
        return dict(
            host = self.host,
            port = self.port,
            password = self.password,
            database = self.database,
            sentinel = self.sentinel,
            sentinel_master = self.sentinel_master
        )
    
