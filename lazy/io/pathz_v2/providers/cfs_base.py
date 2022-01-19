from types import ModuleType
from typing import Callable, Any, Optional, Coroutine, Type, Union, List, ClassVar

try: import gcsfs
except ImportError: gcsfs: ModuleType = None

try: import s3fs
except ImportError: s3fs: ModuleType = None

from .base import rewrite_async_syntax, NormalAccessor, func_as_method_coro


class CFSType(type):
    fs: ModuleType = None
    fsa: ModuleType = None
    fs_name: str = None # gcsfs

    @classmethod
    def is_ready(cls):
        return bool(cls.fsa and cls.fs)
    
    #@classmethod
    def build_gcsfs(cls, **auth_config):
        from lazy.libz import Lib
        from lazy.configz.cloudz import CloudAuthz

        gcsfs: ModuleType = Lib.import_lib('gcsfs')
        Lib.reload_module(gcsfs)

        authz = CloudAuthz()
        if auth_config: authz.update_authz(**auth_config)
        _config = {}
        gcp_auth = authz.get_gcp_auth()
        #print(gcp_auth)
        if gcp_auth and gcp_auth.exists(): _config['token'] = gcp_auth.as_posix()
        gcp_project = authz.get_gcp_project()
        if gcp_project: _config['project'] = gcp_project
        if authz.gcs_client_config: _config['client_kwargs'] = authz.gcs_client_config
        if authz.gcs_config: _config['config_kwargs'] = authz.gcs_config
        cls.fs = gcsfs.GCSFileSystem(asynchronous=False, **_config)
        cls.fsa = rewrite_async_syntax(gcsfs.GCSFileSystem(asynchronous=True, **_config), 'gs')
    
    #@classmethod
    def build_s3fs(cls, **auth_config):
        from lazy.libz import Lib
        from lazy.configz.cloudz import CloudAuthz

        s3fs = Lib.import_lib('s3fs')
        Lib.reload_module(s3fs)

        authz = CloudAuthz()
        if auth_config: authz.update_authz(**auth_config)
        _config = {}
        if authz.aws_access_key_id:
            _config['key'] = authz.aws_access_key_id
            _config['secret'] = authz.aws_secret_access_key
        elif authz.aws_access_token: _config['token'] = authz.aws_access_token
        elif not authz.boto_config: _config['anon'] = True
        if authz.s3_config: _config['config_kwargs'] = authz.s3_config
        cls.fs = s3fs.S3FileSystem(asynchronous=False, **_config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))
    
    #@classmethod
    def build_minio(cls, **auth_config):
        from lazy.libz import Lib
        from lazy.configz.cloudz import CloudAuthz

        s3fs: ModuleType = Lib.import_lib('s3fs')
        Lib.reload_module(s3fs)

        authz = CloudAuthz()
        if auth_config: authz.update_authz(**auth_config)
        _config = {}
        
        if authz.minio_secret_key:
            _config['key'] = authz.minio_access_key
            _config['secret'] = authz.minio_secret_key
        elif authz.minio_access_token: _config['token'] = authz.minio_access_token
        _config['client_kwargs'] = {'endpoint_url': authz.minio_endpoint}
        if authz.minio_config: _config['config_kwargs'] = authz.minio_config

        cls.fs = s3fs.S3FileSystem(**_config)
        cls.fsa = rewrite_async_syntax(s3fs.S3FileSystem(asynchronous=True, **_config))

    #@classmethod
    def build_filesystems(cls, force: bool = False, **auth_config):
        """
        Lazily inits the filesystems
        """
        if cls.fs and cls.fsa and not force: return
        if cls.fs_name == 's3fs':
            cls.build_s3fs(**auth_config)
        elif cls.fs_name == 'minio':
            cls.build_minio(**auth_config)
        elif cls.fs_name == 'gcsfs':
            cls.build_gcsfs(**auth_config)


    @classmethod
    def reload_filesystem(cls):
        """ 
        Reinitializes the Filesystem
        """
        raise NotImplementedError


def _dummy_func(*args, **kwargs) -> Optional[Any]:
    pass

async def dummy_async_func(*args, **kwargs)  -> Optional[Any]:
    pass

def create_method_fs(cfs: Type[CFSType], name: Union[str, List[str]],  func: Optional[Callable] = None, fs_type: str = 'fs') -> Optional[Callable]:
    if not hasattr(cfs, fs_type):
        #print(f'{cfs.__name__} has no {fs_type}')
        return _dummy_func
    fs_module = getattr(cfs, fs_type)
    if not isinstance(name, list): name = [name]
    for n in name:
        if hasattr(fs_module, n):
            #print(f'{cfs.__name__}:{fs_module} has func {fs_type}:{n}')
            if func: return func(getattr(fs_module, n))
            return getattr(fs_module, n)
    #print(f'{cfs.__name__} has no func {fs_type}:{name}')
    return _dummy_func

def create_async_method_fs(cfs: Type[CFSType], name: Union[str, List[str]], func: Optional[Callable] = None, fs_type: str = 'fsa') -> Optional[Union[Callable, Coroutine]]:
    if not hasattr(cfs, fs_type):
        return dummy_async_func
    fs_module = getattr(cfs, fs_type)
    if not isinstance(name, list): name = [name]
    for n in name:
        if hasattr(fs_module, n):
            if func: return func(getattr(fs_module, n))
            return getattr(fs_module, n)
    return dummy_async_func

def create_staticmethod(cfs: Type[CFSType], name: Union[str, List[str]], fs_type: str = 'fs'):
    return create_method_fs(cfs, name = name, func = staticmethod, fs_type = fs_type)


def create_async_coro(cfs: Type[CFSType], name: Union[str, List[str]], fs_type: str = 'fs'):
    return create_async_method_fs(cfs, name = name, func = func_as_method_coro, fs_type = fs_type)


class BaseAccessor(NormalAccessor):
    """Dummy Accessor class
    """
    class CFS(metaclass=CFSType):
        pass
    
    info: Callable = create_staticmethod(CFS, 'info')
    stat: Callable = create_staticmethod(CFS, 'stat')
    size: Callable = create_staticmethod(CFS, 'size')
    exists: Callable = create_staticmethod(CFS, 'exists')
    is_dir: Callable = create_staticmethod(CFS, 'isdir')
    is_file: Callable = create_staticmethod(CFS, 'isfile')
    copy_file: Callable = create_staticmethod(CFS, 'cp_file')
    get_file: Callable = create_staticmethod(CFS, 'get_file')
    put_file: Callable = create_staticmethod(CFS, 'put_file')
    metadata: Callable = create_staticmethod(CFS, ['metadata', 'info'])

    open: Callable = create_method_fs(CFS, 'open')
    listdir: Callable = create_method_fs(CFS, 'ls')    
    glob: Callable = create_method_fs(CFS, 'glob')
    touch: Callable = create_method_fs(CFS, 'touch')
    copy: Callable = create_method_fs(CFS, 'copy')
    get: Callable = create_method_fs(CFS, 'get')
    put: Callable = create_method_fs(CFS, 'put')
    mkdir: Callable = create_method_fs(CFS, 'mkdir')
    makedirs: Callable = create_method_fs(CFS, ['makedirs', 'mkdirs'])
    unlink: Callable = create_method_fs(CFS, 'rm_file')
    rmdir: Callable = create_method_fs(CFS, 'rmdir')
    rename : Callable = create_method_fs(CFS, 'rename')
    replace : Callable = create_method_fs(CFS, 'rename')
    remove : Callable = create_method_fs(CFS, 'rm')
    modified: Callable = create_method_fs(CFS, 'modified')
    url: Callable = create_method_fs(CFS, 'url')
    ukey: Callable = create_method_fs(CFS, 'ukey')
    invalidate_cache: Callable = create_method_fs(CFS, 'invalidate_cache')
    
    filesys: ClassVar = CFS.fs
    async_filesys: ClassVar = CFS.fsa
    
    # Async Methods
    async_stat: Callable = create_async_coro(CFS, 'stat')
    async_touch: Callable = create_async_coro(CFS, 'touch')
    async_ukey: Callable = create_async_coro(CFS, 'ukey')
    async_size: Callable = create_async_coro(CFS, 'size')
    async_url: Callable = create_async_coro(CFS, 'url')
    async_modified: Callable = create_async_coro(CFS, 'modified')
    async_invalidate_cache: Callable = create_async_coro(CFS, 'invalidate_cache')
    async_rename: Callable = create_async_coro(CFS, 'rename')
    async_replace: Callable = create_async_coro(CFS, 'rename')

    async_info: Callable = create_async_method_fs(CFS, 'async_info')
    async_exists: Callable = create_async_method_fs(CFS, 'async_exists')
    async_glob: Callable = create_async_method_fs(CFS, 'async_glob')
    async_is_dir: Callable = create_async_method_fs(CFS, 'async_isdir')
    async_is_file: Callable = create_async_method_fs(CFS, 'async_is_file')
    async_copy: Callable = create_async_method_fs(CFS, 'async_copy')
    async_copy_file: Callable = create_async_method_fs(CFS, 'async_cp_file')
    async_get: Callable = create_async_method_fs(CFS, 'async_get')
    async_get_file: Callable = create_async_method_fs(CFS, 'async_get_file')
    async_put: Callable = create_async_method_fs(CFS, 'async_put')
    async_put_file: Callable = create_async_method_fs(CFS, 'async_put_file')
    async_metadata: Callable = create_async_method_fs(CFS, 'async_info')
    async_open: Callable = create_async_method_fs(CFS, '_open')
    async_mkdir: Callable = create_async_method_fs(CFS, 'async_mkdir')
    async_makedirs: Callable = create_async_method_fs(CFS, 'async_makedirs')
    async_unlink: Callable = create_async_method_fs(CFS, 'async_rm_file')
    async_rmdir: Callable = create_async_method_fs(CFS, 'async_rmdir')
    async_remove: Callable = create_async_method_fs(CFS, 'async_rm')
    async_rm: Callable = create_async_method_fs(CFS, 'async_rm')
    async_listdir: Callable = create_async_method_fs(CFS, ['async_listdir', 'async_list_objects'])

    @classmethod
    def reload_cfs(cls, **kwargs):
        cls.CFS.build_filesystems(**kwargs)
        cls.info: Callable = create_staticmethod(cls.CFS, 'info')
        cls.stat: Callable = create_staticmethod(cls.CFS, 'stat')
        cls.size: Callable = create_staticmethod(cls.CFS, 'size')
        cls.size: Callable = create_staticmethod(cls.CFS, 'size')
        cls.exists: Callable = create_staticmethod(cls.CFS, 'exists')
        cls.is_dir: Callable = create_staticmethod(cls.CFS, 'isdir')
        cls.is_file: Callable = create_staticmethod(cls.CFS, 'isfile')
        cls.copy_file: Callable = create_staticmethod(cls.CFS, 'cp_file')
        cls.get_file: Callable = create_staticmethod(cls.CFS, 'get_file')
        cls.put_file: Callable = create_staticmethod(cls.CFS, 'put_file')
        cls.metadata: Callable = create_staticmethod(cls.CFS, ['metadata', 'info'])

        cls.open: Callable = create_method_fs(cls.CFS, 'open')
        cls.listdir: Callable = create_method_fs(cls.CFS, 'ls')    
        cls.glob: Callable = create_method_fs(cls.CFS, 'glob')
        cls.touch: Callable = create_method_fs(cls.CFS, 'touch')
        cls.copy: Callable = create_method_fs(cls.CFS, 'copy')
        cls.get: Callable = create_method_fs(cls.CFS, 'get')
        cls.put: Callable = create_method_fs(cls.CFS, 'put')
        cls.mkdir: Callable = create_method_fs(cls.CFS, 'mkdir')
        cls.makedirs: Callable = create_method_fs(cls.CFS, ['makedirs', 'mkdirs'])
        cls.unlink: Callable = create_method_fs(cls.CFS, 'rm_file')
        cls.rmdir: Callable = create_method_fs(cls.CFS, 'rmdir')
        cls.rename : Callable = create_method_fs(cls.CFS, 'rename')
        cls.replace : Callable = create_method_fs(cls.CFS, 'rename')
        cls.remove : Callable = create_method_fs(cls.CFS, 'rm')
        cls.modified: Callable = create_method_fs(cls.CFS, 'modified')
        cls.url: Callable = create_method_fs(cls.CFS, 'url')
        cls.ukey: Callable = create_method_fs(cls.CFS, 'ukey')
        cls.invalidate_cache: Callable = create_method_fs(cls.CFS, 'invalidate_cache')
        
        cls.filesys = cls.CFS.fs
        cls.async_filesys = cls.CFS.fsa
        
        # Async Methods
        cls.async_stat: Callable = create_async_coro(cls.CFS, 'stat')
        cls.async_touch: Callable = create_async_coro(cls.CFS, 'touch')
        cls.async_ukey: Callable = create_async_coro(cls.CFS, 'ukey')
        cls.async_size: Callable = create_async_coro(cls.CFS, 'size')
        cls.async_url: Callable = create_async_coro(cls.CFS, 'url')
        cls.async_modified: Callable = create_async_coro(cls.CFS, 'modified')
        cls.async_invalidate_cache: Callable = create_async_coro(cls.CFS, 'invalidate_cache')
        cls.async_rename: Callable = create_async_coro(cls.CFS, 'rename')
        cls.async_replace: Callable = create_async_coro(cls.CFS, 'rename')

        cls.async_info: Callable = create_async_method_fs(cls.CFS, 'async_info')
        cls.async_exists: Callable = create_async_method_fs(cls.CFS, 'async_exists')
        cls.async_glob: Callable = create_async_method_fs(cls.CFS, 'async_glob')
        cls.async_is_dir: Callable = create_async_method_fs(cls.CFS, 'async_isdir')
        cls.async_is_file: Callable = create_async_method_fs(cls.CFS, 'async_is_file')
        cls.async_copy: Callable = create_async_method_fs(cls.CFS, 'async_copy')
        cls.async_copy_file: Callable = create_async_method_fs(cls.CFS, 'async_cp_file')
        cls.async_get: Callable = create_async_method_fs(cls.CFS, 'async_get')
        cls.async_get_file: Callable = create_async_method_fs(cls.CFS, 'async_get_file')
        cls.async_put: Callable = create_async_method_fs(cls.CFS, 'async_put')
        cls.async_put_file: Callable = create_async_method_fs(cls.CFS, 'async_put_file')
        cls.async_metadata: Callable = create_async_method_fs(cls.CFS, 'async_info')
        cls.async_open: Callable = create_async_method_fs(cls.CFS, '_open')
        cls.async_mkdir: Callable = create_async_method_fs(cls.CFS, 'async_mkdir')
        cls.async_makedirs: Callable = create_async_method_fs(cls.CFS, 'async_makedirs')
        cls.async_unlink: Callable = create_async_method_fs(cls.CFS, 'async_rm_file')
        cls.async_rmdir: Callable = create_async_method_fs(cls.CFS, 'async_rmdir')
        cls.async_remove: Callable = create_async_method_fs(cls.CFS, 'async_rm')
        cls.async_rm: Callable = create_async_method_fs(cls.CFS, 'async_rm')
        cls.async_listdir: Callable = create_async_method_fs(cls.CFS, ['async_listdir', 'async_list_objects'])



class GCP_CFS(metaclass=CFSType):
    fs: 'gcsfs.GCSFileSystem' = None
    fsa: 'gcsfs.GCSFileSystem' = None
    fs_name: str = 'gcsfs'

class AWS_CFS(metaclass=CFSType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 's3fs'

class Minio_CFS(metaclass=CFSType):
    fs: 's3fs.S3FileSystem' = None
    fsa: 's3fs.S3FileSystem' = None
    fs_name: str = 'minio'


class GCP_Accessor(BaseAccessor):
    """
    GCP Filelike Accessor that inherits from BaseAccessor
    """
    class CFS(GCP_CFS):
        pass

class AWS_Accessor(BaseAccessor):
    """
    AWS Filelike Accessor that inherits from BaseAccessor
    """
    class CFS(AWS_CFS):
        pass

class Minio_Accessor(BaseAccessor):
    """
    S3 Filelike Accessor that inherits from BaseAccessor
    """
    class CFS(Minio_CFS):
        pass

_GCPAccessor: GCP_Accessor = None
_AWSAccessor: AWS_Accessor = None
_MinioAccessor: Minio_Accessor = None

def _get_gcp_accessor(**kwargs) -> GCP_Accessor:
    global _GCPAccessor, GCP_Accessor
    if not _GCPAccessor:
        GCP_CFS.build_filesystems(**kwargs)
        GCP_Accessor.reload_cfs(**kwargs)
        _GCPAccessor = GCP_Accessor()
    return _GCPAccessor

def _get_aws_accessor(**kwargs) -> AWS_Accessor:
    global _AWSAccessor, AWS_Accessor
    if not _AWSAccessor:
        AWS_CFS.build_filesystems(**kwargs)
        AWS_Accessor.reload_cfs(**kwargs)
        _AWSAccessor = AWS_Accessor()
    return _AWSAccessor

def _get_minio_accessor(**kwargs) -> Minio_Accessor:
    global _MinioAccessor, Minio_Accessor
    if not _MinioAccessor:
        Minio_CFS.build_filesystems(**kwargs)
        Minio_Accessor.reload_cfs(**kwargs)
        _MinioAccessor = Minio_Accessor()
    return _MinioAccessor

_accessor_getters = {
    'gs': _get_gcp_accessor,
    's3': _get_aws_accessor,
    'minio': _get_minio_accessor
}
_cfs_getters = {
    'gs': GCP_CFS,
    's3': AWS_CFS,
    'minio': Minio_CFS
}

AccessorLike = Union[
    BaseAccessor,
    GCP_Accessor,
    AWS_Accessor,
    Minio_Accessor
]
CFSLike = Union[
    CFSType,
    GCP_CFS,
    AWS_CFS,
    Minio_CFS
]

def get_accessor(name: str, **kwargs) -> AccessorLike:
    if not _accessor_getters.get(name, None): return BaseAccessor
    return _accessor_getters[name](**kwargs)

def get_cloud_filesystem(name: str) -> Optional[CFSLike]:
    return _cfs_getters.get(name, None)

