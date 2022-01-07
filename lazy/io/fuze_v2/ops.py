
"""
Modified version from fusepy
"""

import sys
import ctypes
import errno
import os
import warnings
import functools
from signal import signal, SIGINT, SIG_DFL
from stat import S_IFDIR
import inspect

try:
    from functools import partial
except ImportError:
    # http://docs.python.org/library/functools.html#functools.partial
    def partial(func, *args, **keywords):
        def newfunc(*fargs, **fkeywords):
            newkeywords = keywords.copy()
            newkeywords.update(fkeywords)
            return func(*(args + fargs), **newkeywords)

        newfunc.func = func
        newfunc.args = args
        newfunc.keywords = keywords
        return newfunc


from typing import Callable, Dict, Any
from types import ModuleType
from lazy.libz import Lib
from lazy.utils import get_logger

logger = get_logger('Fuze')


if Lib.is_avail_fuse:
    import fuse
    _FUZE_READY = True
else: 
    fuse = ModuleType
    _FUZE_READY = False

_FUZE_ALLOWED: bool = not sys.platform.startswith('win')

def _prepare_fuze():
    global _FUZE_READY, fuse
    assert _FUZE_ALLOWED, 'Windows is not supported'
    if _FUZE_READY: return
    
    Lib._ensure_binary_installed('fuse')
    fuse = Lib.import_lib('fuse', 'fusepy')

    _FUZE_READY = True

def async_wrapper(fn):
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        await fn(*args, **kwargs)
    return wrapper


def iscoroutinefunction(obj):
    if inspect.iscoroutinefunction(obj): return True
    if hasattr(obj, '__call__') and inspect.iscoroutinefunction(obj.__call__): return True
    return False


class FuseOSError(OSError):
    def __init__(self, errno):
        super(FuseOSError, self).__init__(errno, os.strerror(errno))

class FUZE(object):
    '''
    This class is the lower level interface and should not be subclassed under
    normal use. Its methods are called by fuse.

    Assumes API version 2.6 or later.
    '''

    OPTIONS = (
        ('foreground', '-f'),
        ('debug', '-d'),
        ('nothreads', '-s'),
    )

    def __init__(self, operations, mountpoint, raw_fi=False, encoding='utf-8', **kwargs):

        '''
        Setting raw_fi to True will cause FUSE to pass the fuse_file_info
        class as is to Operations, instead of just the fh field.

        This gives you access to direct_io, keep_cache, etc.
        '''
        _prepare_fuze()
        self.operations = operations
        self.raw_fi = raw_fi
        self.encoding = encoding
        self.__critical_exception = None

        self.use_ns = getattr(operations, 'use_ns', False)
        if not self.use_ns:
            warnings.warn(
                'Time as floating point seconds for utimens is deprecated!\n'
                'To enable time as nanoseconds set the property "use_ns" to '
                'True in your operations class or set your fusepy '
                'requirements to <4.',
                DeprecationWarning)

        args = ['fuse']
        args.extend(flag for arg, flag in self.OPTIONS if kwargs.pop(arg, False))
        kwargs.setdefault('fsname', operations.__class__.__name__)
        args.append('-o')
        args.append(','.join(self._normalize_fuse_options(**kwargs)))
        args.append(mountpoint)

        args = [arg.encode(encoding) for arg in args]
        argv = (ctypes.c_char_p * len(args))(*args)

        fuse_ops = fuse.fuse_operations()
        for ent in fuse.fuse_operations._fields_:
            name, prototype = ent[:2]
            check_name = name
            # ftruncate()/fgetattr() are implemented in terms of their
            # non-f-prefixed versions in the operations object
            if check_name in ["ftruncate", "fgetattr"]: check_name = check_name[1:]

            val = getattr(operations, check_name, None)
            if val is None: continue

            # Function pointer members are tested for using the
            # getattr(operations, name) above but are dynamically
            # invoked using self.operations(name)
            if hasattr(prototype, 'argtypes'):
                val = prototype(partial(self._wrapper, getattr(self, name)))

            setattr(fuse_ops, name, val)

        try: old_handler = signal(SIGINT, SIG_DFL)
        except ValueError: old_handler = SIG_DFL

        err = fuse._libfuse.fuse_main_real(len(args), argv, ctypes.pointer(fuse_ops), ctypes.sizeof(fuse_ops), None)
        try: signal(SIGINT, old_handler)
        except ValueError: pass

        del self.operations     # Invoke the destructor
        if self.__critical_exception: raise self.__critical_exception
        if err: raise RuntimeError(err)

    @staticmethod
    def _normalize_fuse_options(**kargs):
        for key, value in kargs.items():
            if isinstance(value, bool):
                if value is True:
                    yield key
            else:
                yield '%s=%s' % (key, value)

    @staticmethod
    def _wrapper(func, *args, **kwargs):
        'Decorator for the methods that follow'

        try:
            if func.__name__ == "init":
                # init may not fail, as its return code is just stored as
                # private_data field of struct fuse_context
                return func(*args, **kwargs) or 0

            else:
                try:
                    return func(*args, **kwargs) or 0

                except OSError as e:
                    if e.errno > 0:
                        logger.debug("FUSE operation %s raised a %s, returning errno %s.", func.__name__, type(e), e.errno, exc_info=True)
                        return -e.errno
                    else:
                        logger.error("FUSE operation %s raised an OSError with negative errno %s, returning errno.EINVAL.", func.__name__, e.errno, exc_info=True)
                        return -errno.EINVAL

                except Exception:
                    logger.error("Uncaught exception from FUSE operation %s, returning errno.EINVAL.", func.__name__, exc_info=True)
                    return -errno.EINVAL

        except BaseException as e:
            self.__critical_exception = e
            logger.critical("Uncaught critical exception from FUSE operation %s, aborting.", func.__name__, exc_info=True)
            # the raised exception (even SystemExit) will be caught by FUSE
            # potentially causing SIGSEGV, so tell system to stop/interrupt FUSE
            fuse.fuse_exit()
            return -errno.EFAULT

    def _decode_optional_path(self, path):
        # NB: this method is intended for fuse operations that
        #     allow the path argument to be NULL,
        #     *not* as a generic path decoding method
        if path is None: return None
        return path.decode(self.encoding)

    def getattr(self, path, buf):
        return self.fgetattr(path, buf, None)

    def readlink(self, path, buf, bufsize):
        ret = self.operations('readlink', path.decode(self.encoding)).encode(self.encoding)
        # copies a string into the given buffer
        # (null terminated and truncated if necessary)
        data = ctypes.create_string_buffer(ret[:bufsize - 1])
        ctypes.memmove(buf, data, len(data))
        return 0

    def mknod(self, path, mode, dev):
        return self.operations('mknod', path.decode(self.encoding), mode, dev)

    def mkdir(self, path, mode):
        return self.operations('mkdir', path.decode(self.encoding), mode)

    def unlink(self, path):
        return self.operations('unlink', path.decode(self.encoding))

    def rmdir(self, path):
        return self.operations('rmdir', path.decode(self.encoding))

    def symlink(self, source, target):
        'creates a symlink `target -> source` (e.g. ln -s source target)'

        return self.operations('symlink', target.decode(self.encoding), source.decode(self.encoding))

    def rename(self, old, new):
        return self.operations('rename', old.decode(self.encoding), new.decode(self.encoding))

    def link(self, source, target):
        'creates a hard link `target -> source` (e.g. ln source target)'
        return self.operations('link', target.decode(self.encoding), source.decode(self.encoding))

    def chmod(self, path, mode):
        return self.operations('chmod', path.decode(self.encoding), mode)

    def chown(self, path, uid, gid):
        # Check if any of the arguments is a -1 that has overflowed
        if fuse.c_uid_t(uid + 1).value == 0: uid = -1
        if fuse.c_gid_t(gid + 1).value == 0: gid = -1
        return self.operations('chown', path.decode(self.encoding), uid, gid)

    def truncate(self, path, length):
        return self.operations('truncate', path.decode(self.encoding), length)

    def open(self, path, fip):
        fi = fip.contents
        if self.raw_fi: 
            return self.operations('open', path.decode(self.encoding), fi)
        else:
            fi.fh = self.operations('open', path.decode(self.encoding), fi.flags)
            return 0

    def read(self, path, buf, size, offset, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        ret = self.operations('read', self._decode_optional_path(path), size, offset, fh)

        if not ret: return 0

        retsize = len(ret)
        assert retsize <= size, 'actual amount read %d greater than expected %d' % (retsize, size)
        ctypes.memmove(buf, ret, retsize)
        return retsize

    def write(self, path, buf, size, offset, fip):
        data = ctypes.string_at(buf, size)

        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('write', self._decode_optional_path(path), data, offset, fh)

    def statfs(self, path, buf):
        stv = buf.contents
        attrs = self.operations('statfs', path.decode(self.encoding))
        for key, val in attrs.items():
            if hasattr(stv, key): setattr(stv, key, val)
        return 0

    def flush(self, path, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('flush', self._decode_optional_path(path), fh)

    def release(self, path, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('release', self._decode_optional_path(path), fh)

    def fsync(self, path, datasync, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('fsync', self._decode_optional_path(path), datasync, fh)

    def setxattr(self, path, name, value, size, options, *args):
        return self.operations('setxattr', path.decode(self.encoding), name.decode(self.encoding), ctypes.string_at(value, size), options, *args)

    def getxattr(self, path, name, value, size, *args):
        ret = self.operations('getxattr', path.decode(self.encoding), name.decode(self.encoding), *args)
        retsize = len(ret)
        # allow size queries
        if not value: return retsize

        # do not truncate
        if retsize > size: return -errno.ERANGE

        # Does not add trailing 0
        buf = ctypes.create_string_buffer(ret, retsize)
        ctypes.memmove(value, buf, retsize)

        return retsize

    def listxattr(self, path, namebuf, size):
        attrs = self.operations('listxattr', path.decode(self.encoding)) or ''
        ret = '\x00'.join(attrs).encode(self.encoding)
        if len(ret) > 0: ret += '\x00'.encode(self.encoding)

        retsize = len(ret)
        # allow size queries
        if not namebuf: return retsize

        # do not truncate
        if retsize > size: return -errno.ERANGE

        buf = ctypes.create_string_buffer(ret, retsize)
        ctypes.memmove(namebuf, buf, retsize)

        return retsize

    def removexattr(self, path, name):
        return self.operations('removexattr', path.decode(self.encoding), name.decode(self.encoding))

    def opendir(self, path, fip):
        # Ignore raw_fi
        fip.contents.fh = self.operations('opendir', path.decode(self.encoding))
        return 0

    def readdir(self, path, buf, filler, offset, fip):
        # Ignore raw_fi
        for item in self.operations('readdir', self._decode_optional_path(path), fip.contents.fh):
            if isinstance(item, fuse.basestring): name, st, offset = item, None, 0
            else:
                name, attrs, offset = item
                if attrs:
                    st = fuse.c_stat()
                    fuse.set_st_attrs(st, attrs, use_ns=self.use_ns)
                else:
                    st = None

            if filler(buf, name.encode(self.encoding), st, offset) != 0: break

        return 0

    def releasedir(self, path, fip):
        # Ignore raw_fi
        return self.operations('releasedir', self._decode_optional_path(path), fip.contents.fh)

    def fsyncdir(self, path, datasync, fip):
        # Ignore raw_fi
        return self.operations('fsyncdir', self._decode_optional_path(path), datasync, fip.contents.fh)

    def init(self, conn):
        return self.operations('init', '/')

    def destroy(self, private_data):
        return self.operations('destroy', '/')

    def access(self, path, amode):
        return self.operations('access', path.decode(self.encoding), amode)

    def create(self, path, mode, fip):
        fi = fip.contents
        path = path.decode(self.encoding)

        if self.raw_fi: return self.operations('create', path, mode, fi)
        fi.fh = self.operations('create', path, mode)
        return 0

    def ftruncate(self, path, length, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('truncate', self._decode_optional_path(path), length, fh)

    def fgetattr(self, path, buf, fip):
        ctypes.memset(buf, 0, ctypes.sizeof(fuse.c_stat))

        st = buf.contents
        if not fip: fh = fip
        elif self.raw_fi: fh = fip.contents
        else: fh = fip.contents.fh

        attrs = self.operations('getattr', self._decode_optional_path(path), fh)
        fuse.set_st_attrs(st, attrs, use_ns=self.use_ns)
        return 0

    def lock(self, path, fip, cmd, lock):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('lock', self._decode_optional_path(path), fh, cmd, lock)

    def utimens(self, path, buf):
        if buf:
            atime = fuse.time_of_timespec(buf.contents.actime, use_ns=self.use_ns)
            mtime = fuse.time_of_timespec(buf.contents.modtime, use_ns=self.use_ns)
            times = (atime, mtime)
        else:
            times = None

        return self.operations('utimens', path.decode(self.encoding), times)

    def bmap(self, path, blocksize, idx):
        return self.operations('bmap', path.decode(self.encoding), blocksize, idx)

    def ioctl(self, path, cmd, arg, fip, flags, data):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('ioctl', path.decode(self.encoding), cmd, arg, fh, flags, data)


class AsyncFUZE(object):
    '''
    This class is the lower level interface and should not be subclassed under
    normal use. Its methods are called by fuse.

    Assumes API version 2.6 or later.
    '''

    OPTIONS = (
        ('foreground', '-f'),
        ('debug', '-d'),
        ('nothreads', '-s'),
    )

    def __init__(self, operations, mountpoint, raw_fi=False, encoding='utf-8', **kwargs):

        '''
        Setting raw_fi to True will cause FUSE to pass the fuse_file_info
        class as is to Operations, instead of just the fh field.

        This gives you access to direct_io, keep_cache, etc.
        '''
        _prepare_fuze()
        self.operations = operations
        self.raw_fi = raw_fi
        self.encoding = encoding
        self.__critical_exception = None

        self.use_ns = getattr(operations, 'use_ns', False)
        if not self.use_ns:
            warnings.warn(
                'Time as floating point seconds for utimens is deprecated!\n'
                'To enable time as nanoseconds set the property "use_ns" to '
                'True in your operations class or set your fusepy '
                'requirements to <4.',
                DeprecationWarning)

        args = ['fuse']
        args.extend(flag for arg, flag in self.OPTIONS if kwargs.pop(arg, False))
        kwargs.setdefault('fsname', operations.__class__.__name__)
        args.append('-o')
        args.append(','.join(self._normalize_fuse_options(**kwargs)))
        args.append(mountpoint)

        args = [arg.encode(encoding) for arg in args]
        argv = (ctypes.c_char_p * len(args))(*args)

        fuse_ops = fuse.fuse_operations()
        for ent in fuse.fuse_operations._fields_:
            name, prototype = ent[:2]
            check_name = name
            # ftruncate()/fgetattr() are implemented in terms of their
            # non-f-prefixed versions in the operations object
            if check_name in ["ftruncate", "fgetattr"]: check_name = check_name[1:]

            val = getattr(operations, check_name, None)
            if val is None: continue

            # Function pointer members are tested for using the
            # getattr(operations, name) above but are dynamically
            # invoked using self.operations(name)
            if hasattr(prototype, 'argtypes'):
                val = prototype(partial(self._wrapper, getattr(self, name)))

            setattr(fuse_ops, name, val)

        try: old_handler = signal(SIGINT, SIG_DFL)
        except ValueError: old_handler = SIG_DFL

        err = fuse._libfuse.fuse_main_real(len(args), argv, ctypes.pointer(fuse_ops), ctypes.sizeof(fuse_ops), None)
        try: signal(SIGINT, old_handler)
        except ValueError: pass

        del self.operations     # Invoke the destructor
        if self.__critical_exception: raise self.__critical_exception
        if err: raise RuntimeError(err)

    @staticmethod
    def _normalize_fuse_options(**kargs):
        for key, value in kargs.items():
            if isinstance(value, bool):
                if value is True:
                    yield key
            else:
                yield '%s=%s' % (key, value)

    @staticmethod
    async def _wrapper(func, *args, **kwargs):
        'Decorator for the methods that follow'

        try:
            if func.__name__ == "init":
                # init may not fail, as its return code is just stored as
                # private_data field of struct fuse_context
                if iscoroutinefunction(func): return await func(*args, **kwargs) or 0
                return func(*args, **kwargs) or 0

            else:
                try:
                    if iscoroutinefunction(func): return await func(*args, **kwargs) or 0
                    return func(*args, **kwargs) or 0

                except OSError as e:
                    if e.errno > 0:
                        logger.debug("FUSE operation %s raised a %s, returning errno %s.", func.__name__, type(e), e.errno, exc_info=True)
                        return -e.errno
                    else:
                        logger.error("FUSE operation %s raised an OSError with negative errno %s, returning errno.EINVAL.", func.__name__, e.errno, exc_info=True)
                        return -errno.EINVAL

                except Exception:
                    logger.error("Uncaught exception from FUSE operation %s, returning errno.EINVAL.", func.__name__, exc_info=True)
                    return -errno.EINVAL

        except BaseException as e:
            self.__critical_exception = e
            logger.critical("Uncaught critical exception from FUSE operation %s, aborting.", func.__name__, exc_info=True)
            # the raised exception (even SystemExit) will be caught by FUSE
            # potentially causing SIGSEGV, so tell system to stop/interrupt FUSE
            fuse.fuse_exit()
            return -errno.EFAULT

    def _decode_optional_path(self, path):
        # NB: this method is intended for fuse operations that
        #     allow the path argument to be NULL,
        #     *not* as a generic path decoding method
        if path is None: return None
        return path.decode(self.encoding)

    async def getattr(self, path, buf):
        return await self.fgetattr(path, buf, None)

    async def readlink(self, path, buf, bufsize):
        ret = self.operations('readlink', path.decode(self.encoding)).encode(self.encoding)
        # copies a string into the given buffer
        # (null terminated and truncated if necessary)
        data = ctypes.create_string_buffer(ret[:bufsize - 1])
        ctypes.memmove(buf, data, len(data))
        return 0

    async def mknod(self, path, mode, dev):
        return await self.operations('mknod', path.decode(self.encoding), mode, dev)

    async def mkdir(self, path, mode):
        return await self.operations('mkdir', path.decode(self.encoding), mode)

    async def unlink(self, path):
        return await self.operations('unlink', path.decode(self.encoding))

    async def rmdir(self, path):
        return await self.operations('rmdir', path.decode(self.encoding))

    async def symlink(self, source, target):
        'creates a symlink `target -> source` (e.g. ln -s source target)'

        return await self.operations('symlink', target.decode(self.encoding), source.decode(self.encoding))

    async def rename(self, old, new):
        return await self.operations('rename', old.decode(self.encoding), new.decode(self.encoding))

    async def link(self, source, target):
        'creates a hard link `target -> source` (e.g. ln source target)'
        return await self.operations('link', target.decode(self.encoding), source.decode(self.encoding))

    async def chmod(self, path, mode):
        return await self.operations('chmod', path.decode(self.encoding), mode)

    async def chown(self, path, uid, gid):
        # Check if any of the arguments is a -1 that has overflowed
        if fuse.c_uid_t(uid + 1).value == 0: uid = -1
        if fuse.c_gid_t(gid + 1).value == 0: gid = -1
        return await self.operations('chown', path.decode(self.encoding), uid, gid)

    async def truncate(self, path, length):
        return await self.operations('truncate', path.decode(self.encoding), length)

    async def open(self, path, fip):
        fi = fip.contents
        if self.raw_fi: 
            return await self.operations('open', path.decode(self.encoding), fi)
        else:
            fi.fh = await self.operations('open', path.decode(self.encoding), fi.flags)
            return 0

    async def read(self, path, buf, size, offset, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        ret = await self.operations('read', self._decode_optional_path(path), size, offset, fh)

        if not ret: return 0

        retsize = len(ret)
        assert retsize <= size, 'actual amount read %d greater than expected %d' % (retsize, size)
        ctypes.memmove(buf, ret, retsize)
        return retsize

    async def write(self, path, buf, size, offset, fip):
        data = ctypes.string_at(buf, size)

        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('write', self._decode_optional_path(path), data, offset, fh)

    async def statfs(self, path, buf):
        stv = buf.contents
        attrs = await self.operations('statfs', path.decode(self.encoding))
        for key, val in attrs.items():
            if hasattr(stv, key): setattr(stv, key, val)
        return 0

    async def flush(self, path, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('flush', self._decode_optional_path(path), fh)

    async def release(self, path, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('release', self._decode_optional_path(path), fh)

    async def fsync(self, path, datasync, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('fsync', self._decode_optional_path(path), datasync, fh)

    async def setxattr(self, path, name, value, size, options, *args):
        return await self.operations('setxattr', path.decode(self.encoding), name.decode(self.encoding), ctypes.string_at(value, size), options, *args)

    async def getxattr(self, path, name, value, size, *args):
        ret = await self.operations('getxattr', path.decode(self.encoding), name.decode(self.encoding), *args)
        retsize = len(ret)
        # allow size queries
        if not value: return retsize

        # do not truncate
        if retsize > size: return -errno.ERANGE

        # Does not add trailing 0
        buf = ctypes.create_string_buffer(ret, retsize)
        ctypes.memmove(value, buf, retsize)

        return retsize

    async def listxattr(self, path, namebuf, size):
        attrs = await self.operations('listxattr', path.decode(self.encoding)) or ''
        ret = '\x00'.join(attrs).encode(self.encoding)
        if len(ret) > 0: ret += '\x00'.encode(self.encoding)

        retsize = len(ret)
        # allow size queries
        if not namebuf: return retsize

        # do not truncate
        if retsize > size: return -errno.ERANGE

        buf = ctypes.create_string_buffer(ret, retsize)
        ctypes.memmove(namebuf, buf, retsize)

        return retsize

    async def removexattr(self, path, name):
        return await self.operations('removexattr', path.decode(self.encoding), name.decode(self.encoding))

    async def opendir(self, path, fip):
        # Ignore raw_fi
        fip.contents.fh = await self.operations('opendir', path.decode(self.encoding))
        return 0

    async def readdir(self, path, buf, filler, offset, fip):
        # Ignore raw_fi
        for item in await self.operations('readdir', self._decode_optional_path(path), fip.contents.fh):
            if isinstance(item, fuse.basestring): name, st, offset = item, None, 0
            else:
                name, attrs, offset = item
                if attrs:
                    st = fuse.c_stat()
                    fuse.set_st_attrs(st, attrs, use_ns=self.use_ns)
                else:
                    st = None

            if filler(buf, name.encode(self.encoding), st, offset) != 0: break

        return 0

    async def releasedir(self, path, fip):
        # Ignore raw_fi
        return await self.operations('releasedir', self._decode_optional_path(path), fip.contents.fh)

    async def fsyncdir(self, path, datasync, fip):
        # Ignore raw_fi
        return await self.operations('fsyncdir', self._decode_optional_path(path), datasync, fip.contents.fh)

    async def init(self, conn):
        return await self.operations('init', '/')

    async def destroy(self, private_data):
        return await self.operations('destroy', '/')

    async def access(self, path, amode):
        return await self.operations('access', path.decode(self.encoding), amode)

    async def create(self, path, mode, fip):
        fi = fip.contents
        path = path.decode(self.encoding)

        if self.raw_fi: return await self.operations('create', path, mode, fi)
        fi.fh = await self.operations('create', path, mode)
        return 0

    async def ftruncate(self, path, length, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('truncate', self._decode_optional_path(path), length, fh)

    async def fgetattr(self, path, buf, fip):
        ctypes.memset(buf, 0, ctypes.sizeof(fuse.c_stat))

        st = buf.contents
        if not fip: fh = fip
        elif self.raw_fi: fh = fip.contents
        else: fh = fip.contents.fh

        attrs = await self.operations('getattr', self._decode_optional_path(path), fh)
        fuse.set_st_attrs(st, attrs, use_ns=self.use_ns)
        return 0

    async def lock(self, path, fip, cmd, lock):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('lock', self._decode_optional_path(path), fh, cmd, lock)

    async def utimens(self, path, buf):
        if buf:
            atime = fuse.time_of_timespec(buf.contents.actime, use_ns=self.use_ns)
            mtime = fuse.time_of_timespec(buf.contents.modtime, use_ns=self.use_ns)
            times = (atime, mtime)
        else:
            times = None

        return await self.operations('utimens', path.decode(self.encoding), times)

    async def bmap(self, path, blocksize, idx):
        return await self.operations('bmap', path.decode(self.encoding), blocksize, idx)

    async def ioctl(self, path, cmd, arg, fip, flags, data):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return await self.operations('ioctl', path.decode(self.encoding), cmd, arg, fh, flags, data)
    
    


class Operations(object):
    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    def __call__(self, op, *args):
        if not hasattr(self, op):
            raise FuseOSError(errno.EFAULT)
        return getattr(self, op)(*args)

    def access(self, path, amode):
        return 0

    bmap = None

    def chmod(self, path, mode):
        raise FuseOSError(errno.EROFS)

    def chown(self, path, uid, gid):
        raise FuseOSError(errno.EROFS)

    def create(self, path, mode, fi=None):
        '''
        When raw_fi is False (default case), fi is None and create should
        return a numerical file handle.

        When raw_fi is True the file handle should be set directly by create
        and return 0.
        '''

        raise FuseOSError(errno.EROFS)

    def destroy(self, path):
        'Called on filesystem destruction. Path is always /'

        pass

    def flush(self, path, fh):
        return 0

    def fsync(self, path, datasync, fh):
        return 0

    def fsyncdir(self, path, datasync, fh):
        return 0

    def getattr(self, path, fh=None):
        '''
        Returns a dictionary with keys identical to the stat C structure of
        stat(2).

        st_atime, st_mtime and st_ctime should be floats.

        NOTE: There is an incompatibility between Linux and Mac OS X
        concerning st_nlink of directories. Mac OS X counts all files inside
        the directory, while Linux counts only the subdirectories.
        '''

        if path != '/':
            raise FuseOSError(errno.ENOENT)
        return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)

    def getxattr(self, path, name, position=0):
        raise FuseOSError(fuse.ENOTSUP)

    def init(self, path):
        '''
        Called on filesystem initialization. (Path is always /)

        Use it instead of __init__ if you start threads on initialization.
        '''

        pass

    def ioctl(self, path, cmd, arg, fip, flags, data):
        raise FuseOSError(errno.ENOTTY)

    def link(self, target, source):
        'creates a hard link `target -> source` (e.g. ln source target)'

        raise FuseOSError(errno.EROFS)

    def listxattr(self, path):
        return []

    lock = None

    def mkdir(self, path, mode):
        raise FuseOSError(errno.EROFS)

    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EROFS)

    def open(self, path, flags):
        '''
        When raw_fi is False (default case), open should return a numerical
        file handle.

        When raw_fi is True the signature of open becomes:
            open(self, path, fi)

        and the file handle should be set directly.
        '''

        return 0

    def opendir(self, path):
        'Returns a numerical file handle.'

        return 0

    def read(self, path, size, offset, fh):
        'Returns a string containing the data requested.'

        raise FuseOSError(errno.EIO)

    def readdir(self, path, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''

        return ['.', '..']

    def readlink(self, path):
        raise FuseOSError(errno.ENOENT)

    def release(self, path, fh):
        return 0

    def releasedir(self, path, fh):
        return 0

    def removexattr(self, path, name):
        raise FuseOSError(fuse.ENOTSUP)

    def rename(self, old, new):
        raise FuseOSError(errno.EROFS)

    def rmdir(self, path):
        raise FuseOSError(errno.EROFS)

    def setxattr(self, path, name, value, options, position=0):
        raise FuseOSError(fuse.ENOTSUP)

    def statfs(self, path):
        '''
        Returns a dictionary with keys identical to the statvfs C structure of
        statvfs(3).

        On Mac OS X f_bsize and f_frsize must be a power of 2
        (minimum 512).
        '''

        return {}

    def symlink(self, target, source):
        'creates a symlink `target -> source` (e.g. ln -s source target)'

        raise FuseOSError(errno.EROFS)

    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.EROFS)

    def unlink(self, path):
        raise FuseOSError(errno.EROFS)

    def utimens(self, path, times=None):
        'Times is a (atime, mtime) tuple. If None use current time.'

        return 0

    def write(self, path, data, offset, fh):
        raise FuseOSError(errno.EROFS)

class AsyncOperations(object):
    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.
    '''

    async def __call__(self, op, *args):
        if not hasattr(self, op):
            raise FuseOSError(errno.EFAULT)
        a = getattr(self, op)
        if iscoroutinefunction(a): return a(*args)
        return await a(*args)

    async def access(self, path, amode):
        return 0

    bmap = None

    async def chmod(self, path, mode):
        raise FuseOSError(errno.EROFS)

    async def chown(self, path, uid, gid):
        raise FuseOSError(errno.EROFS)

    async def create(self, path, mode, fi=None):
        '''
        When raw_fi is False (default case), fi is None and create should
        return a numerical file handle.

        When raw_fi is True the file handle should be set directly by create
        and return 0.
        '''

        raise FuseOSError(errno.EROFS)

    async def destroy(self, path):
        'Called on filesystem destruction. Path is always /'

        pass

    async def flush(self, path, fh):
        return 0

    async def fsync(self, path, datasync, fh):
        return 0

    async def fsyncdir(self, path, datasync, fh):
        return 0

    async def getattr(self, path, fh=None):
        '''
        Returns a dictionary with keys identical to the stat C structure of
        stat(2).

        st_atime, st_mtime and st_ctime should be floats.

        NOTE: There is an incompatibility between Linux and Mac OS X
        concerning st_nlink of directories. Mac OS X counts all files inside
        the directory, while Linux counts only the subdirectories.
        '''

        if path != '/':
            raise FuseOSError(errno.ENOENT)
        return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)

    async def getxattr(self, path, name, position=0):
        raise FuseOSError(fuse.ENOTSUP)

    async def init(self, path):
        '''
        Called on filesystem initialization. (Path is always /)

        Use it instead of __init__ if you start threads on initialization.
        '''

        pass

    async def ioctl(self, path, cmd, arg, fip, flags, data):
        raise FuseOSError(errno.ENOTTY)

    async def link(self, target, source):
        'creates a hard link `target -> source` (e.g. ln source target)'

        raise FuseOSError(errno.EROFS)

    async def listxattr(self, path):
        return []

    lock = None

    async def mkdir(self, path, mode):
        raise FuseOSError(errno.EROFS)

    async def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EROFS)

    async def open(self, path, flags):
        '''
        When raw_fi is False (default case), open should return a numerical
        file handle.

        When raw_fi is True the signature of open becomes:
            open(self, path, fi)

        and the file handle should be set directly.
        '''

        return 0

    async def opendir(self, path):
        'Returns a numerical file handle.'

        return 0

    async def read(self, path, size, offset, fh):
        'Returns a string containing the data requested.'

        raise FuseOSError(errno.EIO)

    async def readdir(self, path, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''

        return ['.', '..']

    async def readlink(self, path):
        raise FuseOSError(errno.ENOENT)

    async def release(self, path, fh):
        return 0

    async def releasedir(self, path, fh):
        return 0

    async def removexattr(self, path, name):
        raise FuseOSError(fuse.ENOTSUP)

    async def rename(self, old, new):
        raise FuseOSError(errno.EROFS)

    async def rmdir(self, path):
        raise FuseOSError(errno.EROFS)

    async def setxattr(self, path, name, value, options, position=0):
        raise FuseOSError(fuse.ENOTSUP)

    async def statfs(self, path):
        '''
        Returns a dictionary with keys identical to the statvfs C structure of
        statvfs(3).

        On Mac OS X f_bsize and f_frsize must be a power of 2
        (minimum 512).
        '''

        return {}

    async def symlink(self, target, source):
        'creates a symlink `target -> source` (e.g. ln -s source target)'

        raise FuseOSError(errno.EROFS)

    async def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.EROFS)

    async def unlink(self, path):
        raise FuseOSError(errno.EROFS)

    async def utimens(self, path, times=None):
        'Times is a (atime, mtime) tuple. If None use current time.'

        return 0

    async def write(self, path, data, offset, fh):
        raise FuseOSError(errno.EROFS)
