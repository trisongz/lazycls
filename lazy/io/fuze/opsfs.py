#from fuse import FUSE
import logging
import os
import stat
import threading
import fuse
import errno
import time
from os import fsencode, fsdecode

from lazy.utils import get_logger

logger = get_logger('FuzeOps')

class FUSEr(fuse.Operations):
    def __init__(self, fs, path, ready_file=False):
        self.fs = fs
        self.cache = {}
        self.root = path.rstrip("/") + "/"
        self.counter = 0
        logger.info("Starting FUSE at %s", path)
        self._ready_file = ready_file

    def getattr(self, path, fh=None):
        logger.debug("getattr %s", path)
        if self._ready_file and path in ["/.fuse_ready", ".fuse_ready", "/.fuze_mounted", ".fuze_mounted"]: return {"type": "file", "st_size": 5}
        path = "".join([self.root, path.lstrip("/")]).rstrip("/")
        try: info = self.fs.info(path)
        except FileNotFoundError: raise fuse.FuseOSError(errno.ENOENT)
        data = {"st_uid": info.get("uid", 1000), "st_gid": info.get("gid", 1000)}
        perm = info.get("mode", 0o777)

        if info["type"] != "file":
            data["st_mode"] = stat.S_IFDIR | perm
            data["st_size"] = 0
            data["st_blksize"] = 0
        else:
            data["st_mode"] = stat.S_IFREG | perm
            data["st_size"] = info["size"]
            data["st_blksize"] = 5 * 2 ** 20
            data["st_nlink"] = 1
        data["st_atime"] = time.time()
        data["st_ctime"] = time.time()
        data["st_mtime"] = time.time()
        return data

    def readdir(self, path, fh):
        logger.debug("readdir %s", path)
        path = "".join([self.root, path.lstrip("/")])
        files = self.fs.ls(path, False)
        files = [os.path.basename(f.rstrip("/")) for f in files]
        return [".", ".."] + files

    def mkdir(self, path, mode):
        path = "".join([self.root, path.lstrip("/")])
        self.fs.mkdir(path)
        return 0

    def rmdir(self, path):
        path = "".join([self.root, path.lstrip("/")])
        self.fs.rmdir(path)
        return 0

    def read(self, path, size, offset, fh):
        logger.debug("read %s", (path, size, offset))
        if self._ready_file and path in ["/.fuse_ready", ".fuse_ready", "/.fuze_mounted", ".fuze_mounted"]: return b"ready"
        f = self.cache[fh]
        f.seek(offset)
        return f.read(size)

    def write(self, path, data, offset, fh):
        logger.debug("write %s", (path, offset))
        f = self.cache[fh]
        f.write(data)
        return len(data)

    def create(self, path, flags, fi=None):
        logger.debug("create %s", (path, flags))
        fn = "".join([self.root, path.lstrip("/")])
        self.fs.touch(fn)  # OS will want to get attributes immediately
        f = self.fs.open(fn, "wb")
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1

    def open(self, path, flags):
        logger.debug("open %s", (path, flags))
        fn = "".join([self.root, path.lstrip("/")])
        if flags % 2 == 0: mode = "rb"
        else: mode = "wb"
        self.cache[self.counter] = self.fs.open(fn, mode)
        self.counter += 1
        return self.counter - 1

    def truncate(self, path, length, fh=None):
        fn = "".join([self.root, path.lstrip("/")])
        if length != 0: raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.fs.touch(fn)

    def unlink(self, path):
        fn = "".join([self.root, path.lstrip("/")])
        try: self.fs.rm(fn, False)
        except (IOError, FileNotFoundError): raise fuse.FuseOSError(errno.EIO)

    def release(self, path, fh):
        try:
            if fh in self.cache:
                f = self.cache[fh]
                f.close()
                self.cache.pop(fh)
        except Exception as e:
            print(e)
        return 0

    def chmod(self, path, mode):
        if hasattr(self.fs, "chmod"):
            path = "".join([self.root, path.lstrip("/")])
            return self.fs.chmod(path, mode)
        raise NotImplementedError


class AsyncFUSEr(fuse.Operations):
    def __init__(self, fs, path, ready_file=False):
        self.fs = fs
        self.cache = {}
        self.root = path.rstrip("/") + "/"
        self.counter = 0
        logger.info("Starting FUSE at %s", path)
        self._ready_file = ready_file

    async def getattr(self, path, fh=None):
        logger.debug("getattr %s", path)
        if self._ready_file and path in ["/.fuse_ready", ".fuse_ready", "/.fuze_mounted", ".fuze_mounted"]: return {"type": "file", "st_size": 5}
        path = "".join([self.root, path.lstrip("/")]).rstrip("/")
        try: info = await self.fs.info(path)
        except FileNotFoundError: raise fuse.FuseOSError(errno.ENOENT)
        data = {"st_uid": info.get("uid", 1000), "st_gid": info.get("gid", 1000)}
        perm = info.get("mode", 0o777)

        if info["type"] != "file":
            data["st_mode"] = stat.S_IFDIR | perm
            data["st_size"] = 0
            data["st_blksize"] = 0
        else:
            data["st_mode"] = stat.S_IFREG | perm
            data["st_size"] = info["size"]
            data["st_blksize"] = 5 * 2 ** 20
            data["st_nlink"] = 1
        data["st_atime"] = time.time()
        data["st_ctime"] = time.time()
        data["st_mtime"] = time.time()
        return data

    async def readdir(self, path, fh):
        logger.debug("readdir %s", path)
        path = "".join([self.root, path.lstrip("/")])
        files = await self.fs.ls(path, False)
        files = [os.path.basename(f.rstrip("/")) for f in files]
        return [".", ".."] + files

    async def mkdir(self, path, mode):
        path = "".join([self.root, path.lstrip("/")])
        await self.fs.mkdir(path)
        return 0

    async def rmdir(self, path):
        path = "".join([self.root, path.lstrip("/")])
        await self.fs.rmdir(path)
        return 0

    async def read(self, path, size, offset, fh):
        logger.debug("read %s", (path, size, offset))
        if self._ready_file and path in ["/.fuse_ready", ".fuse_ready", "/.fuze_mounted", ".fuze_mounted"]: return b"ready"
        f = self.cache[fh]
        await f.seek(offset)
        return await f.read(size)

    async def write(self, path, data, offset, fh):
        logger.debug("write %s", (path, offset))
        f = self.cache[fh]
        await f.write(data)
        return len(data)

    async def create(self, path, flags, fi=None):
        logger.debug("create %s", (path, flags))
        fn = "".join([self.root, path.lstrip("/")])
        await self.fs.touch(fn)  # OS will want to get attributes immediately
        f = await self.fs.open(fn, "wb")
        self.cache[self.counter] = f
        self.counter += 1
        return self.counter - 1

    async def open(self, path, flags):
        logger.debug("open %s", (path, flags))
        fn = "".join([self.root, path.lstrip("/")])
        if flags % 2 == 0: mode = "rb"
        else: mode = "wb"
        self.cache[self.counter] = await self.fs.open(fn, mode)
        self.counter += 1
        return self.counter - 1

    async def truncate(self, path, length, fh=None):
        fn = "".join([self.root, path.lstrip("/")])
        if length != 0: raise NotImplementedError
        # maybe should be no-op since open with write sets size to zero anyway
        self.fs.touch(fn)

    async def unlink(self, path):
        fn = "".join([self.root, path.lstrip("/")])
        try: self.fs.rm(fn, False)
        except (IOError, FileNotFoundError): raise fuse.FuseOSError(errno.EIO)

    async def release(self, path, fh):
        try:
            if fh in self.cache:
                f = self.cache[fh]
                await f.close()
                self.cache.pop(fh)
        except Exception as e:
            print(e)
        return 0

    async def chmod(self, path, mode):
        if hasattr(self.fs, "chmod"):
            path = "".join([self.root, path.lstrip("/")])
            return await self.fs.chmod(path, mode)
        raise NotImplementedError
