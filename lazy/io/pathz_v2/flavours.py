from __future__ import annotations
from pathlib import _PosixFlavour, _WindowsFlavour
from typing import Optional, Callable, Awaitable, Dict, List, TYPE_CHECKING
from errno import EINVAL
import os
import sys
from .aiopathz.wrap import func_to_async_func as wrap_async

try:
    from pathlib import _getfinalpathname
    _async_getfinalpathname = wrap_async(_getfinalpathname)

except ImportError:
    def _getfinalpathname(*args, **kwargs):
        raise ImportError("_getfinalpathname() requires a Windows/NT platform")

    async def _async_getfinalpathname(*args, **kwargs):
        raise ImportError("_getfinalpathname() requires a Windows/NT platform")

if TYPE_CHECKING:  # keep mypy quiet
    from .base import PathzPath, _PathzAccessor


class _AsyncSyncPosixFlavour(_PosixFlavour):
    
    def gethomedir(self, username: str) -> str:
        return super().gethomedir(username)

    async def async_gethomedir(self, username: str) -> str:
        gethomedir: Callable[[str], Awaitable[str]] = wrap_async(super().gethomedir)
        return await gethomedir(username)

    def resolve(self, path: PathzPath, strict: bool = False) -> Optional[str]:
        sep: str = self.sep
        accessor: '_PathzAccessor' = path._accessor
        seen: Dict[str, Optional[str]] = {}

        def _resolve(path: str, rest: str) -> str:
            if rest.startswith(sep): path = ''

            for name in rest.split(sep):
                if not name or name == '.': continue

                if name == '..':
                    # parent dir
                    path, _, _ = path.rpartition(sep)
                    continue

                newpath = path + name if path.endswith(sep) else path + sep + name
                if newpath in seen:
                    # Already seen this path
                    path = seen[newpath]
                    if path is not None: continue

                    # The symlink is not resolved, so we must have a symlink loop.
                    raise RuntimeError(f"Symlink loop from {newpath}")

                # Resolve the symbolic link
                try: target = accessor.readlink(newpath)

                except OSError as e:
                    if e.errno != EINVAL and strict: raise
                    # Not a symlink, or non-strict mode. We just leave the path
                    # untouched.
                    path = newpath
                else:
                    seen[newpath] = None # not resolved symlink
                    path = _resolve(path, target)
                    seen[newpath] = path # resolved symlink

            return path
        
        # NOTE: according to POSIX, getcwd() cannot contain path components
        # which are symlinks.
        base = '' if path.is_absolute() else os.getcwd()
        result = _resolve(base, str(path))
        return result or sep

    async def async_resolve(self, path: PathzPath, strict: bool = False) -> Optional[str]:
        sep: str = self.sep
        accessor: '_PathzAccessor' = path._accessor
        seen: Dict[str, Optional[str]] = {}

        async def _resolve(path: str, rest: str) -> str:
            if rest.startswith(sep): path = ''

            for name in rest.split(sep):
                if not name or name == '.': continue

                if name == '..':
                    # parent dir
                    path, _, _ = path.rpartition(sep)
                    continue

                newpath = path + name if path.endswith(sep) else path + sep + name
                if newpath in seen:
                    # Already seen this path
                    path = seen[newpath]
                    if path is not None: continue

                    # The symlink is not resolved, so we must have a symlink loop.
                    raise RuntimeError(f"Symlink loop from {newpath}")

                # Resolve the symbolic link
                try: target = await accessor.async_readlink(newpath)

                except OSError as e:
                    if e.errno != EINVAL and strict: raise
                    # Not a symlink, or non-strict mode. We just leave the path
                    # untouched.
                    path = newpath
                else:
                    seen[newpath] = None # not resolved symlink
                    path = await _resolve(path, target)
                    seen[newpath] = path # resolved symlink

            return path
        
        # NOTE: according to POSIX, getcwd() cannot contain path components
        # which are symlinks.
        base = '' if path.is_absolute() else os.getcwd()
        result = await _resolve(base, str(path))
        return result or sep


class _AsyncSyncWindowsFlavour(_WindowsFlavour):
    def gethomedir(self, username: str) -> str: 
        return super().gethomedir(username)

    async def async_gethomedir(self, username: str) -> str: 
        gethomedir: Callable[[str], Awaitable[str]] = wrap_async(super().gethomedir)
        return await gethomedir(username)

    def resolve(self, path: 'PathzPath', strict: bool = False) -> Optional[str]:
        s = str(path)

        if not s: return os.getcwd()

        previous_s: Optional[str] = None
        if _getfinalpathname is not None:
            if strict: return self._ext_to_normal(_getfinalpathname(s))
      
        else:
            tail_parts: List[str] = []  # End of the path after the first one not found
            while True:
                try: s = self._ext_to_normal(_getfinalpathname(s))
                except FileNotFoundError:
                    previous_s = s
                    s, tail = os.path.split(s)
                    tail_parts.append(tail)
                    if previous_s == s: return path
                else: return os.path.join(s, *reversed(tail_parts))
        return None
    
    async def async_resolve(self, path: '_PathzAccessor', strict: bool = False) -> Optional[str]:
        s = str(path)

        if not s: return os.getcwd()

        previous_s: Optional[str] = None
        if _async_getfinalpathname is not None:
            if strict: return self._ext_to_normal(await _async_getfinalpathname(s))
      
        else:
            tail_parts: List[str] = []  # End of the path after the first one not found
            while True:
                try: s = self._ext_to_normal(await _async_getfinalpathname(s))
                except FileNotFoundError:
                    previous_s = s
                    s, tail = os.path.split(s)
                    tail_parts.append(tail)
                    if previous_s == s: return path
                else: return os.path.join(s, *reversed(tail_parts))
        return None


_async_sync_windows_flavour = _AsyncSyncWindowsFlavour()
_async_sync_posix_flavour = _AsyncSyncPosixFlavour()
