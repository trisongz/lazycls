"""
Extended Classes based off of 
https://github.com/kbairak/pipepy/blob/master/src/pipepy/misc.py
"""

import os
import re
from .base import *

class cd:
    """ `cd` replacement that can be also used as a context processor.
        Equivalent to `os.chdir`:
            >>> from lazycls.ext._cmd.contrib import cd, pwd
            >>> print(pwd())
            <<< /foo
            >>> cd('bar')
            >>> print(pwd())
            <<< /foo/bar
            >>> cd('..')
            >>> print(pwd())
            <<< /foo
        Usage as context processor
            >>> from lazycls.ext._cmd.contrib import cd, pwd
            >>> print(pwd())
            <<< /foo
            >>> with cd('bar'):
            ...     print(pwd())
            <<< /foo/bar
            >>> print(pwd())
            <<< /foo
    """

    def __init__(self, *args, **kwargs):
        self._previous_dir = os.path.abspath(os.curdir)
        os.chdir(*args, **kwargs)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        os.chdir(self._previous_dir)


class export:
    """ `export` replacement that can also be used as a context processor.
        Equivalent to `export`:
            >>> import os
            >>> from lazycls.ext._cmd.contrib import export
            >>> print(os.environ['PATH'])
            <<< foo
            >>> export(PATH="foo:bar")
            >>> print(os.environ['PATH'])
            <<< foo:bar
            >>> export(PATH="foo")
            >>> print(os.environ['PATH'])
            <<< foo
        Usage as a context processor:
            >>> import os
            >>> from lazycls.ext._cmd.contrib import export
            >>> print(os.environ['PATH'])
            <<< foo
            >>> with export(PATH="foo:bar"):
            ...     print(os.environ['PATH'])
            <<< foo:bar
            >>> print(os.environ['PATH'])
            <<< foo
        If an env variable is further changed within the body of `with`, it is
        not restored.
            >>> with export(PATH="foo:bar"):
            ...     export(PATH="foo:BAR")
            >>> print(os.environ['PATH'])
            <<< foo:BAR
    """

    def __init__(self, **kwargs):
        self._previous_env = dict(os.environ)
        self._kwargs = kwargs
        os.environ.update(kwargs)

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        for key, value in self._kwargs.items():
            if os.environ[key] != value:
                # Variable changed within the body of the `with` block, skip
                pass
            elif key in self._previous_env:
                # Value was changed by the `with` statement, restore
                os.environ[key] = self._previous_env[key]
            else:
                # Value was added by the `with` statement, delete
                del os.environ[key]


def source(filename, *, recursive=False, quiet=True, shell="bash"):
    """ Source a bash script and export any environment variables defined
        there.
        - filename: The name of the file being sourced, defaults to 'env'
        - recursive: Whether to go through all the parent directories to find
              similarly named bash scripts, defaults to `False`
        - shell: which shell to use for sourcing, defaults to 'bash'
        Can also be used as a context processor for temporary environment
        changes, like `export` (in fact, it uses `export` internally).
        Usage:
        Assuming our directory structure is:
            - a/
              - env (export AAA="aaa")
              - b/
                - env (export BBB="bbb")
        and our current directory is `a/b`:
            >>> 'BBB' in os.environ
            <<< False
            >>>  with source('env'):
            ...     os.environ['BBB']
            <<< 'bbb'
            >>> 'BBB' in os.environ
            <<< False
            >>> source('env')
            >>> os.environ['BBB']
            <<< 'bbb'
            >>>  with source('env', recursive=True):
            ...     os.environ['AAA']
            <<< 'aaa'
            >>> 'AAA' in os.environ
            <<< False
            >>> source('env', recursive=True)
            >>> os.environ['AAA']
            <<< 'aa'
    """

    ptr = pathlib.Path('.').resolve()
    filenames = []
    if (ptr / filename).exists() and (ptr / filename).is_file():
        filenames.append(str((ptr / filename).resolve()))
    if recursive:
        while True:
            ptr = ptr.parent
            if (ptr / filename).exists() and (ptr / filename).is_file():
                filenames.append(str((ptr / filename).resolve()))
            if ptr == ptr.parent: break

    env = {}
    shell_cmd = globals()[shell]
    for filename in reversed(filenames):
        result = f"source {filename} && declare -x" | shell_cmd
        if not result:
            if quiet: continue
            else: result.raise_for_returncode()
        for line in result:
            match = re.search(r'^declare -x ([^=]+)="(.*)"$', line.strip())
            if not match: continue
            key, value = match.groups()
            if key not in os.environ or value != os.environ[key]: env[key] = value
    return export(**env)

