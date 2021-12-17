import os
import sys
import subprocess
import logging
import platform
from pathlib import Path
from typing import Union, List, Type


logger = logging.getLogger(name='lazycls')


def exec_cmd(cmd, raise_error: bool = True):
    try:
        out = subprocess.check_output(cmd, shell=True)
        if isinstance(out, bytes): out = out.decode('utf8')
        return out.strip()
    except Exception as e:
        if not raise_error: return ""
        raise e

def exec_daemon(cmd: Union[List[str], str], stdout = subprocess.PIPE, stderr = subprocess.STDOUT, set_proc_uid: bool = True, *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    if set_proc_uid and platform.system() != 'Darwin': return subprocess.Popen(cmd, stdout = stdout, stderr = stderr, preexec_fn = lambda: os.setuid(1), *args, **kwargs)
    return subprocess.Popen(cmd, stdout = stdout, stderr = stderr, *args, **kwargs)

def exec_run(cmd: Union[List[str], str], *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    return subprocess.run(cmd, *args, **kwargs)

def exec_out(cmd: Union[List[str], str], shell: bool = True, *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    return subprocess.check_output(cmd, shell = shell, *args, **kwargs)


def exec_shell(cmd): return os.system(cmd)
def getParentPath(p: str) -> Path: return Path(p).parent
def to_camelcase(string: str) -> str: return ''.join(word.capitalize() for word in string.split('_'))

def toPath(path: Union[str, Path], resolve: bool = True) -> Type[Path]:
    if isinstance(path, str): path = Path(path)
    if resolve: path.resolve()
    return path

def get_user_path(path: Union[str, Path], resolve: bool = False) -> Type[Path]:
    if isinstance(path, str): path = Path(path)
    path = path.expanduser()
    if resolve: path.resolve()
    return path


def get_cwd(*paths, posix: bool = True) -> Union[str, Path]:
    if not paths:
        if posix: return Path.cwd().as_posix()
        return Path.cwd()
    if posix: return Path.cwd().joinpath(*paths).as_posix()
    return Path.cwd().joinpath(*paths)

def list_to_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def list_to_dict(items, delim='='):
    res = {}
    for item in items:
        i = item.split(delim, 1)
        res[i[0].strip()] = i[-1].strip()
    return res


def get_variable_separator():
    """
    Returns the environment variable separator for the current platform.
    :return: Environment variable separator
    """
    if sys.platform.startswith('win'):
        return ';'
    return ':'


def find_binary_in_path(filename):
    """
    Searches for a binary named `filename` in the current PATH. If an executable is found, its absolute path is returned
    else None.
    :param filename: Filename of the binary
    :return: Absolute path or None
    """
    if 'PATH' not in os.environ:
        return None
    for directory in os.environ['PATH'].split(get_variable_separator()):
        binary = os.path.abspath(os.path.join(directory, filename))
        if os.path.isfile(binary) and os.access(binary, os.X_OK):
            return binary
    return None

def exec_sed(path: Union[str, Path], changes: List[set], new_path: Union[str, Path] = None, chmod: int = None) -> Path:
    """ Does a cleaner `sed` command 
        - path: path to file
        - changes: List of {find, replace, n_times}
        - new_path: new filepath, otherwise will write to original
        - chmod: int 
    """
    f = toPath(path)
    txt = f.read_text(encoding='utf-8')
    for c in changes:
        txt = txt.replace(str(c[0]), str(c[1]), c[2]) if len(c) == 3 else txt.replace(str(c[0]), str(c[1]))
    if new_path: f = toPath(new_path)
    f.write_text(txt, encoding='utf-8')
    if chmod: f.chmod(chmod)
    return f

# Aliases that wont be exported by default
run = exec_run
cmd = exec_cmd
exec_command = exec_cmd

get_parent_path = getParentPath
to_path = toPath


__all__ = [
    'exec_run',
    'exec_out',
    'exec_cmd',
    'exec_daemon',
    'exec_command',
    'exec_sed',
    'get_parent_path',
    'getParentPath',
    'to_path',
    'to_user_path',
    'get_cwd',
    'toPath',
    'to_camelcase',
]

