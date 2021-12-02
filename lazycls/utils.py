import os
import sys
import subprocess
import logging
from pathlib import Path
from typing import Union, List


logger = logging.getLogger(name='lazycls')


def exec_cmd(cmd):
    out = subprocess.check_output(cmd, shell=True)
    if isinstance(out, bytes): out = out.decode('utf8')
    return out.strip()

def exec_daemon(cmd: Union[List[str], str], stdout = subprocess.PIPE, stderr = subprocess.STDOUT, *args, **kwargs):
    if isinstance(cmd, str): cmd = [cmd]
    return subprocess.Popen(cmd, stdout = stdout, stderr = stderr, preexec_fn = lambda: os.setuid(1), *args, **kwargs)

def exec_shell(cmd): return os.system(cmd)
def getParentPath(p: str) -> Path: return Path(p).parent
def to_camelcase(string: str) -> str: return ''.join(word.capitalize() for word in string.split('_'))

def toPath(path: Union[str, Path], resolve: bool = True) -> Path:
    if isinstance(path, str): path = Path(path)
    if resolve: path.resolve()
    return path



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

# Aliases that wont be exported by default
run = exec_cmd
cmd = exec_cmd
exec_command = exec_cmd

get_parent_path = getParentPath
to_path = toPath


__all__ = [
    'exec_cmd',
    'exec_daemon',
    'exec_command',
    'get_parent_path',
    'getParentPath',
    'to_path',
    'toPath',
    'to_camelcase',
]