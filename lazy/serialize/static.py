
READ = 'r'
WRITE = 'w'
APPEND = 'a'

READ_BINARY = 'rb'
WRITE_BINARY = 'wb'
BINARY_MODES = (READ_BINARY, WRITE_BINARY)

class Mode:
    read = READ
    read_binary = READ_BINARY
    write = WRITE
    write_binary = WRITE_BINARY
    append = APPEND
    binary_modes = BINARY_MODES


NEWLINE = '\n'
BINARY_NEWLINE = b'\n'

WHENCE_START = 0
WHENCE_CURRENT = 1
WHENCE_END = 2
WHENCE_CHOICES = (WHENCE_START, WHENCE_CURRENT, WHENCE_END)

DEFAULT_BASE_METHOD = 'base64'
DEFAULT_HASH_METHOD = 'sha256'
DEFAULT_UUID_METHOD = 'uuid4'
DEFAULT_YAML_LOADER = 'default'
DEFAULT_YAML_DUMPER = 'default'


class Defaults:
    base_method: str = DEFAULT_BASE_METHOD
    hash_method: str = DEFAULT_HASH_METHOD
    uuid_method: str = DEFAULT_UUID_METHOD
    yaml_loader: str = DEFAULT_YAML_LOADER
    yaml_dumper: str = DEFAULT_YAML_DUMPER

