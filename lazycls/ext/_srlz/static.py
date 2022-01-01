
READ = 'r'
WRITE = 'w'
APPEND = 'a'

READ_BINARY = 'rb'
WRITE_BINARY = 'wb'
BINARY_MODES = (READ_BINARY, WRITE_BINARY)

NEWLINE = '\n'
BINARY_NEWLINE = b'\n'

WHENCE_START = 0
WHENCE_CURRENT = 1
WHENCE_END = 2
WHENCE_CHOICES = (WHENCE_START, WHENCE_CURRENT, WHENCE_END)

class Mode:
    read = READ
    read_binary = READ_BINARY
    write = WRITE
    write_binary = WRITE_BINARY
    append = APPEND
    binary_modes = BINARY_MODES


    