import string
import secrets
from typing import Dict
from .static import Defaults
from ._base import Base

ALPHA_NUMERIC = string.ascii_letters + string.digits

class Secret:
    
    @classmethod
    def uuid_passcode(cls, length: int = None, clean: bool = True, method: str = Defaults.uuid_method):
        rez = Base.get_uuid(method=method)
        if clean: rez = rez.replace('-', '').strip()
        if length: rez = rez[:length]
        return rez
    
    @classmethod
    def alphanumeric_passcode(cls, length: int = 16):
        return ''.join(secrets.choice(ALPHA_NUMERIC) for i in range(length))

    @classmethod
    def token(cls, length: int = 32, safe: bool = False, clean: bool = True):
        rez = secrets.token_hex(length) if safe else secrets.token_urlsafe(length)
        if clean:
            for i in rez: 
                if i not in ALPHA_NUMERIC: rez.replace(i, secrets.choice(ALPHA_NUMERIC))
        return rez

    @classmethod
    def keypair(cls, k_len: int = 16, s_len: int = 36) -> Dict[str, str]:
        return {
            'key': cls.alphanumeric_passcode(k_len),
            'secret': cls.alphanumeric_passcode(s_len) 
        }

