import string
import secrets
import jwt
import time
from typing import Dict, Any
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
    
    @classmethod
    def openssl_random_key(cls, length: int = 64, base: bool = True):
        # openssl rand 64 | base64
        key = secrets.token_hex(length)
        if base: key = Base.b64_encode(key)
        return key


    @classmethod
    def jwt_encode(cls, data: Dict[str, Any], secret: str = None, algorithm="HS256"):
        if not secret: 
            secret = cls.openssl_random_key()
        return {'secret': secret, 'key': jwt.encode(data, secret, algorithm=algorithm), 'data': data, 'algorithm': algorithm}

    @classmethod
    def jwt_decode(cls, data: str, secret: str, algorithm="HS256"):
        return jwt.decode(data, secret, algorithm=algorithm)

    @classmethod
    def supabase_keys(cls, secret: str = None, expiration: int = 157766400, algorithm="HS256"):
        # Helper method to create Supabase ANON Keys and Service Keys
        if not secret: secret = cls.openssl_random_key()
        iat = round(int(time.time()), -3)
        exp = iat + expiration
        payloads = {
            'anon_key': {
                "role": "anon",
                "iss": "supabase",
                "iat": iat,
                "exp": exp,
            },
            'service_key': {
                "role": "service_role",
                "iss": "supabase",
                "iat": iat,
                "exp": exp,
            },
            "secret": secret
        }
        payloads['anon_key_jwt'] = cls.jwt_encode(payloads['anon_key'], secret = secret, algorithm = algorithm)
        payloads['service_key_jwt'] = cls.jwt_encode(payloads['service_key'], secret = secret, algorithm = algorithm)
        return payloads
        



