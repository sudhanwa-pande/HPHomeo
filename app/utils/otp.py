import secrets
import hashlib
import hmac
from typing import Union

from pydantic import SecretStr


def generate_otp(length: int = 6) -> str:
    n = secrets.randbelow(10 ** length)
    return str(n).zfill(length)


def _secret_to_str(secret: Union[str, SecretStr]) -> str:
    if isinstance(secret, SecretStr):
        return secret.get_secret_value()
    return str(secret)


def hash_otp(code: str, secret: Union[str, SecretStr]) -> str:
    secret_str = _secret_to_str(secret)
    return hmac.new(
        secret_str.encode("utf-8"),
        code.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_otp(code: str, code_hash: str, secret: Union[str, SecretStr]) -> bool:
    return hmac.compare_digest(hash_otp(code, secret), code_hash)