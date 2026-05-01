import secrets
import hashlib
import hmac

from app.core.security import decrypt_secret, encrypt_secret

def generate_magic_token(nbytes: int = 32) -> str:
    # URL-safe, cryptographically secure
    return secrets.token_urlsafe(nbytes)


def hash_magic_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def verify_magic_token(token: str, token_hash: str) -> bool:
    return hmac.compare_digest(hash_magic_token(token), token_hash)


def encrypt_magic_token(token: str) -> str:
    return encrypt_secret(token)


def decrypt_magic_token(token_enc: str) -> str:
    return decrypt_secret(token_enc)
