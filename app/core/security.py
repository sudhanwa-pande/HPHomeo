from __future__ import annotations

import base64
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from cryptography.fernet import Fernet
from jose import jwt
from passlib.context import CryptContext

from app.core.config import settings

if TYPE_CHECKING:
    from fastapi.responses import Response

pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    return pwd_context.verify(password, hashed)

def create_access_token(data: dict, expires_minutes: int | None = None) -> str:
    to_encode = data.copy()
    now = datetime.now(timezone.utc)
    expire = now + timedelta(minutes=expires_minutes or settings.JWT_EXPIRE_MIN)
    to_encode.update({
        "iat": now,
        "exp": expire,
        "jti": to_encode.get("jti") or str(uuid.uuid4()),
    })
    return jwt.encode(to_encode, settings.JWT_SECRET.get_secret_value(), algorithm=settings.JWT_ALGO)

def create_refresh_token(data: dict, expires_days: int | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        days=expires_days or settings.JWT_REFRESH_EXPIRE_DAYS
    )
    to_encode.update(
        {
            "exp": expire,
            "jti": to_encode.get("jti") or str(uuid.uuid4()),
            "type": "refresh",
        }
    )
    return jwt.encode(to_encode, settings.JWT_SECRET.get_secret_value(), algorithm=settings.JWT_ALGO)

def decode_token(token: str) -> dict:
    return jwt.decode(
        token,
        settings.JWT_SECRET.get_secret_value(),
        algorithms=[settings.JWT_ALGO],
        options={"verify_exp": True, "verify_signature": True},
    )


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _fernet() -> Fernet:
    raw = settings.TOTP_ENCRYPTION_KEY.get_secret_value().encode("utf-8")
    key = base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
    return Fernet(key)


def set_auth_cookies(
    response: Response,
    *,
    access_token: str,
    refresh_token: str,
    role: str,
    access_max_age: int | None = None,
    refresh_max_age: int | None = None,
) -> None:
    """Set httpOnly auth cookies on the response for a given role (doctor/patient)."""
    if access_max_age is None:
        access_max_age = settings.JWT_EXPIRE_MIN * 60
    if refresh_max_age is None:
        refresh_max_age = settings.JWT_REFRESH_EXPIRE_DAYS * 86400

    cookie_kwargs: dict = {
        "httponly": True,
        "secure": True,
        "samesite": "lax",
        "domain": settings.COOKIE_DOMAIN,
        "path": "/",
    }
    response.set_cookie(
        key="access_token",
        value=access_token,
        max_age=access_max_age,
        **cookie_kwargs,
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        max_age=refresh_max_age,
        **cookie_kwargs,
    )


def clear_auth_cookies(response: Response, *, role: str) -> None:
    """Delete httpOnly auth cookies for a given role."""
    response.delete_cookie("access_token", path="/")
    response.delete_cookie("refresh_token", path="/")


def set_public_patient_access_cookie(
    response: Response,
    *,
    token: str,
    max_age: int,
) -> None:
    response.set_cookie(
        key=settings.PUBLIC_PATIENT_ACCESS_COOKIE_NAME,
        value=token,
        max_age=max_age,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN,
        path=settings.PUBLIC_PATIENT_ACCESS_COOKIE_PATH,
    )


def clear_public_patient_access_cookie(response: Response) -> None:
    response.delete_cookie(
        key=settings.PUBLIC_PATIENT_ACCESS_COOKIE_NAME,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN,
        path=settings.PUBLIC_PATIENT_ACCESS_COOKIE_PATH,
    )


def set_admin_session_cookie(
    response: Response,
    *,
    token: str,
    max_age: int,
) -> None:
    """Set httpOnly cookie for admin re-auth session token."""
    response.set_cookie(
        key="admin_session_token",
        value=token,
        max_age=max_age,
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN,
        path="/",
    )


def clear_admin_session_cookie(response: Response) -> None:
    """Delete admin session cookie."""
    response.delete_cookie(
        key="admin_session_token",
        httponly=True,
        secure=settings.COOKIE_SECURE,
        samesite=settings.COOKIE_SAMESITE,
        domain=settings.COOKIE_DOMAIN,
        path="/",
    )


def encrypt_secret(value: str) -> str:
    return _fernet().encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_secret(value: str) -> str:
    return _fernet().decrypt(value.encode("utf-8")).decode("utf-8")
