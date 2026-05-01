import re
from typing import Any

SENSITIVE_FIELDS = {
    "password_hash",
    "password",
    "totp_secret",
    "refresh_token_hash",
    "aadhaar_number",
}


def strip_sensitive_fields(data: Any) -> Any:
    if isinstance(data, dict):
        clean: dict[str, Any] = {}
        for key, value in data.items():
            if key in SENSITIVE_FIELDS:
                continue
            clean[key] = strip_sensitive_fields(value)
        return clean
    if isinstance(data, list):
        return [strip_sensitive_fields(item) for item in data]
    return data


def sanitize_search_query(value: str | None, max_len: int = 100) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", cleaned)
    if not cleaned:
        return None
    return cleaned[:max_len]


def escaped_regex(value: str) -> dict:
    return {"$regex": re.escape(value), "$options": "i"}


def mask_aadhaar(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) != 12:
        return None
    return f"XXXX-XXXX-{digits[-4:]}"
