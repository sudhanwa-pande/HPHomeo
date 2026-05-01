import re

PHONE_E164_RE = re.compile(r"^\+[1-9]\d{7,14}$")


def normalize_phone_e164(value: str, *, default_country_code: str = "91") -> str:
    raw = (value or "").strip()
    cleaned = re.sub(r"[\s().-]", "", raw)
    if not cleaned:
        raise ValueError("Phone is required")

    if cleaned.startswith("+"):
        candidate = "+" + re.sub(r"\D", "", cleaned[1:])
    else:
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) == 10:
            candidate = f"+{default_country_code}{digits}"
        elif digits.startswith(default_country_code) and len(digits) == len(default_country_code) + 10:
            candidate = f"+{digits}"
        elif digits.startswith("0") and len(digits) == 11:
            candidate = f"+{default_country_code}{digits[1:]}"
        else:
            raise ValueError("Invalid phone number format")

    if not PHONE_E164_RE.fullmatch(candidate):
        raise ValueError("Invalid phone number format")
    return candidate
