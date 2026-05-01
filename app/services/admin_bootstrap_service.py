import logging

from pymongo.errors import DuplicateKeyError

from app.core.config import settings
from app.core.security import hash_password
from app.utils.phone import normalize_phone_e164
from app.utils.time import utc_now

logger = logging.getLogger(__name__)


def _required_bootstrap_values() -> tuple[str, str, str, str, str] | None:
    email = (settings.ADMIN_BOOTSTRAP_EMAIL or "").strip().lower()
    raw_phone = (settings.ADMIN_BOOTSTRAP_PHONE or "").strip()
    password = settings.ADMIN_BOOTSTRAP_PASSWORD or ""
    full_name = (settings.ADMIN_BOOTSTRAP_FULL_NAME or "").strip()
    registration_no = (settings.ADMIN_BOOTSTRAP_REGISTRATION_NO or "").strip()

    if not (email and raw_phone and password and full_name and registration_no):
        return None
    try:
        phone = normalize_phone_e164(raw_phone)
    except ValueError:
        logger.warning("Admin bootstrap skipped: ADMIN_BOOTSTRAP_PHONE is invalid E.164/normalizable format")
        return None
    return email, phone, password, full_name, registration_no


async def bootstrap_admin_if_needed(db) -> None:
    if not settings.ADMIN_BOOTSTRAP_ENABLED:
        return

    existing_admin = await db.doctors.find_one({"is_admin": True}, {"_id": 1})
    if existing_admin:
        logger.info("Admin bootstrap skipped: admin already exists")
        return

    required = _required_bootstrap_values()
    if not required:
        logger.warning(
            "Admin bootstrap enabled but missing required settings. "
            "Set ADMIN_BOOTSTRAP_EMAIL/PHONE/PASSWORD/FULL_NAME/REGISTRATION_NO."
        )
        return

    email, phone, password, full_name, registration_no = required
    now = utc_now()
    doc = {
        "full_name": full_name,
        "email": email,
        "phone": phone,
        "password_hash": hash_password(password),
        "registration_no": registration_no,
        "role": "doctor",
        "is_admin": True,
        "verification_status": "approved",
        "rejection_reason": None,
        "verified_at": now,
        "verified_by_admin_id": None,
        "logo_url": None,
        "signature_url": None,
        "totp_enabled": False,
        "totp_secret": None,
        "totp_enabled_at": None,
        "email_otp_enabled": True,
        "refresh_token_hash": None,
        "refresh_token_expires_at": None,
        "refresh_token_rotated_at": None,
        "last_login_at": None,
        "last_login_ip": None,
        "failed_login_attempts": 0,
        "locked_until": None,
        "is_suspended": False,
        "created_at": now,
        "updated_at": now,
    }

    try:
        res = await db.doctors.insert_one(doc)
        logger.info("Admin bootstrap complete: doctor_id=%s email=%s", res.inserted_id, email)
    except DuplicateKeyError:
        logger.warning("Admin bootstrap skipped: unique conflict for email/phone/registration_no")
