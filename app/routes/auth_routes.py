import base64
import io
import logging
from datetime import datetime, timedelta, timezone

import pyotp
import qrcode
import httpx
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from jose import JWTError
from pymongo.errors import DuplicateKeyError

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.token_blacklist import blacklist_token, is_token_blacklisted
from app.core.security import (
    clear_admin_session_cookie,
    clear_auth_cookies,
    create_access_token,
    create_refresh_token,
    decrypt_secret,
    decode_token,
    encrypt_secret,
    hash_password,
    hash_token,
    set_auth_cookies,
    verify_password,
)
from app.schemas.doctor_schema import (
    ChangePasswordIn,
    DoctorLogin,
    ForgotPasswordIn,
    LoginOtpVerifyIn,
    LoginStepOut,
    LoginTotpValidateIn,
    DoctorRegister,
    ResetPasswordIn,
    TotpEnableIn,
)
from app.services.email_service import send_doctor_login_otp, send_password_reset_otp, safe_send_email
from app.services.otp_redis_service import (
    OTP_MAX_ATTEMPTS,
    acquire_resend_cooldown,
    clear_otp,
    consume_otp_hash,
    get_attempts,
    get_otp_hash,
    increment_attempts,
    is_locked,
    store_otp,
)
from app.services.pending_login_service import (
    PENDING_LOGIN_TTL_SECONDS,
    consume_pending_login,
    create_pending_login,
    get_pending_login,
)
from app.utils.otp import generate_otp, hash_otp, verify_otp
from app.utils.phone import normalize_phone_e164
from app.utils.security_sanitize import strip_sensitive_fields
from app.utils.time import ensure_utc, utc_now

router = APIRouter(prefix="/auth", tags=["Auth"])
logger = logging.getLogger(__name__)

FAILED_LOGIN_LIMIT = 10
LOCK_MINUTES = 15
DOCTOR_LOGIN_OTP_PURPOSE = "doctor_login"


def _is_profile_complete(doc: dict, *, has_availability: bool | None = None) -> bool:
    """
    A doctor's profile is complete when ALL of these are set:
    - specialization
    - available_modes (at least one)
    - online_fee (if 'online' in available_modes)
    - walkin_fee (if 'walk_in' in available_modes)
    - about (bio text)
    - profile_photo
    - signature_url
    - availability schedule (passed via has_availability flag, or ignored if None)
    """
    specialization = (doc.get("specialization") or "").strip()
    available_modes = doc.get("available_modes") or []

    if not specialization or not isinstance(available_modes, list) or not available_modes:
        return False

    has_online = "online" in available_modes
    has_walk_in = "walk_in" in available_modes

    if has_online and doc.get("online_fee") is None:
        return False
    if has_walk_in and doc.get("walkin_fee") is None:
        return False

    if not (doc.get("about") or "").strip():
        return False
    if not doc.get("profile_photo"):
        return False
    if not doc.get("signature_url"):
        return False

    if has_availability is not None and not has_availability:
        return False

    return True


def doctor_doc_to_out(doc: dict):
    return {
        "id": str(doc["_id"]),
        "full_name": doc.get("full_name"),
        "email": doc.get("email"),
        "phone": doc.get("phone"),
        "registration_no": doc.get("registration_no"),
        "role": doc.get("role", "doctor"),
        "is_admin": bool(doc.get("is_admin", False)),
        "is_suspended": bool(doc.get("is_suspended", False)),
        "verification_status": doc.get("verification_status", "pending"),
        "profile_photo": doc.get("profile_photo"),
        "profile_complete": _is_profile_complete(doc, has_availability=doc.get("_has_availability")),
        "totp_enabled": bool(doc.get("totp_enabled", False)),
    }


def _client_ip(request: Request) -> str | None:
    # 1. Prioritize Cloudflare's real client IP
    cf_ip = request.headers.get("cf-connecting-ip")
    if cf_ip:
        return cf_ip.strip()

    # 2. Fallback to standard forwarded-for
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()

    # 3. Last resort: direct socket host
    return request.client.host if request.client else None


def _user_agent(request: Request) -> str:
    return (request.headers.get("user-agent") or "").strip()[:512]


def _normalize_identity(value: str) -> str:
    return value.strip().lower()


def _build_token_pair(doctor_id: str, *, is_admin: bool = False) -> dict:
    """Build access + refresh token strings for a doctor."""
    payload = {"sub": doctor_id, "role": "doctor", "is_admin": bool(is_admin)}
    access_token = create_access_token(payload)
    refresh_token = create_refresh_token(payload)
    return {"access_token": access_token, "refresh_token": refresh_token}


async def _issue_doctor_tokens(
    *, db, doctor_doc: dict, request: Request, response: Response,
) -> dict:
    """Create tokens, persist refresh hash, set httpOnly cookies. Returns doctor info."""
    now = utc_now()
    tokens = _build_token_pair(
        str(doctor_doc["_id"]),
        is_admin=bool(doctor_doc.get("is_admin", False)),
    )
    refresh_payload = decode_token(tokens["refresh_token"])
    refresh_exp = refresh_payload.get("exp")
    refresh_expires_at = now
    if isinstance(refresh_exp, (int, float)):
        refresh_expires_at = datetime.fromtimestamp(refresh_exp, tz=timezone.utc)

    await db.doctors.update_one(
        {"_id": doctor_doc["_id"]},
        {
            "$set": {
                "last_login_at": now,
                "last_login_ip": _client_ip(request),
                "failed_login_attempts": 0,
                "locked_until": None,
                "refresh_token_hash": hash_token(tokens["refresh_token"]),
                "refresh_token_expires_at": refresh_expires_at,
                "refresh_token_rotated_at": now,
                "updated_at": now,
            }
        },
    )

    set_auth_cookies(
        response,
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        role="doctor",
    )
    avail = await db.doctor_availability.find_one({"doctor_id": doctor_doc["_id"]}, {"_id": 1})
    doctor_doc["_has_availability"] = avail is not None
    return doctor_doc_to_out(doctor_doc)


def _pending_context_ok(pending: dict, request: Request) -> bool:
    """
    Check if the client context matches the pending login state.
    We prioritize User-Agent over IP because IP is unreliable behind proxies (e.g. Cloudflare).
    The short-lived temp_token itself provides the primary security guarantee.
    """
    return str(pending.get("user_agent") or "") == _user_agent(request)


async def _decode_access_token(token: str) -> dict:
    try:
        payload = decode_token(token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    if payload.get("type") == "refresh":
        raise HTTPException(status_code=401, detail="Invalid token")
    return payload


# ---------------------------------------------------------------------------
# Dependencies: read access token from httpOnly cookie
# ---------------------------------------------------------------------------

async def get_current_doctor(request: Request):
    doc = await get_current_doctor_any_status(request)
    if doc.get("verification_status") != "approved":
        raise HTTPException(status_code=403, detail="Doctor verification pending")
    return doc


async def get_current_doctor_any_status(request: Request):
    token = request.cookies.get("doctor_access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    db = get_db()
    payload = await _decode_access_token(token)
    if payload.get("role") != "doctor":
        raise HTTPException(status_code=401, detail="Invalid token")
    if await is_token_blacklisted(db, payload.get("jti")):
        raise HTTPException(status_code=401, detail="Token revoked")

    doctor_id = payload.get("sub")
    if not doctor_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        doc = await db.doctors.find_one({"_id": ObjectId(doctor_id)})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not doc:
        raise HTTPException(status_code=401, detail="Doctor not found")
    tokens_invalidated_before = doc.get("tokens_invalidated_before")
    if tokens_invalidated_before is not None:
        iat = payload.get("iat")
        if iat is None or datetime.fromtimestamp(float(iat), tz=timezone.utc) < ensure_utc(tokens_invalidated_before):
            raise HTTPException(status_code=401, detail="Session invalidated. Please log in again.")
    if doc.get("is_suspended"):
        raise HTTPException(status_code=403, detail="Account suspended")
    if doc.get("verification_status") not in {"pending", "approved"}:
        raise HTTPException(status_code=403, detail="Account not allowed")
    return doc


async def get_current_admin(request: Request):
    token = request.cookies.get("doctor_access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    db = get_db()
    payload = await _decode_access_token(token)
    role = payload.get("role")
    is_admin_claim = bool(payload.get("is_admin", False))
    if role != "doctor":
        raise HTTPException(status_code=401, detail="Invalid token")
    if not is_admin_claim:
        raise HTTPException(status_code=403, detail="Admin access required")
    if await is_token_blacklisted(db, payload.get("jti")):
        raise HTTPException(status_code=401, detail="Token revoked")

    admin_id = payload.get("sub")
    if not admin_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        doc = await db.doctors.find_one({"_id": ObjectId(admin_id)})
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not doc:
        raise HTTPException(status_code=401, detail="Admin not found")
    if not bool(doc.get("is_admin", False)):
        raise HTTPException(status_code=403, detail="Admin access required")
    if doc.get("is_suspended"):
        raise HTTPException(status_code=403, detail="Account suspended")
    return doc


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/register",
    dependencies=[rl(settings.RL_DOCTOR_REGISTER_TIMES, settings.RL_DOCTOR_REGISTER_SECONDS)],
)
async def register(data: DoctorRegister):
    db = get_db()
    now = utc_now()

    turnstile_secret = getattr(settings, "TURNSTILE_SECRET_KEY", None)
    if turnstile_secret:
        if not data.turnstileToken:
            raise HTTPException(status_code=403, detail="CAPTCHA token missing")
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(
                    "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                    data={"secret": turnstile_secret, "response": data.turnstileToken},
                    timeout=5.0
                )
                outcome = resp.json()
                if not outcome.get("success"):
                    logger.warning(f"Turnstile verification failed: {outcome}")
                    raise HTTPException(status_code=403, detail="CAPTCHA verification failed")
            except httpx.RequestError as e:
                logger.error(f"Error contacting Cloudflare: {e}")
                raise HTTPException(status_code=500, detail="CAPTCHA service unavailable")

    try:
        email = data.email.strip().lower()
        try:
            phone = normalize_phone_e164(data.phone)
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid phone number format")
        reg_no = data.registration_no.strip()
        if await db.doctors.find_one({"email": email}, {"_id": 1}):
            raise HTTPException(status_code=409, detail="Email already registered")
        if await db.doctors.find_one({"phone": phone}, {"_id": 1}):
            raise HTTPException(status_code=409, detail="Phone already registered")
        if await db.doctors.find_one({"registration_no": reg_no}, {"_id": 1}):
            raise HTTPException(status_code=409, detail="Registration number already in use")

        doc = {
            "full_name": data.full_name.strip(),
            "email": email,
            "phone": phone,
            "password_hash": hash_password(data.password),
            "registration_no": reg_no,
            "role": "doctor",
            "is_admin": False,
            "verification_status": "pending",
            "rejection_reason": None,
            "verified_at": None,
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
        res = await db.doctors.insert_one(doc)
        return {
            "message": "registered",
            "doctor_id": str(res.inserted_id),
            "verification_status": "pending",
        }
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="Email, phone, or registration number already registered")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Register failed")
        raise HTTPException(status_code=500, detail="Register failed")


@router.post(
    "/login",
    dependencies=[rl(settings.RL_DOCTOR_LOGIN_TIMES, settings.RL_DOCTOR_LOGIN_SECONDS)],
)
async def login(request: Request, response: Response, payload: DoctorLogin):
    db = get_db()
    now = utc_now()
    email = _normalize_identity(payload.email)

    doc = await db.doctors.find_one({"email": email})
    if not doc:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    locked_until = ensure_utc(doc.get("locked_until"))
    if locked_until and locked_until > now:
        raise HTTPException(status_code=423, detail="Account locked due to failed attempts. Try later.")

    if not verify_password(payload.password, doc["password_hash"]):
        failed_attempts = int(doc.get("failed_login_attempts") or 0) + 1
        update = {
            "failed_login_attempts": failed_attempts,
            "updated_at": now,
        }
        if failed_attempts >= FAILED_LOGIN_LIMIT:
            update["locked_until"] = now + timedelta(minutes=LOCK_MINUTES)
        await db.doctors.update_one({"_id": doc["_id"]}, {"$set": update})
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if doc.get("is_suspended"):
        raise HTTPException(status_code=403, detail="Account suspended")
    if doc.get("verification_status") != "approved":
        raise HTTPException(status_code=403, detail="Doctor verification pending")

    if not getattr(settings, "AUTH_2STEP_ENABLED", False):
        doctor_info = await _issue_doctor_tokens(
            db=db, doctor_doc=doc, request=request, response=response,
        )
        return {"message": "authenticated", "doctor": doctor_info}

    method = "totp" if doc.get("totp_enabled") else "email_otp"
    temp_token = await create_pending_login(
        doctor_id=str(doc["_id"]),
        email=email,
        role="doctor",
        method=method,
        ip=_client_ip(request),
        user_agent=_user_agent(request),
        ttl_seconds=PENDING_LOGIN_TTL_SECONDS,
    )

    if method == "email_otp":
        cooldown_purpose = f"{DOCTOR_LOGIN_OTP_PURPOSE}:cooldown"
        existing_cooldown = await get_otp_hash(
            channel="email",
            identity=email,
            purpose=cooldown_purpose,
        )
        if existing_cooldown:
            raise HTTPException(status_code=429, detail="OTP recently sent. Please wait before retrying.")

        otp_code = generate_otp(6)
        otp_hash = hash_otp(otp_code, settings.OTP_SECRET)
        await store_otp(
            channel="email",
            identity=email,
            purpose=cooldown_purpose,
            code_hash="cooldown",
            ttl_seconds=45,
        )
        await store_otp(
            channel="email",
            identity=email,
            purpose=f"{DOCTOR_LOGIN_OTP_PURPOSE}:{temp_token}",
            code_hash=otp_hash,
            ttl_seconds=int(getattr(settings, "OTP_TTL_MINUTES", 5)) * 60,
        )
        await safe_send_email(
            send_doctor_login_otp(
                email=email,
                code=otp_code,
                ttl_minutes=int(getattr(settings, "OTP_TTL_MINUTES", 5)),
            ),
            "doctor login otp",
        )
        return LoginStepOut(
            step="otp_required",
            temp_token=temp_token,
            expires_in_seconds=PENDING_LOGIN_TTL_SECONDS,
            otp_channel="email",
        )

    return LoginStepOut(
        step="totp_required",
        temp_token=temp_token,
        expires_in_seconds=PENDING_LOGIN_TTL_SECONDS,
    )


@router.post(
    "/otp/verify",
    dependencies=[rl(settings.RL_DOCTOR_VERIFY_OTP_TIMES, settings.RL_DOCTOR_VERIFY_OTP_SECONDS)],
)
async def verify_login_otp(request: Request, response: Response, payload: LoginOtpVerifyIn):
    if not getattr(settings, "AUTH_2STEP_ENABLED", False):
        raise HTTPException(status_code=404, detail="2-step auth is disabled")

    db = get_db()
    pending = await get_pending_login(payload.temp_token)
    if not pending:
        raise HTTPException(status_code=401, detail="Invalid or expired temp token")
    if pending.get("method") != "email_otp":
        raise HTTPException(status_code=409, detail="Use TOTP validation for this login")
    if not _pending_context_ok(pending, request):
        await consume_pending_login(payload.temp_token)
        raise HTTPException(status_code=401, detail="Client context mismatch")

    email = _normalize_identity(str(pending.get("email") or ""))
    purpose = f"{DOCTOR_LOGIN_OTP_PURPOSE}:{payload.temp_token}"

    attempts = await get_attempts(channel="email", identity=email, purpose=purpose)
    if is_locked(attempts):
        raise HTTPException(
            status_code=429,
            detail=f"Too many invalid OTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
        )

    code_hash = await get_otp_hash(channel="email", identity=email, purpose=purpose)
    if not code_hash:
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    if not verify_otp(payload.code, code_hash, settings.OTP_SECRET):
        attempts = await increment_attempts(channel="email", identity=email, purpose=purpose)
        if is_locked(attempts):
            raise HTTPException(
                status_code=429,
                detail=f"Too many invalid OTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
            )
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    consumed = await consume_pending_login(payload.temp_token)
    if not consumed:
        raise HTTPException(status_code=401, detail="Invalid or expired temp token")
    if not _pending_context_ok(consumed, request):
        raise HTTPException(status_code=401, detail="Client context mismatch")

    doctor_id = consumed.get("doctor_id")
    try:
        doctor_oid = ObjectId(str(doctor_id))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid login state")

    doc = await db.doctors.find_one({"_id": doctor_oid})
    if not doc:
        raise HTTPException(status_code=401, detail="Doctor not found")
    if doc.get("is_suspended"):
        raise HTTPException(status_code=403, detail="Account suspended")
    if doc.get("verification_status") != "approved":
        raise HTTPException(status_code=403, detail="Doctor verification pending")

    # Atomic final gate: GET+DEL the OTP key in one Lua script so a concurrent
    # request that also passed the initial verify cannot race to token issuance.
    consumed_hash = await consume_otp_hash(channel="email", identity=email, purpose=purpose)
    if not consumed_hash or not verify_otp(payload.code, consumed_hash, settings.OTP_SECRET):
        raise HTTPException(status_code=401, detail="OTP already used")
    await clear_otp(channel="email", identity=email, purpose=purpose)  # clean up attempts/resend keys
    doctor_info = await _issue_doctor_tokens(
        db=db, doctor_doc=doc, request=request, response=response,
    )
    return {"message": "authenticated", "doctor": doctor_info}


@router.post(
    "/totp/validate",
    dependencies=[rl(settings.RL_DOCTOR_VERIFY_TOTP_TIMES, settings.RL_DOCTOR_VERIFY_TOTP_SECONDS)],
)
async def validate_login_totp(request: Request, response: Response, payload: LoginTotpValidateIn):
    if not getattr(settings, "AUTH_2STEP_ENABLED", False):
        raise HTTPException(status_code=404, detail="2-step auth is disabled")

    db = get_db()
    pending = await get_pending_login(payload.temp_token)
    if not pending:
        raise HTTPException(status_code=401, detail="Invalid or expired temp token")
    if pending.get("method") != "totp":
        raise HTTPException(status_code=409, detail="Use OTP verification for this login")
    if not _pending_context_ok(pending, request):
        await consume_pending_login(payload.temp_token)
        raise HTTPException(status_code=401, detail="Client context mismatch")

    doctor_id = str(pending.get("doctor_id") or "")
    purpose = f"{DOCTOR_LOGIN_OTP_PURPOSE}:{payload.temp_token}"
    attempts = await get_attempts(channel="totp", identity=doctor_id, purpose=purpose)
    if is_locked(attempts):
        raise HTTPException(
            status_code=429,
            detail=f"Too many invalid TOTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
        )

    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid login state")

    doc = await db.doctors.find_one({"_id": doctor_oid})
    if not doc:
        await consume_pending_login(payload.temp_token)
        raise HTTPException(status_code=401, detail="Doctor not found")
    if not doc.get("totp_enabled"):
        await consume_pending_login(payload.temp_token)
        raise HTTPException(status_code=409, detail="TOTP is not enabled for this account")
    if doc.get("is_suspended"):
        await consume_pending_login(payload.temp_token)
        raise HTTPException(status_code=403, detail="Account suspended")
    if doc.get("verification_status") != "approved":
        await consume_pending_login(payload.temp_token)
        raise HTTPException(status_code=403, detail="Doctor verification pending")

    encrypted = doc.get("totp_secret")
    if not encrypted:
        raise HTTPException(status_code=400, detail="Invalid TOTP setup state")
    try:
        secret = decrypt_secret(encrypted)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid TOTP setup state")

    totp = pyotp.TOTP(secret)

    if not totp.verify(payload.code, valid_window=1):
        attempts = await increment_attempts(channel="totp", identity=doctor_id, purpose=purpose)
        if is_locked(attempts):
            raise HTTPException(
                status_code=429,
                detail=f"Too many invalid TOTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
            )
        raise HTTPException(status_code=401, detail="Invalid TOTP code")

    consumed = await consume_pending_login(payload.temp_token)
    if not consumed:
        raise HTTPException(status_code=401, detail="Invalid or expired temp token")
    if not _pending_context_ok(consumed, request):
        raise HTTPException(status_code=401, detail="Client context mismatch")

    await clear_otp(channel="totp", identity=doctor_id, purpose=purpose)
    doctor_info = await _issue_doctor_tokens(
        db=db, doctor_doc=doc, request=request, response=response,
    )
    return {"message": "authenticated", "doctor": doctor_info}


@router.post(
    "/refresh",
    dependencies=[rl(settings.RL_DOCTOR_REFRESH_TIMES, settings.RL_DOCTOR_REFRESH_SECONDS)],
)
async def refresh(request: Request, response: Response):
    """Refresh doctor tokens. Reads refresh token from httpOnly cookie."""
    refresh_token = request.cookies.get("doctor_refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="No refresh token")

    db = get_db()
    now = utc_now()
    try:
        decoded = decode_token(refresh_token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    if decoded.get("type") != "refresh" or decoded.get("role") != "doctor":
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    doctor_id = decoded.get("sub")
    if not doctor_id:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    doc = await db.doctors.find_one({"_id": doctor_oid})
    if not doc or doc.get("refresh_token_hash") != hash_token(refresh_token):
        raise HTTPException(status_code=401, detail="Refresh token revoked")
    if doc.get("is_suspended"):
        raise HTTPException(status_code=403, detail="Account suspended")
    if doc.get("verification_status") == "rejected":
        raise HTTPException(status_code=403, detail="Account not allowed")

    refresh_token_expires_at = ensure_utc(doc.get("refresh_token_expires_at"))
    if refresh_token_expires_at and refresh_token_expires_at <= now:
        raise HTTPException(status_code=401, detail="Refresh token expired")

    tokens = _build_token_pair(
        str(doc["_id"]),
        is_admin=bool(doc.get("is_admin", False)),
    )
    refresh_payload = decode_token(tokens["refresh_token"])
    refresh_exp = refresh_payload.get("exp")
    refresh_expires_at = now
    if isinstance(refresh_exp, (int, float)):
        refresh_expires_at = datetime.fromtimestamp(refresh_exp, tz=timezone.utc)

    await db.doctors.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "refresh_token_hash": hash_token(tokens["refresh_token"]),
                "refresh_token_expires_at": refresh_expires_at,
                "refresh_token_rotated_at": now,
                "updated_at": now,
            }
        },
    )

    set_auth_cookies(
        response,
        access_token=tokens["access_token"],
        refresh_token=tokens["refresh_token"],
        role="doctor",
    )
    return {"message": "refreshed"}


@router.post(
    "/totp/setup",
    dependencies=[rl(settings.RL_DOCTOR_TOTP_SETUP_TIMES, settings.RL_DOCTOR_TOTP_SETUP_SECONDS)],
)
async def setup_totp(request: Request, current=Depends(get_current_doctor)):
    db = get_db()
    now = utc_now()
    if current.get("totp_enabled"):
        raise HTTPException(status_code=409, detail="TOTP is already enabled")

    secret = pyotp.random_base32()
    encrypted = encrypt_secret(secret)
    issuer = (getattr(settings, "APP_NAME", None) or "HPHomeo").strip()
    email = current.get("email") or f"doctor-{current.get('_id')}"
    uri = pyotp.TOTP(secret).provisioning_uri(name=email, issuer_name=issuer)

    qr = qrcode.QRCode(box_size=6, border=2)
    qr.add_data(uri)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    qr_data_url = "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode("utf-8")

    await db.doctors.update_one(
        {"_id": current["_id"]},
        {"$set": {"totp_secret": encrypted, "totp_enabled": False, "updated_at": now}},
    )

    return strip_sensitive_fields(
        {
            "totp_enabled": False,
            "otpauth_url": uri,
            "qr_code_data_url": qr_data_url,
            "manual_entry_key": secret,
        }
    )


@router.post(
    "/totp/enable",
    dependencies=[rl(settings.RL_DOCTOR_TOTP_ENABLE_TIMES, settings.RL_DOCTOR_TOTP_ENABLE_SECONDS)],
)
async def enable_totp(request: Request, payload: TotpEnableIn, current=Depends(get_current_doctor)):
    db = get_db()
    now = utc_now()

    if current.get("totp_enabled"):
        raise HTTPException(status_code=409, detail="TOTP is already enabled")

    encrypted = current.get("totp_secret")
    if not encrypted:
        raise HTTPException(status_code=400, detail="TOTP setup required before enabling")

    try:
        secret = decrypt_secret(encrypted)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid TOTP setup state")

    doctor_id = str(current["_id"])
    purpose = "totp_setup"
    attempts = await get_attempts(channel="totp", identity=doctor_id, purpose=purpose)
    if is_locked(attempts):
        raise HTTPException(
            status_code=429,
            detail=f"Too many invalid TOTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
        )

    totp = pyotp.TOTP(secret)

    if not totp.verify(payload.code, valid_window=1):
        attempts = await increment_attempts(channel="totp", identity=doctor_id, purpose=purpose)
        if is_locked(attempts):
            raise HTTPException(
                status_code=429,
                detail=f"Too many invalid TOTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
            )
        raise HTTPException(status_code=400, detail="Invalid TOTP code")

    await clear_otp(channel="totp", identity=doctor_id, purpose=purpose)

    await db.doctors.update_one(
        {"_id": current["_id"]},
        {
            "$set": {
                "totp_enabled": True,
                "totp_enabled_at": now,
                "email_otp_enabled": False,
                "updated_at": now,
            }
        },
    )
    return {"message": "totp_enabled", "totp_enabled": True, "email_otp_enabled": False}


@router.post(
    "/logout",
    dependencies=[rl(settings.RL_DOCTOR_LOGOUT_TIMES, settings.RL_DOCTOR_LOGOUT_SECONDS)],
)
async def logout(request: Request, response: Response):
    """Logout doctor. Reads access token from cookie, blacklists it, clears cookies."""
    access_token = request.cookies.get("doctor_access_token")
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    db = get_db()
    now = utc_now()
    payload = await _decode_access_token(access_token)
    if payload.get("role") != "doctor":
        raise HTTPException(status_code=401, detail="Invalid token")

    jti = payload.get("jti")
    exp = payload.get("exp")
    if not jti or exp is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not isinstance(exp, (int, float)):
        raise HTTPException(status_code=401, detail="Invalid token")

    expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
    await blacklist_token(db, jti=jti, expires_at=expires_at, created_at=now)

    doctor_id = payload.get("sub")
    if doctor_id:
        clear_set = {
            "refresh_token_hash": None,
            "refresh_token_expires_at": None,
            "refresh_token_rotated_at": now,
            "updated_at": now,
        }
        try:
            await db.doctors.update_one({"_id": ObjectId(doctor_id)}, {"$set": clear_set})
        except Exception:
            pass

    clear_auth_cookies(response, role="doctor")
    clear_admin_session_cookie(response)
    return {"message": "logged_out"}


@router.get(
    "/me",
    dependencies=[rl(settings.RL_DOCTOR_ME_TIMES, settings.RL_DOCTOR_ME_SECONDS)],
)
async def me(request: Request, current=Depends(get_current_doctor)):
    db = get_db()
    avail = await db.doctor_availability.find_one({"doctor_id": current["_id"]}, {"_id": 1})
    current["_has_availability"] = avail is not None
    return strip_sensitive_fields(doctor_doc_to_out(current))


_PASSWORD_RESET_PURPOSE = "password_reset"
_PASSWORD_RESET_COOLDOWN_PURPOSE = "password_reset:cooldown"


@router.post(
    "/forgot-password",
    dependencies=[rl(settings.RL_DOCTOR_FORGOT_PASSWORD_TIMES, settings.RL_DOCTOR_FORGOT_PASSWORD_SECONDS)],
)
async def forgot_password(payload: ForgotPasswordIn):
    db = get_db()
    email = _normalize_identity(payload.email)
    ttl_seconds = int(getattr(settings, "OTP_TTL_MINUTES", 5)) * 60

    # Always return the same response — never reveal whether email is registered.
    generic_response = {
        "message": "If this email is registered, a reset code has been sent.",
        "expires_in_seconds": ttl_seconds,
    }

    doc = await db.doctors.find_one({"email": email}, {"_id": 1, "is_suspended": 1})
    if not doc or doc.get("is_suspended"):
        return generic_response

    # Per-email cooldown: prevent spamming the same address.
    acquired = await acquire_resend_cooldown(
        channel="email",
        identity=email,
        purpose=_PASSWORD_RESET_COOLDOWN_PURPOSE,
        ttl_seconds=60,
    )
    if not acquired:
        return generic_response

    otp_code = generate_otp(6)
    otp_hash = hash_otp(otp_code, settings.OTP_SECRET)
    await store_otp(
        channel="email",
        identity=email,
        purpose=_PASSWORD_RESET_PURPOSE,
        code_hash=otp_hash,
        ttl_seconds=ttl_seconds,
    )
    await safe_send_email(
        send_password_reset_otp(
            email=email,
            code=otp_code,
            ttl_minutes=int(getattr(settings, "OTP_TTL_MINUTES", 5)),
        ),
        "password reset otp",
    )
    return generic_response


@router.post(
    "/reset-password",
    dependencies=[rl(settings.RL_DOCTOR_RESET_PASSWORD_TIMES, settings.RL_DOCTOR_RESET_PASSWORD_SECONDS)],
)
async def reset_password(response: Response, payload: ResetPasswordIn):
    db = get_db()
    email = _normalize_identity(payload.email)

    doc = await db.doctors.find_one({"email": email})
    if not doc:
        raise HTTPException(status_code=401, detail="Invalid or expired reset code")

    attempts = await get_attempts(channel="email", identity=email, purpose=_PASSWORD_RESET_PURPOSE)
    if is_locked(attempts):
        raise HTTPException(
            status_code=429,
            detail="Too many invalid attempts. Request a new reset code.",
        )

    code_hash = await get_otp_hash(channel="email", identity=email, purpose=_PASSWORD_RESET_PURPOSE)
    if not code_hash:
        raise HTTPException(status_code=401, detail="Invalid or expired reset code")

    if not verify_otp(payload.code, code_hash, settings.OTP_SECRET):
        await increment_attempts(channel="email", identity=email, purpose=_PASSWORD_RESET_PURPOSE)
        raise HTTPException(status_code=401, detail="Invalid or expired reset code")

    # Atomic final gate: prevents concurrent reset requests from both succeeding.
    consumed_hash = await consume_otp_hash(channel="email", identity=email, purpose=_PASSWORD_RESET_PURPOSE)
    if not consumed_hash or not verify_otp(payload.code, consumed_hash, settings.OTP_SECRET):
        raise HTTPException(status_code=401, detail="Reset code already used")
    await clear_otp(channel="email", identity=email, purpose=_PASSWORD_RESET_PURPOSE)

    now = utc_now()
    await db.doctors.update_one(
        {"_id": doc["_id"]},
        {
            "$set": {
                "password_hash": hash_password(payload.new_password),
                "refresh_token_hash": None,
                "refresh_token_expires_at": None,
                "refresh_token_rotated_at": now,
                "tokens_invalidated_before": now,
                "failed_login_attempts": 0,
                "locked_until": None,
                "updated_at": now,
            }
        },
    )
    clear_auth_cookies(response, role="doctor")
    clear_admin_session_cookie(response)
    return {"message": "password_reset"}


@router.post(
    "/change-password",
    dependencies=[rl(settings.RL_DOCTOR_CHANGE_PASSWORD_TIMES, settings.RL_DOCTOR_CHANGE_PASSWORD_SECONDS)],
)
async def change_password(
    request: Request,
    response: Response,
    payload: ChangePasswordIn,
    current=Depends(get_current_doctor),
):
    db = get_db()

    doc = await db.doctors.find_one({"_id": current["_id"]}, {"password_hash": 1})
    if not doc:
        raise HTTPException(status_code=401, detail="Not authenticated")

    if not verify_password(payload.current_password, doc["password_hash"]):
        raise HTTPException(status_code=401, detail="Current password is incorrect")

    if payload.current_password == payload.new_password:
        raise HTTPException(status_code=400, detail="New password must be different from the current one")

    now = utc_now()

    # Blacklist the current access token so it's immediately dead.
    access_token = request.cookies.get("doctor_access_token")
    if access_token:
        try:
            token_payload = await _decode_access_token(access_token)
            jti = token_payload.get("jti")
            exp = token_payload.get("exp")
            if jti and exp:
                expires_at = datetime.fromtimestamp(float(exp), tz=timezone.utc)
                await blacklist_token(db, jti=jti, expires_at=expires_at, created_at=now)
        except Exception:
            pass

    await db.doctors.update_one(
        {"_id": current["_id"]},
        {
            "$set": {
                "password_hash": hash_password(payload.new_password),
                "refresh_token_hash": None,
                "refresh_token_expires_at": None,
                "refresh_token_rotated_at": now,
                "updated_at": now,
            }
        },
    )
    clear_auth_cookies(response, role="doctor")
    clear_admin_session_cookie(response)
    return {"message": "password_changed"}
