from datetime import datetime, timezone
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from bson import ObjectId
from jose import JWTError

from app.core.config import settings
from app.core.database import get_db
from app.core.rate_limits import rl
from app.core.token_blacklist import blacklist_token, is_token_blacklisted
from app.core.security import (
    clear_auth_cookies,
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_token,
    set_auth_cookies,
)
from app.schemas.patient_auth_schema import (
    PatientProfileUpdateIn,
    PatientRequestOtpIn,
    PatientVerifyOtpIn,
    PatientMeOut,
)
from app.services.otp_redis_service import (
    OTP_MAX_ATTEMPTS,
    acquire_resend_cooldown,
    clear_otp,
    clear_resend_state,
    consume_otp_hash,
    get_attempts,
    get_otp_hash,
    get_resend_cooldown_remaining,
    increment_attempts,
    increment_resend_count,
    is_locked,
    store_otp,
)
from app.services.whatsapp_service import send_patient_login_otp_whatsapp
from app.utils.phone import normalize_phone_e164
from app.utils.otp import generate_otp, hash_otp, verify_otp
from app.utils.time import ensure_utc, utc_now

router = APIRouter(prefix="/patient/auth", tags=["Patient Auth"])
logger = logging.getLogger(__name__)


async def _phone_identifier(request: Request) -> str:
    # Cache the parsed body on request.state so the route handler's own
    # body parsing (via FastAPI's Pydantic model) reads the same bytes —
    # Starlette caches the raw bytes after first read, but we still want
    # a single JSON-parse attempt whose failure mode is explicit here.
    if not hasattr(request.state, "_parsed_body"):
        try:
            request.state._parsed_body = await request.json()
        except Exception:
            request.state._parsed_body = {}
    phone = str((request.state._parsed_body).get("phone") or "").strip()
    if phone:
        try:
            return f"phone:{normalize_phone_e164(phone)}"
        except ValueError:
            return f"phone:{phone}"
    return request.client.host if request.client else "unknown"


async def get_current_patient(request: Request):
    """Read patient access token from httpOnly cookie."""
    token = request.cookies.get("patient_access_token")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    db = get_db()
    try:
        payload = decode_token(token)
        if payload.get("type") == "refresh":
            raise HTTPException(status_code=401, detail="Invalid token")
        role = payload.get("role")
        if role != "patient":
            raise HTTPException(status_code=401, detail="Invalid token")
        jti = payload.get("jti")
        if not jti:
            raise HTTPException(status_code=401, detail="Invalid token")
        if await is_token_blacklisted(db, jti):
            raise HTTPException(status_code=401, detail="Token revoked")
        patient_user_id = payload.get("sub")
        if not patient_user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        token_iat = payload.get("iat")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        patient_oid = ObjectId(patient_user_id)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user = await db.patient_users.find_one({"_id": patient_oid})
    if not user:
        raise HTTPException(status_code=401, detail="Patient not found")
    tokens_invalidated_before = user.get("tokens_invalidated_before")
    if tokens_invalidated_before is not None:
        if token_iat is None or datetime.fromtimestamp(float(token_iat), tz=timezone.utc) < ensure_utc(tokens_invalidated_before):
            raise HTTPException(status_code=401, detail="Session invalidated. Please log in again.")
    return user


@router.post(
    "/request-otp",
    dependencies=[
        rl(
            settings.RL_PATIENT_OTP_PHONE_TIMES,
            settings.RL_PATIENT_OTP_PHONE_SECONDS,
            identifier=_phone_identifier,
        ),
        rl(settings.RL_PATIENT_OTP_GENERAL_TIMES, settings.RL_PATIENT_OTP_GENERAL_SECONDS),
    ],
)
async def request_otp(request: Request, data: PatientRequestOtpIn):
    channel = "phone"
    identity = data.phone
    cooldown_seconds = int(getattr(settings, "OTP_RESEND_COOLDOWN_SECONDS", 60))
    ttl_seconds = int(getattr(settings, "OTP_TTL_MINUTES", 5)) * 60

    acquired = await acquire_resend_cooldown(
        channel=channel,
        identity=identity,
        purpose=data.purpose,
        ttl_seconds=cooldown_seconds,
    )
    if not acquired:
        retry_after = await get_resend_cooldown_remaining(
            channel=channel,
            identity=identity,
            purpose=data.purpose,
        )
        raise HTTPException(
            status_code=429,
            detail={
                "message": "otp_recently_sent",
                "retry_after_seconds": retry_after,
            },
            headers={"Retry-After": str(max(1, retry_after))},
        )

    code = generate_otp(6)
    code_hash = hash_otp(code, settings.OTP_SECRET)
    await store_otp(
        channel=channel,
        identity=identity,
        purpose=data.purpose,
        code_hash=code_hash,
        ttl_seconds=ttl_seconds,
    )
    try:
        await send_patient_login_otp_whatsapp(
            data.phone,
            code,
            int(getattr(settings, "OTP_TTL_MINUTES", 5)),
        )
    except Exception:
        await clear_otp(channel=channel, identity=identity, purpose=data.purpose)
        await clear_resend_state(channel=channel, identity=identity, purpose=data.purpose)
        logger.warning("OTP send failed for phone=%s purpose=%s", data.phone, data.purpose, exc_info=True)
        raise HTTPException(
            status_code=502,
            detail={"message": "otp_send_failed"},
        )

    resend_count = await increment_resend_count(
        channel=channel,
        identity=identity,
        purpose=data.purpose,
        ttl_seconds=ttl_seconds,
    )
    logger.info("OTP generated for phone=%s purpose=%s", data.phone, data.purpose)
    return {
        "message": "otp_sent",
        "retry_after_seconds": cooldown_seconds,
        "resend_count": resend_count,
    }


@router.post(
    "/verify-otp",
    dependencies=[rl(settings.RL_PATIENT_VERIFY_OTP_TIMES, settings.RL_PATIENT_VERIFY_OTP_SECONDS)],
)
async def verify_otp_route(request: Request, response: Response, data: PatientVerifyOtpIn):
    db = get_db()
    now = utc_now()
    channel = "phone"
    identity = data.phone

    attempts = await get_attempts(channel=channel, identity=identity, purpose=data.purpose)
    if is_locked(attempts):
        raise HTTPException(
            status_code=429,
            detail=f"Too many invalid OTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
        )

    code_hash = await get_otp_hash(channel=channel, identity=identity, purpose=data.purpose)
    if not code_hash:
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    if not verify_otp(data.code, code_hash, settings.OTP_SECRET):
        attempts = await increment_attempts(channel=channel, identity=identity, purpose=data.purpose)
        if is_locked(attempts):
            raise HTTPException(
                status_code=429,
                detail=f"Too many invalid OTP attempts. Try again later (max {OTP_MAX_ATTEMPTS}).",
            )
        raise HTTPException(status_code=401, detail="Invalid or expired OTP")

    # Atomic final gate: GET+DEL in one Lua script prevents two concurrent
    # requests with the same OTP from both proceeding to session creation.
    consumed_hash = await consume_otp_hash(channel=channel, identity=identity, purpose=data.purpose)
    if not consumed_hash or not verify_otp(data.code, consumed_hash, settings.OTP_SECRET):
        raise HTTPException(status_code=401, detail="OTP already used")
    await clear_otp(channel=channel, identity=identity, purpose=data.purpose)  # clean up attempts/resend keys

    user = await db.patient_users.find_one({"phone": data.phone})
    if not user:
        user_doc = {
            "phone": data.phone,
            "full_name": None,
            "email": None,  # keep explicit
            "is_phone_verified": True,
            "created_at": now,
            "updated_at": now,
            "last_login_at": now,
        }
        res = await db.patient_users.insert_one(user_doc)
        user_id = res.inserted_id
        user = user_doc
    else:
        user_id = user["_id"]
        await db.patient_users.update_one(
            {"_id": user_id},
            {
                "$set": {
                    "phone": data.phone,
                    "is_phone_verified": True,
                    "last_login_at": now,
                    "updated_at": now,
                }
            },
        )

    # Link legacy phone-based bookings to this authenticated patient without overriding existing ownership.
    await db.appointments.update_many(
        {
            "patient_phone": data.phone,
            "$or": [
                {"patient_user_id": {"$exists": False}},
                {"patient_user_id": None},
            ],
        },
        {"$set": {"patient_user_id": user_id, "updated_at": now}},
    )

    # Backfill profile from latest linked appointment only for missing fields.
    latest_appt = await db.appointments.find_one(
        {
            "patient_user_id": user_id,
            "patient_phone": data.phone,
        },
        sort=[("created_at", -1)],
    )
    if latest_appt:
        profile_updates: dict = {"updated_at": now}
        if not user.get("full_name") and latest_appt.get("patient_name"):
            profile_updates["full_name"] = latest_appt.get("patient_name")
        if not user.get("email") and latest_appt.get("patient_email"):
            profile_updates["email"] = latest_appt.get("patient_email")
        if user.get("age") is None and latest_appt.get("patient_age") is not None:
            profile_updates["age"] = latest_appt.get("patient_age")
        if user.get("sex") is None and latest_appt.get("patient_sex"):
            profile_updates["sex"] = latest_appt.get("patient_sex")
        if len(profile_updates) > 1:
            await db.patient_users.update_one({"_id": user_id}, {"$set": profile_updates})

    access_token = create_access_token({"sub": str(user_id), "role": "patient"})
    refresh_token = create_refresh_token({"sub": str(user_id), "role": "patient"})
    refresh_payload = decode_token(refresh_token)
    refresh_exp = refresh_payload.get("exp")
    refresh_expires_at = now
    if isinstance(refresh_exp, (int, float)):
        refresh_expires_at = datetime.fromtimestamp(refresh_exp, tz=timezone.utc)

    await db.patient_users.update_one(
        {"_id": user_id},
        {
            "$set": {
                "refresh_token_hash": hash_token(refresh_token),
                "refresh_token_expires_at": refresh_expires_at,
                "refresh_token_rotated_at": now,
                "updated_at": now,
            }
        },
    )

    set_auth_cookies(
        response,
        access_token=access_token,
        refresh_token=refresh_token,
        role="patient",
    )
    return {
        "message": "authenticated",
        "patient": {
            "patient_user_id": str(user_id),
            "phone": data.phone,
            "full_name": user.get("full_name"),
            "email": user.get("email"),
            "age": user.get("age"),
            "sex": user.get("sex"),
        },
    }


@router.post(
    "/refresh",
    dependencies=[rl(settings.RL_PATIENT_REFRESH_TIMES, settings.RL_PATIENT_REFRESH_SECONDS)],
)
async def patient_refresh(request: Request, response: Response):
    """Refresh patient tokens. Reads refresh token from httpOnly cookie."""
    refresh_token = request.cookies.get("patient_refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="No refresh token")

    db = get_db()
    now = utc_now()
    try:
        decoded = decode_token(refresh_token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    if decoded.get("type") != "refresh" or decoded.get("role") != "patient":
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    patient_id = decoded.get("sub")
    if not patient_id:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    try:
        patient_oid = ObjectId(patient_id)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    user = await db.patient_users.find_one({"_id": patient_oid})
    if not user or user.get("refresh_token_hash") != hash_token(refresh_token):
        raise HTTPException(status_code=401, detail="Refresh token revoked")
    refresh_token_expires_at = ensure_utc(user.get("refresh_token_expires_at"))
    if refresh_token_expires_at and refresh_token_expires_at <= now:
        raise HTTPException(status_code=401, detail="Refresh token expired")

    new_access = create_access_token({"sub": patient_id, "role": "patient"})
    new_refresh = create_refresh_token({"sub": patient_id, "role": "patient"})
    new_refresh_payload = decode_token(new_refresh)
    new_refresh_exp = new_refresh_payload.get("exp")
    new_refresh_expires_at = now
    if isinstance(new_refresh_exp, (int, float)):
        new_refresh_expires_at = datetime.fromtimestamp(new_refresh_exp, tz=timezone.utc)

    await db.patient_users.update_one(
        {"_id": user["_id"]},
        {
            "$set": {
                "refresh_token_hash": hash_token(new_refresh),
                "refresh_token_expires_at": new_refresh_expires_at,
                "refresh_token_rotated_at": now,
                "updated_at": now,
            }
        },
    )

    set_auth_cookies(
        response,
        access_token=new_access,
        refresh_token=new_refresh,
        role="patient",
    )
    return {"message": "refreshed"}


@router.post(
    "/logout",
    dependencies=[rl(settings.RL_PATIENT_LOGOUT_TIMES, settings.RL_PATIENT_LOGOUT_SECONDS)],
)
async def patient_logout(request: Request, response: Response):
    """Logout patient. Reads access token from cookie, blacklists it, clears cookies."""
    access_token = request.cookies.get("patient_access_token")
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    db = get_db()
    now = utc_now()
    try:
        payload = decode_token(access_token)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    if payload.get("type") == "refresh" or payload.get("role") != "patient":
        raise HTTPException(status_code=401, detail="Invalid token")

    jti = payload.get("jti")
    exp = payload.get("exp")
    sub = payload.get("sub")
    if not jti or exp is None or not sub:
        raise HTTPException(status_code=401, detail="Invalid token")
    if not isinstance(exp, (int, float)):
        raise HTTPException(status_code=401, detail="Invalid token")

    expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
    await blacklist_token(db, jti=jti, expires_at=expires_at, created_at=now)
    clear_set = {
        "refresh_token_hash": None,
        "refresh_token_expires_at": None,
        "refresh_token_rotated_at": now,
        "updated_at": now,
    }
    try:
        await db.patient_users.update_one({"_id": ObjectId(sub)}, {"$set": clear_set})
    except Exception:
        pass

    clear_auth_cookies(response, role="patient")
    return {"message": "logged_out"}


@router.get(
    "/me",
    response_model=PatientMeOut,
    dependencies=[rl(settings.RL_PATIENT_ME_TIMES, settings.RL_PATIENT_ME_SECONDS)],
)
async def patient_me(request: Request, current=Depends(get_current_patient)):
    return {
        "patient_user_id": str(current["_id"]),
        "phone": current.get("phone"),
        "full_name": current.get("full_name"),
        "email": current.get("email"),
        "age": current.get("age"),
        "sex": current.get("sex"),
    }

@router.patch(
    "/profile",
    dependencies=[rl(settings.RL_PATIENT_PROFILE_UPDATE_TIMES, settings.RL_PATIENT_PROFILE_UPDATE_SECONDS)],
)
async def update_patient_profile(
    request: Request,
    payload: PatientProfileUpdateIn,
    current=Depends(get_current_patient),
):
    db = get_db()
    now = utc_now()
    update_fields = payload.model_dump(exclude_none=True)
    if not update_fields:
        return {"message": "no_changes"}
    update_fields["updated_at"] = now
    await db.patient_users.update_one(
        {"_id": current["_id"]},
        {"$set": update_fields},
    )

    return {"message": "profile_updated"}
