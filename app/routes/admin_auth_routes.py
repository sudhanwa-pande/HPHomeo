import pyotp
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request, Response

from app.core.config import settings
from app.core.rate_limits import rl
from app.core.security import (
    clear_admin_session_cookie,
    create_access_token,
    decode_token,
    decrypt_secret,
    set_admin_session_cookie,
)
from app.routes.auth_routes import get_current_admin
from app.schemas.doctor_schema import TotpEnableIn
from app.core.database import get_db
from app.core.token_blacklist import is_token_blacklisted
from app.services.audit_service import log_audit
from app.services.cache_service import invalidate_doctor_cache
from app.utils.time import utc_now

router = APIRouter(prefix="/admin/auth", tags=["Admin Auth"])

ADMIN_SESSION_MINUTES = 30


async def verify_admin_session(
    request: Request,
    current=Depends(get_current_admin),
):
    """Read admin session token from httpOnly cookie instead of header."""
    db = get_db()

    # Admin-sensitive actions must be TOTP-gated.
    if not current.get("totp_enabled"):
        raise HTTPException(status_code=403, detail="Admin TOTP setup required")

    admin_session_token = request.cookies.get("admin_session_token")
    if not admin_session_token:
        raise HTTPException(status_code=403, detail="Admin re-auth required")

    try:
        payload = decode_token(admin_session_token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid admin session token")

    if payload.get("type") == "refresh":
        raise HTTPException(status_code=401, detail="Invalid admin session token")
    if payload.get("session_type") != "admin_reauth":
        raise HTTPException(status_code=401, detail="Invalid admin session token")
    if payload.get("role") != "doctor" or not bool(payload.get("is_admin", False)):
        raise HTTPException(status_code=403, detail="Admin access required")
    if str(payload.get("sub")) != str(current["_id"]):
        raise HTTPException(status_code=401, detail="Admin session does not match access token")
    if await is_token_blacklisted(db, payload.get("jti")):
        raise HTTPException(status_code=401, detail="Admin session revoked")
    return current


@router.post(
    "/verify",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def verify_admin_reauth(
    request: Request,
    response: Response,
    payload: TotpEnableIn,
    current=Depends(get_current_admin),
):
    if not current.get("totp_enabled"):
        raise HTTPException(status_code=403, detail="Admin TOTP setup required")

    encrypted = current.get("totp_secret")
    if not encrypted:
        raise HTTPException(status_code=400, detail="Invalid TOTP setup state")
    try:
        secret = decrypt_secret(encrypted)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid TOTP setup state")

    if not pyotp.TOTP(secret).verify(payload.code, valid_window=1):
        raise HTTPException(status_code=401, detail="Invalid TOTP code")

    token = create_access_token(
        {
            "sub": str(current["_id"]),
            "role": "doctor",
            "is_admin": True,
            "amr": "totp",
            "session_type": "admin_reauth",
        },
        expires_minutes=ADMIN_SESSION_MINUTES,
    )

    set_admin_session_cookie(
        response,
        token=token,
        max_age=ADMIN_SESSION_MINUTES * 60,
    )
    return {
        "message": "admin_session_created",
        "expires_in_seconds": ADMIN_SESSION_MINUTES * 60,
    }


@router.post(
    "/logout",
    dependencies=[rl(settings.RL_ADMIN_LOGOUT_TIMES, settings.RL_ADMIN_LOGOUT_SECONDS)],
)
async def logout_admin_session(
    response: Response,
    current=Depends(get_current_admin),
):
    clear_admin_session_cookie(response)
    return {"message": "admin_session_cleared"}


@router.post(
    "/admins/{doctor_id}/grant",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def grant_admin_privilege(
    request: Request,
    doctor_id: str,
    current=Depends(verify_admin_session),
):
    db = get_db()
    now = utc_now()

    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")

    update_res = await db.doctors.update_one(
        {
            "_id": doctor_oid,
            "is_suspended": {"$ne": True},
            "is_admin": {"$ne": True},
        },
        {
            "$set": {
                "is_admin": True,
                "role": "doctor",
                "updated_at": now,
                "admin_granted_at": now,
                "admin_granted_by": current["_id"],
            }
        },
    )
    if update_res.modified_count == 0:
        doc = await db.doctors.find_one({"_id": doctor_oid}, {"_id": 1, "is_admin": 1, "is_suspended": 1})
        if not doc:
            raise HTTPException(status_code=404, detail="Doctor not found")
        if doc.get("is_suspended"):
            raise HTTPException(status_code=409, detail="Cannot grant admin to suspended doctor")
        raise HTTPException(status_code=409, detail="Doctor already has admin privilege")

    await log_audit(
        db,
        actor_type="admin",
        actor_id=current["_id"],
        action="grant_admin_privilege",
        target_type="doctor",
        target_id=doctor_oid,
        meta={"is_admin": True},
        request=request,
    )
    await invalidate_doctor_cache(str(doctor_oid), invalidate_list=True)

    return {
        "message": "admin_privilege_granted",
        "doctor_id": doctor_id,
        "is_admin": True,
    }


@router.post(
    "/admins/{doctor_id}/revoke",
    dependencies=[rl(settings.RL_ADMIN_WRITE_TIMES, settings.RL_ADMIN_WRITE_SECONDS)],
)
async def revoke_admin_privilege(
    request: Request,
    doctor_id: str,
    current=Depends(verify_admin_session),
):
    db = get_db()
    now = utc_now()

    try:
        doctor_oid = ObjectId(doctor_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid doctor_id")

    if doctor_oid == current["_id"]:
        raise HTTPException(status_code=409, detail="You cannot revoke your own admin privilege")

    update_res = await db.doctors.update_one(
        {
            "_id": doctor_oid,
            "is_admin": True,
        },
        {
            "$set": {
                "is_admin": False,
                "role": "doctor",
                "updated_at": now,
                "admin_revoked_at": now,
                "admin_revoked_by": current["_id"],
            }
        },
    )
    if update_res.modified_count == 0:
        doc = await db.doctors.find_one({"_id": doctor_oid}, {"_id": 1, "is_admin": 1})
        if not doc:
            raise HTTPException(status_code=404, detail="Doctor not found")
        raise HTTPException(status_code=409, detail="Doctor does not have admin privilege")

    await log_audit(
        db,
        actor_type="admin",
        actor_id=current["_id"],
        action="revoke_admin_privilege",
        target_type="doctor",
        target_id=doctor_oid,
        meta={"is_admin": False},
        request=request,
    )
    await invalidate_doctor_cache(str(doctor_oid), invalidate_list=True)

    return {
        "message": "admin_privilege_revoked",
        "doctor_id": doctor_id,
        "is_admin": False,
    }
