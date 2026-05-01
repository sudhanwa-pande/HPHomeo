import logging

from fastapi import Request

from app.utils.time import utc_now

logger = logging.getLogger(__name__)


async def log_audit(
    db,
    *,
    actor_type: str,
    actor_id,
    action: str,
    target_type: str,
    target_id,
    meta: dict | None = None,
    request: Request | None = None,
) -> None:
    try:
        ip = None
        user_agent = None
        if request:
            forwarded = request.headers.get("X-Forwarded-For")
            if forwarded:
                ip = forwarded.split(",")[0].strip()
            elif request.client:
                ip = request.client.host
            user_agent = request.headers.get("User-Agent")

        await db.audit_logs.insert_one(
            {
                "actor_type": actor_type,
                "actor_id": actor_id,
                "action": action,
                "target_type": target_type,
                "target_id": target_id,
                "meta": {
                    **(meta or {}),
                    "ip": ip,
                    "user_agent": user_agent,
                },
                "created_at": utc_now(),
            }
        )
    except Exception:
        logger.exception(
            "Audit log failed: action=%s target=%s target_id=%s",
            action,
            target_type,
            target_id,
        )
