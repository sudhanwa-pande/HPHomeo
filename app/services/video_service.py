"""
Video Service — LiveKit token generation and room management.

State management has moved to call_state_machine.py.
This module only handles:
  - Token creation (JWT for LiveKit)
  - Room name generation and persistence
  - Payment validation for video calls
"""

import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException
from jose import jwt

from app.core.config import settings


def generate_room_name(appointment_id: str) -> str:
    suffix = secrets.token_hex(4)
    return f"appt_{appointment_id}_{suffix}"


def create_video_token(
    *,
    room: str,
    identity: str,
    metadata: dict[str, Any],
    ttl_seconds: int | None = None,
) -> str:
    if not settings.LIVEKIT_URL or not settings.LIVEKIT_API_KEY or not settings.LIVEKIT_API_SECRET.get_secret_value():
        raise HTTPException(status_code=500, detail="Video provider is not configured")

    now = datetime.now(timezone.utc)
    exp = now + timedelta(seconds=int(ttl_seconds or settings.LIVEKIT_TOKEN_TTL_SECONDS))
    payload = {
        "iss": settings.LIVEKIT_API_KEY,
        "sub": identity,
        "nbf": int(now.timestamp()),
        "exp": int(exp.timestamp()),
        "metadata": json.dumps(metadata),
        "video": {
            "roomJoin": True,
            "room": room,
            "canPublish": True,
            "canSubscribe": True,
            "canPublishData": True,
        },
    }
    return jwt.encode(payload, settings.LIVEKIT_API_SECRET.get_secret_value(), algorithm="HS256")


async def ensure_video_room(db, appointment: dict) -> str:
    if appointment.get("video_room"):
        return str(appointment["video_room"])

    from pymongo import ReturnDocument
    from app.utils.time import utc_now

    room_name = generate_room_name(str(appointment["_id"]))
    updated = await db.appointments.find_one_and_update(
        {
            "_id": appointment["_id"],
            "$or": [
                {"video_room": {"$exists": False}},
                {"video_room": None},
                {"video_room": ""},
            ],
        },
        {"$set": {"video_room": room_name, "video_provider": "livekit", "updated_at": utc_now()}},
        return_document=ReturnDocument.AFTER,
    )

    if updated and updated.get("video_room"):
        room_name = str(updated["video_room"])
        try:
            from livekit import api
            import logging
            logger = logging.getLogger(__name__)
            room_client = api.RoomService(settings.LIVEKIT_URL, settings.LIVEKIT_API_KEY, settings.LIVEKIT_API_SECRET.get_secret_value())
            req = api.CreateRoomRequest(name=room_name, empty_timeout=5*60, max_participants=2)
            await room_client.create_room(req)
            await room_client.aclose()
            logger.info("livekit_room_created", extra={"room": room_name, "max_participants": 2})
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error("livekit_room_creation_failed", extra={"room": room_name, "error": str(e)})

        return room_name

    # If another request already set it concurrently
    appt_latest = await db.appointments.find_one({"_id": appointment["_id"]}, {"video_room": 1})
    if not appt_latest or not appt_latest.get("video_room"):
        raise HTTPException(status_code=500, detail="Failed to initialize video room")
    return str(appt_latest["video_room"])


def check_video_payment(appointment: dict, role: str = "patient") -> None:
    status = appointment.get("status")
    payment_choice = appointment.get("payment_choice")
    payment_status = appointment.get("payment_status")

    if status not in {"confirmed", "connected"}:
        raise HTTPException(status_code=403, detail="Appointment is not confirmed")

    if payment_choice == "pay_at_clinic":
        return
    if payment_status != "paid":
        raise HTTPException(status_code=403, detail="Payment required to join")
