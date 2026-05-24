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


from livekit.api import AccessToken, VideoGrants

def create_video_token(
    *,
    room: str,
    identity: str,
    metadata: dict[str, Any],
    ttl_seconds: int | None = None,
) -> str:
    if not settings.LIVEKIT_URL or not settings.LIVEKIT_API_KEY or not settings.LIVEKIT_API_SECRET.get_secret_value():
        raise HTTPException(status_code=500, detail="Video provider is not configured")

    grant = VideoGrants(
        room_join=True,
        room=room,
        can_publish=True,
        can_subscribe=True,
        can_publish_data=True,
    )
    
    access_token = (
        AccessToken(
            settings.LIVEKIT_API_KEY,
            settings.LIVEKIT_API_SECRET.get_secret_value()
        )
        .with_identity(identity)
        .with_metadata(json.dumps(metadata))
        .with_grants(grant)
        .with_ttl(timedelta(seconds=int(ttl_seconds or settings.LIVEKIT_TOKEN_TTL_SECONDS)))
    )
    
    return access_token.to_jwt()


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
            
            async with api.LiveKitAPI(settings.LIVEKIT_URL, settings.LIVEKIT_API_KEY, settings.LIVEKIT_API_SECRET.get_secret_value()) as lkapi:
                req = api.CreateRoomRequest(name=room_name, empty_timeout=30*60, max_participants=2)
                await lkapi.room.create_room(req)
                
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
