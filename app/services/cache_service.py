import hashlib
import json
import logging
from datetime import date, datetime
from typing import Any

from bson import ObjectId

from app.core.redis import get_redis

logger = logging.getLogger(__name__)

TTL_2_MINUTES = 120
TTL_5_MINUTES = 300
TTL_10_MINUTES = 600
TTL_30_MINUTES = 1800
TTL_1_HOUR = 3600


def doctors_list_key(*, q: str | None, limit: int, skip: int) -> str:
    raw = f"{(q or '').strip().lower()}|{limit}|{skip}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"doctors:list:{digest}"


def doctor_profile_key(doctor_id: str) -> str:
    return f"doctor:profile:{doctor_id}"


def doctor_public_profile_key(doctor_id: str) -> str:
    return f"doctor:public_profile:{doctor_id}"


def slots_key(doctor_id: str, day: str) -> str:
    return f"slots:{doctor_id}:{day}"


def doctor_availability_key(doctor_id: str) -> str:
    return f"doctor:availability:{doctor_id}"


def doctor_appointments_key(doctor_id: str, day: str) -> str:
    return f"doctor:appointments:{doctor_id}:{day}"


def doctor_patients_key(doctor_id: str) -> str:
    return f"doctor:patients:{doctor_id}"


def doctor_templates_key(doctor_id: str) -> str:
    return f"doctor:templates:{doctor_id}"


def patient_appointments_key(patient_id: str) -> str:
    return f"patient:appointments:{patient_id}"


def patient_appointments_list_key(
    patient_id: str,
    *,
    upcoming: bool,
    limit: int,
    skip: int,
) -> str:
    raw = f"{int(upcoming)}|{limit}|{skip}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{patient_appointments_key(patient_id)}:{digest}"


def patient_appointment_detail_key(patient_id: str, appointment_id: str) -> str:
    return f"{patient_appointments_key(patient_id)}:detail:{appointment_id}"


def patient_prescriptions_key(patient_id: str) -> str:
    return f"patient:prescriptions:{patient_id}"


def prescription_data_key(prescription_id: str) -> str:
    return f"prescription:data:{prescription_id}"


def prescription_signed_url_key(prescription_id: str) -> str:
    return f"prescription:signed_url:{prescription_id}"


def patient_receipts_key(patient_id: str) -> str:
    return f"patient:receipts:{patient_id}"


def receipt_data_key(receipt_id: str) -> str:
    return f"receipt:data:{receipt_id}"


def receipt_signed_url_key(receipt_id: str) -> str:
    return f"receipt:signed_url:{receipt_id}"


def doctor_stats_key(doctor_id: str) -> str:
    return f"doctor:stats:{doctor_id}"


def doctor_daily_stats_key(doctor_id: str, days: int) -> str:
    return f"doctor:stats:daily:{days}:{doctor_id}"


async def cache_get_json(key: str) -> Any | None:
    try:
        redis = get_redis()
        value = await redis.get(key)
        if not value:
            return None
        return json.loads(value)
    except Exception:
        logger.exception("Cache get failed", extra={"cache_key": key})
        return None


async def cache_set_json(key: str, value: Any, ttl_seconds: int = TTL_5_MINUTES) -> None:
    if ttl_seconds <= 0:
        raise ValueError(
            f"cache_set_json requires a positive ttl_seconds, got {ttl_seconds!r} for key={key!r}. "
            "All cache keys must have an explicit TTL to prevent unbounded Redis memory growth."
        )

    def _json_default(v: Any):
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        if isinstance(v, ObjectId):
            return str(v)
        return str(v)

    try:
        redis = get_redis()
        await redis.set(key, json.dumps(value, default=_json_default), ex=ttl_seconds)
    except Exception:
        logger.exception("Cache set failed", extra={"cache_key": key, "ttl_seconds": ttl_seconds})


async def cache_delete_keys(*keys: str) -> None:
    try:
        valid_keys = [k for k in keys if k]
        if not valid_keys:
            return
        redis = get_redis()
        await redis.delete(*valid_keys)
    except Exception:
        logger.exception("Cache delete failed", extra={"cache_keys": list(keys)})


async def cache_delete_pattern(pattern: str, batch_size: int = 200) -> None:
    try:
        redis = get_redis()
        cursor = 0
        while True:
            cursor, keys = await redis.scan(cursor=cursor, match=pattern, count=batch_size)
            if keys:
                await redis.delete(*keys)
            if cursor == 0:
                break
    except Exception:
        logger.exception("Cache delete pattern failed", extra={"pattern": pattern})


async def invalidate_doctor_cache(
    doctor_id: str,
    day: str | date | None = None,
    *,
    invalidate_list: bool = False,
) -> None:
    doctor_id = str(doctor_id)
    normalized_day = day.isoformat() if isinstance(day, date) else day

    await cache_delete_keys(
        doctor_profile_key(doctor_id),
        doctor_public_profile_key(doctor_id),
        doctor_availability_key(doctor_id),
        doctor_patients_key(doctor_id),
        doctor_templates_key(doctor_id),
        doctor_stats_key(doctor_id),
    )
    # Invalidate all daily stats variants (days=1, 7, 30, etc.)
    await cache_delete_pattern(f"doctor:stats:daily:*:{doctor_id}")

    if invalidate_list:
        await cache_delete_pattern("doctors:list:*")

    if normalized_day:
        await cache_delete_keys(
            slots_key(doctor_id, normalized_day),
            doctor_appointments_key(doctor_id, normalized_day),
        )
        return

    await cache_delete_pattern(f"slots:{doctor_id}:*")
    await cache_delete_pattern(f"doctor:appointments:{doctor_id}:*")


async def invalidate_patient_cache(patient_id: str) -> None:
    patient_id = str(patient_id)
    appt_prefix = patient_appointments_key(patient_id)
    await cache_delete_keys(
        patient_prescriptions_key(patient_id),
        patient_receipts_key(patient_id),
        appt_prefix,
    )
    await cache_delete_pattern(f"{appt_prefix}:*")


async def invalidate_prescription_cache(prescription_id: str) -> None:
    prescription_id = str(prescription_id)
    await cache_delete_keys(
        prescription_data_key(prescription_id),
        prescription_signed_url_key(prescription_id),
    )


async def invalidate_receipt_cache(receipt_db_id: str) -> None:
    receipt_db_id = str(receipt_db_id)
    await cache_delete_keys(
        receipt_data_key(receipt_db_id),
        receipt_signed_url_key(receipt_db_id),
    )
