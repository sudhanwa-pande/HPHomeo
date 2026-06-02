from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import OperationFailure
from app.core.config import settings

_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None


async def connect_db():
    global _client, _db
    _client = AsyncIOMotorClient(settings.MONGODB_URI.get_secret_value())
    _db = _client[settings.MONGODB_DB]
    await _db.command("ping")  # sanity check

    # ----------------------------
    # Doctors
    # ----------------------------
    await _db.doctors.create_index("email", unique=True)
    await _db.doctors.create_index("phone", unique=True)

    # NEW: registration number must be globally unique
    await _db.doctors.create_index("registration_no", unique=True)
    # Supports status lifecycle: pending -> approved/rejected
    await _db.doctors.create_index("verification_status")
    await _db.doctors.create_index("is_suspended")
    await _db.doctors.create_index("is_admin")
    await _db.doctors.create_index([("verification_status", 1), ("is_suspended", 1), ("_id", -1)])

    # ----------------------------
    # Patients (doctor-side patient records)
    # ----------------------------
    # If email can be None, sparse avoids unique collisions on null
    await _db.patients.create_index([("doctor_id", 1), ("email", 1)], unique=True, sparse=True)
    # Phone should be unique per doctor (prevents duplicates)
    await _db.patients.create_index([("doctor_id", 1), ("phone", 1)], unique=True)
    await _db.patients.create_index([("doctor_id", 1), ("created_at", -1)])
    await _db.patients.create_index([("doctor_id", 1), ("full_name", 1)])
    # $lookup from appointments.patient_user_id → patients.user_id (age distribution pipeline)
    await _db.patients.create_index("user_id")

    # ----------------------------
    # Patient users (auth identity)
    # ----------------------------
    await _db.patient_users.create_index("phone", unique=True)
    await _db.patient_users.create_index("refresh_token_hash")

    # ----------------------------
    # Access token blacklist (TTL)
    # ----------------------------
    await _db.token_blacklist.create_index("jti", unique=True)
    await _db.token_blacklist.create_index("expires_at", expireAfterSeconds=0)

    # ----------------------------
    # Doctor availability
    # ----------------------------
    await _db.doctor_availability.create_index("doctor_id", unique=True)

    # ----------------------------
    # Doctor availability exceptions
    # ----------------------------
    await _db.doctor_availability_exceptions.create_index(
        [("doctor_id", 1), ("date", 1)]
    )

    # ----------------------------
    # Appointments
    # ----------------------------

    # Patient history (public + hybrid)
    await _db.appointments.create_index([("patient_phone", 1), ("scheduled_at", -1)])
    # ✅ NEW: authenticated patient listing (hybrid query benefit)
    await _db.appointments.create_index([("patient_user_id", 1), ("scheduled_at", -1)])
    await _db.appointments.create_index([("patient_phone", 1), ("status", 1), ("scheduled_at", -1)])
    await _db.appointments.create_index([("patient_user_id", 1), ("status", 1), ("scheduled_at", -1)])

    # Slot collision checks / doctor schedule queries
    # Drop the old non-unique version so we can recreate it as a unique partial index.
    try:
        await _db.appointments.drop_index("doctor_id_1_scheduled_at_1")
    except OperationFailure:
        pass
    # Unique partial index — only active (confirmed / pending-payment) appointments
    # count toward the uniqueness constraint, so cancelled/completed rows don't block
    # rebooking a previously-cancelled slot.  This also turns the DuplicateKeyError
    # handler in the booking routes into a real race-condition safety net.
    await _db.appointments.create_index(
        [("doctor_id", 1), ("scheduled_at", 1)],
        unique=True,
        partialFilterExpression={
            "status": {"$in": ["confirmed", "pending_payment"]},
        },
        name="uq_doctor_slot_active",
    )

    # Doctor list filtering by status + time ordering
    await _db.appointments.create_index([("doctor_id", 1), ("status", 1), ("scheduled_at", 1)])

    # Active appointment check
    await _db.appointments.create_index([("doctor_id", 1), ("patient_phone", 1), ("status", 1), ("scheduled_at", 1)])

    # Payment hold expiry scans (APScheduler later)
    await _db.appointments.create_index([("status", 1), ("pending_payment_expires_at", 1)])

    # Revenue / paid-stats queries (doctor stats + daily revenue pipeline)
    await _db.appointments.create_index(
        [("doctor_id", 1), ("payment_status", 1), ("scheduled_at", 1)]
    )
    await _db.appointments.create_index(
        [("status", 1), ("scheduled_at", 1), ("email_reminder_24hr_sent", 1)],
        sparse=True,
    )
    await _db.appointments.create_index(
        [("status", 1), ("scheduled_at", 1), ("wa_reminder_12hr_sent", 1)],
        sparse=True,
    )
    await _db.appointments.create_index(
        [("is_follow_up_eligible", 1), ("follow_up_eligible_until", 1)],
        sparse=True,
    )
    try:
        await _db.appointments.create_index(
            [("call_status", 1), ("updated_at", 1)],
            name="idx_call_lifecycle"
        )
    except OperationFailure as e:
        if e.code == 86:
            # Drop the auto-generated conflicting index once
            await _db.appointments.drop_index("call_status_1_updated_at_1")
            await _db.appointments.create_index(
                [("call_status", 1), ("updated_at", 1)],
                name="idx_call_lifecycle"
            )
        else:
            raise
    await _db.appointments.create_index(
        [("refund_status", 1), ("payment_id", 1)],
        sparse=True,
    )
    # Video call lifecycle queries (optional global ops)
    await _db.appointments.create_index("call_status", sparse=True)
    try:
        await _db.appointments.drop_index("video_room_1")
    except OperationFailure:
        pass
    await _db.appointments.create_index(
        "video_room",
        unique=True,
        partialFilterExpression={"video_room": {"$type": "string"}},
    )
    
    # Refund lookup
    await _db.appointments.create_index(
        [("refund_id", 1)],
        sparse=True,
    )
    
    # Doctor scheduling timeline queries
    await _db.appointments.create_index(
        [("doctor_id", 1), ("scheduled_at", -1)]
    )

    # Magic token lookups (hashed storage)
    try:
        await _db.appointments.drop_index("patient_access_token_1")
    except OperationFailure:
        pass
    try:
        await _db.appointments.drop_index("patient_access_token_hash_1")
    except OperationFailure:
        pass
    await _db.appointments.create_index(
        [("patient_access_token_hash", 1)],
        unique=True,
        partialFilterExpression={"patient_access_token_hash": {"$type": "string"}},
    )
    # Webhook lookups
    await _db.appointments.create_index(
        [("payment_order_id", 1)],
        partialFilterExpression={"payment_order_id": {"$type": "string"}},
    )
    await _db.appointments.create_index(
        [("payment_id", 1)],
        partialFilterExpression={"payment_id": {"$type": "string"}},
    )

    # Payment idempotency guards (provider + gateway IDs)
    try:
        await _db.appointments.drop_index("payment_provider_1_payment_id_1")
    except OperationFailure:
        pass
    await _db.appointments.create_index(
        [("payment_provider", 1), ("payment_id", 1)],
        unique=True,
        partialFilterExpression={"payment_id": {"$type": "string"}},
    )

    try:
        await _db.appointments.drop_index("payment_provider_1_payment_order_id_1")
    except OperationFailure:
        pass
    await _db.appointments.create_index(
        [("payment_provider", 1), ("payment_order_id", 1)],
        unique=True,
        partialFilterExpression={"payment_order_id": {"$type": "string"}},
    )

    # Optional: reschedule linkage
    await _db.appointments.create_index([("rescheduled_from", 1)])

    # Review rating index (for doctor rating aggregation)
    await _db.appointments.create_index(
        [("doctor_id", 1), ("review.rating", 1)],
        sparse=True,
    )

    # ----------------------------
    # Doctor blocks
    # ----------------------------
    await _db.doctor_blocks.create_index([("doctor_id", 1), ("start_at", 1), ("end_at", 1)])

    # ----------------------------
    # Prescriptions (current state)
    # ----------------------------
    await _db.prescriptions.create_index("appointment_id", unique=True)
    await _db.prescriptions.create_index("doctor_id")
    await _db.prescriptions.create_index("patient_id")
    # Prescription patient query
    await _db.prescriptions.create_index([("appointment_id", 1), ("is_draft", 1)])
    # Prescription history
    await _db.prescriptions.create_index([("doctor_id", 1), ("patient_id", 1), ("created_at", -1)])

    # ----------------------------
    # Prescription versions (immutable snapshots)
    # ----------------------------
    await _db.prescription_versions.create_index("prescription_id")
    await _db.prescription_versions.create_index("appointment_id")
    await _db.prescription_versions.create_index("doctor_id")
    await _db.prescription_versions.create_index([("prescription_id", 1), ("version", -1)])

    # ----------------------------
    # Prescription templates
    # ----------------------------
    await _db.prescription_templates.create_index([("doctor_id", 1), ("created_at", -1)])

    
    # Follow-up: enforce only one active (non-cancelled) follow-up per original appointment.
    # Keep ObjectId guard to avoid null collisions from non-follow-up docs.
    # Use positive status match for compatibility with Mongo versions that reject negation in partial filters.
    try:
        await _db.appointments.drop_index("follow_up_of_appointment_id_1")
    except OperationFailure:
        pass

    await _db.appointments.create_index(
        [("follow_up_of_appointment_id", 1)],
        unique=True,
        partialFilterExpression={
            "follow_up_of_appointment_id": {"$type": "objectId"},
            "status": {"$in": ["pending_payment", "confirmed", "completed", "no_show"]},
        },
    )

    # ----------------------------
    # Receipts
    # ----------------------------
    await _db.receipts.create_index("appointment_id", unique=True)
    await _db.receipts.create_index("receipt_id", unique=True)
    await _db.receipts.create_index("doctor_id")
    await _db.receipts.create_index("patient_id", sparse=True)
    await _db.receipts.create_index([("patient_phone", 1), ("created_at", -1)])
    await _db.receipts.create_index([("patient_user_id", 1), ("created_at", -1)], sparse=True)
    await _db.receipts.create_index([("doctor_id", 1), ("patient_id", 1), ("created_at", -1)])

    # ----------------------------
    # RX ID counters
    # ----------------------------
    await _db.counters.create_index("name", unique=True)

    # ----------------------------
    # Doctor notes
    # ----------------------------
    await _db.doctor_notes.create_index([("doctor_id", 1), ("created_at", -1)])

    # ----------------------------
    # Audit logs
    # ----------------------------
    await _db.audit_logs.create_index([("actor_id", 1), ("created_at", -1)])
    await _db.audit_logs.create_index([("target_id", 1), ("created_at", -1)])
    await _db.audit_logs.create_index([("action", 1), ("created_at", -1)])
    # TTL: auto-delete audit log entries older than 90 days to prevent 512MB cap exhaustion
    await _db.audit_logs.create_index("created_at", expireAfterSeconds=60 * 60 * 24 * 90)


def get_db() -> AsyncIOMotorDatabase:
    if _db is None:
        raise RuntimeError("Database not initialized. Startup event not executed.")
    return _db


async def close_db():
    global _client, _db
    if _client is not None:
        _client.close()
    _client, _db = None, None
