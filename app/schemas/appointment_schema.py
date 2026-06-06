from datetime import datetime
from typing import Optional, Literal

from pydantic import BaseModel, Field, field_validator

from app.utils.phone import normalize_phone_e164


# -----------------------------
# Status & Payment Types
# -----------------------------

AppointmentStatus = Literal[
    "pending_payment",
    "confirmed",
    "completed",
    "cancelled",
    "no_show",
]

AppointmentType = Literal[
    "new",
    "follow_up",
]

PaymentStatus = Literal[
    "unpaid",
    "pending",
    "paid",
    "transferred",
    "refunded",
    "failed",
]

RefundStatus = Literal[
    "none",
    "pending",
    "processing",
    "processed",
    "failed",
]

PaymentProvider = Literal[
    "razorpay",
    "test",
]


# -----------------------------
# Booking Input (Public)
# Follow-up NOT allowed publicly
# -----------------------------

class AppointmentBook(BaseModel):
    doctor_id: str
    patient_name: str = Field(min_length=2, max_length=100)
    patient_phone: str
    patient_age: int = Field(..., ge=1, le=120)
    patient_sex: Literal["male", "female", "other"]
    patient_email: Optional[str] = None
    scheduled_at: str
    mode: Literal["online", "walk_in"]
    payment_choice: Literal["pay_now", "pay_at_clinic"]

    # Public booking is always NEW
    appointment_type: Literal["new"] = Field(default="new")

    @field_validator("patient_phone")
    @classmethod
    def normalize_patient_phone(cls, value: str) -> str:
        return normalize_phone_e164(value)


# -----------------------------
# Booking Input (Authenticated Patient)
# Follow-up allowed here
# -----------------------------

class PatientAppointmentBookIn(BaseModel):
    doctor_id: str
    scheduled_at: str
    mode: Literal["online", "walk_in"]
    payment_choice: Literal["pay_now", "pay_at_clinic"]

    appointment_type: AppointmentType = Field(default="new")

    # Required only if appointment_type == "follow_up"
    follow_up_of_appointment_id: Optional[str] = None


# -----------------------------
# Booking Response
# -----------------------------

class AppointmentBookedOut(BaseModel):
    appointment_id: str
    status: AppointmentStatus
    payment_choice: Literal["pay_now", "pay_at_clinic"]
    patient_access_token: str
    patient_access_expires_at: datetime
    message: str


# -----------------------------
# Doctor Cancel Input
# -----------------------------

class DoctorCancelAppointmentIn(BaseModel):
    reason: str = Field(..., min_length=3, max_length=300)


# -----------------------------
# Patient Cancel Input (Token-based)
# -----------------------------

class PatientCancelIn(BaseModel):
    reason: Optional[str] = Field(default=None, max_length=300)


# -----------------------------
# Payment Confirmation
# -----------------------------

class PaymentConfirmIn(BaseModel):
    provider: PaymentProvider = "test"
    payment_id: Optional[str] = Field(default=None, max_length=120)
    signature: Optional[str] = Field(default=None, max_length=200)
    order_id: Optional[str] = Field(default=None, max_length=120)


class PaymentCreateOrderIn(BaseModel):
    appointment_id: str

class PaymentVerifyIn(BaseModel):
    appointment_id: str
    razorpay_payment_id: str
    razorpay_order_id: str
    razorpay_signature: str


# -----------------------------
# Public Appointment View
# -----------------------------

class PublicAppointmentView(BaseModel):
    appointment_id: str
    doctor_id: str
    doctor_name: Optional[str] = None
    patient_name: Optional[str] = None
    scheduled_at: datetime
    duration_min: int
    mode: Literal["walk_in", "online"]
    status: AppointmentStatus
    payment_choice: Literal["pay_now", "pay_at_clinic"]
    consultation_fee: Optional[int] = None
    video_enabled: bool = False
    call_status: Literal["idle", "waiting", "initializing", "connected", "disconnected", "ended"] = "idle"

    # Useful for UI
    appointment_type: AppointmentType = "new"
    follow_up_of_appointment_id: Optional[str] = None
    can_cancel: bool = False
    can_reschedule: bool = False
    cancel_window_hours: int = 2


# -----------------------------
# Patient Reschedule Input
# -----------------------------

class PatientRescheduleIn(BaseModel):
    new_scheduled_at: str = Field(
        ...,
        description="ISO datetime e.g. 2026-03-02T09:00:00"
    )
    reason: Optional[str] = Field(default=None, max_length=300)


class PublicAppointmentAccessIn(BaseModel):
    token: str = Field(min_length=20, max_length=512)
