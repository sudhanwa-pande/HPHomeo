import asyncio
import base64
import logging
from pathlib import Path

import httpx
import resend
from jinja2 import Environment, FileSystemLoader, select_autoescape

from app.core.config import settings
from app.utils.magic_token import decrypt_magic_token
from app.utils.time import format_ist

resend.api_key = settings.RESEND_API_KEY
logger = logging.getLogger(__name__)

_jinja_env = Environment(
    loader=FileSystemLoader("app/templates/emails"),
    autoescape=select_autoescape(["html"]),
)

_LOGO_CID = "hphomeo-logo"
_LOGO_PATH = Path(__file__).parent.parent / "static" / "images" / "logo.png"

def _logo_attachment() -> dict | None:
    try:
        content = base64.b64encode(_LOGO_PATH.read_bytes()).decode("ascii")
        return {
            "filename": "logo.png",
            "content": content,
            "content_type": "image/png",
            "content_id": _LOGO_CID,
            "inline": True,
        }
    except OSError:
        logger.warning("Logo not found at %s — emails will render without logo", _LOGO_PATH)
        return None


def _with_logo(payload: dict) -> dict:
    logo = _logo_attachment()
    if logo:
        payload["attachments"] = [logo] + list(payload.get("attachments", []))
    return payload


def _magic_link(appointment_id: str, token: str) -> str:
    # URL fragments are not sent to backend/proxy logs.
    return f"{settings.APP_BASE_URL}/public/appointments/{appointment_id}#pat={token}"


def _resolve_patient_access_token(appointment: dict) -> str | None:
    plain = appointment.get("patient_access_token")
    if plain:
        return str(plain)

    token_enc = appointment.get("patient_access_token_enc")
    if token_enc:
        try:
            return decrypt_magic_token(str(token_enc))
        except Exception:
            logger.warning(
                "Failed to decrypt patient access token for appointment=%s",
                appointment.get("_id"), # codeql[py/clear-text-logging-sensitive-data]
            )
    return None


def _render(template_name: str, **kwargs) -> str:
    kwargs.setdefault("logo_src", f"cid:{_LOGO_CID}")
    return _jinja_env.get_template(template_name).render(**kwargs)


async def safe_send_email(coro, log_msg: str) -> bool:
    try:
        await coro
        return True
    except Exception:
        logger.warning("Failed to send %s email", log_msg, exc_info=True) # codeql[py/clear-text-logging-sensitive-data]
        return False


def _build_magic_link_for_appt(appointment: dict) -> str:
    appointment_id = str(appointment["_id"])
    token = _resolve_patient_access_token(appointment)
    if token:
        return _magic_link(appointment_id, token)
    return f"{settings.APP_BASE_URL}/public/appointments/{appointment_id}"


async def send_booking_confirmation(appointment: dict):
    if not settings.RESEND_API_KEY:
        return
    if not appointment.get("patient_email"):
        return

    html = _render(
        "booking_confirmation.html",
        doctor_name=appointment.get("doctor_name"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        mode=appointment.get("mode"),
        consultation_fee=appointment.get("consultation_fee"),
        magic_link=_build_magic_link_for_appt(appointment),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": appointment.get("patient_email"),
            "subject": "Your Appointment is Confirmed",
            "html": html,
        }),
    )


async def send_payment_confirmation(
    appointment: dict,
    receipt_pdf_bytes: bytes | None = None,
    receipt_id: str | None = None,
):
    if not settings.RESEND_API_KEY:
        return
    if not appointment.get("patient_email"):
        return

    html = _render(
        "payment_confirmation.html",
        doctor_name=appointment.get("doctor_name"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        mode=appointment.get("mode"),
        consultation_fee=appointment.get("consultation_fee"),
        magic_link=_build_magic_link_for_appt(appointment),
    )

    email_payload: dict = {
        "from": settings.EMAIL_FROM,
        "to": appointment.get("patient_email"),
        "subject": "Payment Confirmed - Appointment Ready",
        "html": html,
    }

    if receipt_pdf_bytes and receipt_id:
        email_payload["attachments"] = [
            {
                "filename": f"Receipt-{receipt_id}.pdf",
                "content": base64.b64encode(receipt_pdf_bytes).decode("ascii"),
                "content_type": "application/pdf",
            }
        ]

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo(email_payload),
    )


async def send_reminder_email(appointment: dict):
    if not settings.RESEND_API_KEY:
        return
    if not appointment.get("patient_email"):
        return

    html = _render(
        "appointment_reminder.html",
        doctor_name=appointment.get("doctor_name"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        mode=appointment.get("mode"),
        magic_link=_build_magic_link_for_appt(appointment),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": appointment.get("patient_email"),
            "subject": "Reminder: Upcoming Appointment in 24 Hours",
            "html": html,
        }),
    )


async def send_reschedule_confirmation(appointment: dict):
    if not settings.RESEND_API_KEY:
        return
    if not appointment.get("patient_email"):
        return

    html = _render(
        "reschedule_confirmation.html",
        doctor_name=appointment.get("doctor_name"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        mode=appointment.get("mode"),
        consultation_fee=appointment.get("consultation_fee"),
        payment_choice=appointment.get("payment_choice"),
        magic_link=_build_magic_link_for_appt(appointment),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": appointment.get("patient_email"),
            "subject": "Your Appointment Has Been Rescheduled",
            "html": html,
        }),
    )


async def send_cancellation_email(appointment: dict):
    if not settings.RESEND_API_KEY:
        return
    if not appointment.get("patient_email"):
        return

    html = _render(
        "cancellation.html",
        doctor_name=appointment.get("doctor_name"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        cancel_reason=appointment.get("cancel_reason"),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": appointment.get("patient_email"),
            "subject": "Your Appointment Has Been Cancelled",
            "html": html,
        }),
    )


async def send_doctor_new_booking(appointment: dict, doctor_email: str):
    if not settings.RESEND_API_KEY:
        return

    html = _render(
        "doctor_new_booking.html",
        appointment_id=str(appointment["_id"]),
        patient_name=appointment.get("patient_name"),
        patient_phone=appointment.get("patient_phone"),
        patient_email=appointment.get("patient_email"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        mode=appointment.get("mode"),
        payment_status=appointment.get("payment_status"),
        consultation_fee=appointment.get("consultation_fee"),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": doctor_email,
            "subject": "New Appointment Booked",
            "html": html,
        }),
    )


async def send_doctor_cancellation(appointment: dict, doctor_email: str, cancelled_by: str = "patient"):
    if not settings.RESEND_API_KEY:
        return

    html = _render(
        "doctor_cancellation.html",
        appointment_id=str(appointment["_id"]),
        patient_name=appointment.get("patient_name"),
        patient_phone=appointment.get("patient_phone"),
        patient_email=appointment.get("patient_email"),
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        mode=appointment.get("mode"),
        payment_status=appointment.get("payment_status"),
        refund_status=appointment.get("refund_status"),
        cancelled_by=cancelled_by,
        cancel_reason=appointment.get("cancel_reason"),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": doctor_email,
            "subject": "Appointment Cancelled",
            "html": html,
        }),
    )


async def send_doctor_reschedule(
    old_appointment: dict,
    new_appointment: dict,
    doctor_email: str,
    rescheduled_by: str = "patient",
):
    if not settings.RESEND_API_KEY:
        return

    html = _render(
        "doctor_reschedule.html",
        old_appointment_id=str(old_appointment["_id"]),
        new_appointment_id=str(new_appointment["_id"]),
        patient_name=new_appointment.get("patient_name") or old_appointment.get("patient_name"),
        patient_phone=new_appointment.get("patient_phone") or old_appointment.get("patient_phone"),
        patient_email=new_appointment.get("patient_email") or old_appointment.get("patient_email"),
        old_scheduled_at=format_ist(old_appointment.get("scheduled_at")),
        new_scheduled_at=format_ist(new_appointment.get("scheduled_at")),
        mode=new_appointment.get("mode") or old_appointment.get("mode"),
        payment_status=new_appointment.get("payment_status") or old_appointment.get("payment_status"),
        rescheduled_by=rescheduled_by,
        reschedule_reason=new_appointment.get("reschedule_reason") or old_appointment.get("reschedule_reason"),
    )

    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": doctor_email,
            "subject": "Appointment Rescheduled",
            "html": html,
        }),
    )


async def send_password_reset_otp(email: str, code: str, ttl_minutes: int) -> None:
    if not settings.RESEND_API_KEY:
        return

    html = _render(
        "password_reset_otp.html",
        code=code,
        ttl_minutes=ttl_minutes,
    )
    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": email,
            "subject": "Reset your HPHomeo password",
            "html": html,
        }),
    )


async def send_prescription_email(
    appointment: dict,
    prescription_pdf_url: str,
    rx_id: str,
) -> None:
    if not settings.RESEND_API_KEY:
        return
    if not appointment.get("patient_email"):
        return

    # Download the prescription PDF from Cloudinary
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(prescription_pdf_url)
            resp.raise_for_status()
            pdf_bytes = resp.content
    except Exception:
        logger.warning("Failed to download prescription PDF for rx_id=%s — skipping email", rx_id) # codeql[py/clear-text-logging-sensitive-data]
        return

    html = _render(
        "prescription_delivery.html",
        patient_name=appointment.get("patient_name") or "Patient",
        scheduled_at=format_ist(appointment.get("scheduled_at")),
        rx_id=rx_id,
    )

    email_payload: dict = {
        "from": settings.EMAIL_FROM,
        "to": appointment["patient_email"],
        "subject": f"Your Digital Prescription — {rx_id}",
        "html": html,
        "attachments": [
            {
                "filename": f"Prescription-{rx_id}.pdf",
                "content": base64.b64encode(pdf_bytes).decode("ascii"),
                "content_type": "application/pdf",
            }
        ],
    }

    await asyncio.to_thread(resend.Emails.send, _with_logo(email_payload))


async def send_doctor_login_otp(email: str, code: str, ttl_minutes: int) -> None:
    if not settings.RESEND_API_KEY:
        return

    html = _render(
        "doctor_login_otp.html",
        code=code,
        ttl_minutes=ttl_minutes,
    )
    await asyncio.to_thread(
        resend.Emails.send,
        _with_logo({
            "from": settings.EMAIL_FROM,
            "to": email,
            "subject": "Your HPHomeo login OTP",
            "html": html,
        }),
    )
