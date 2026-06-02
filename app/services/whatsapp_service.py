import asyncio
import logging
from typing import Any
from zoneinfo import ZoneInfo

import requests

from app.core.config import settings
from app.utils.magic_token import decrypt_magic_token
from app.utils.time import ensure_utc

logger = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")


def _is_enabled() -> bool:
    return bool(
        settings.WHATSAPP_ENABLED
        and settings.WHATSAPP_PHONE_NUMBER_ID
        and settings.WHATSAPP_ACCESS_TOKEN
    )


def _endpoint() -> str:
    return (
        f"{settings.WHATSAPP_API_BASE_URL.rstrip('/')}"
        f"/{settings.WHATSAPP_API_VERSION.strip('/')}"
        f"/{settings.WHATSAPP_PHONE_NUMBER_ID}/messages"
    )


async def _send_template_message(
    *,
    to_phone: str,
    template_name: str,
    body_params: list[str],
    button_url_suffix: str | None = None,
    document_url: str | None = None,
    document_filename: str | None = None,
) -> None:
    if not _is_enabled():
        return

    token = settings.WHATSAPP_ACCESS_TOKEN.get_secret_value() if settings.WHATSAPP_ACCESS_TOKEN else ""
    components: list[dict[str, Any]] = []

    # Document header (receipt PDF attached to template)
    if document_url:
        header_component: dict[str, Any] = {
            "type": "header",
            "parameters": [
                {
                    "type": "document",
                    "document": {"link": document_url},
                }
            ],
        }
        if document_filename:
            header_component["parameters"][0]["document"]["filename"] = document_filename
        components.append(header_component)

    components.append(
        {
            "type": "body",
            "parameters": [{"type": "text", "text": str(x)} for x in body_params],
        }
    )
    if button_url_suffix:
        components.append(
            {
                "type": "button",
                "sub_type": "url",
                "index": "0",
                "parameters": [{"type": "text", "text": button_url_suffix}],
            }
        )

    payload: dict[str, Any] = {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": settings.WHATSAPP_TEMPLATE_LANG},
            "components": components,
        },
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = await asyncio.to_thread(
        requests.post,
        _endpoint(),
        json=payload,
        headers=headers,
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"WhatsApp send failed: {response.status_code} {response.text[:200]}")


async def safe_send_whatsapp(coro, log_msg: str) -> bool:
    try:
        await coro
        return True
    except Exception:
        logger.warning("Failed to send WhatsApp: %s", log_msg, exc_info=True) # codeql[py/clear-text-logging-sensitive-data]
        return False


async def send_patient_login_otp_whatsapp(phone: str, code: str, _ttl_minutes: int) -> None:
    if not _is_enabled():
        return

    token = settings.WHATSAPP_ACCESS_TOKEN.get_secret_value() if settings.WHATSAPP_ACCESS_TOKEN else ""
    payload: dict[str, Any] = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "template",
        "template": {
            "name": settings.WHATSAPP_OTP_TEMPLATE_NAME,
            "language": {"code": settings.WHATSAPP_TEMPLATE_LANG},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": code}],
                },
                {
                    "type": "button",
                    "sub_type": "url",
                    "index": "0",
                    "parameters": [{"type": "text", "text": code}],
                },
            ],
        },
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = await asyncio.to_thread(
        requests.post,
        _endpoint(),
        json=payload,
        headers=headers,
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"WhatsApp send failed: {response.status_code} {response.text[:200]}")


def _patient_name(appointment: dict) -> str:
    return str(appointment.get("patient_name") or "Patient")


def _clinic_name() -> str:
    return str(settings.APP_NAME or "HPHomeo")


def _scheduled_at_local(appointment: dict):
    scheduled_at = ensure_utc(appointment.get("scheduled_at"))
    if not scheduled_at:
        return None
    return scheduled_at.astimezone(IST)


def _scheduled_at_date_text(appointment: dict) -> str:
    scheduled_at = _scheduled_at_local(appointment)
    if not scheduled_at:
        return "N/A"
    return scheduled_at.strftime("%d %b %Y")


def _scheduled_at_time_text(appointment: dict) -> str:
    scheduled_at = _scheduled_at_local(appointment)
    if not scheduled_at:
        return "N/A"
    return scheduled_at.strftime("%I:%M %p").lstrip("0")


def _appointment_subject(appointment: dict) -> str:
    explicit = str(appointment.get("appointment_subject") or "").strip()
    if explicit:
        return explicit
    if appointment.get("appointment_type") == "follow_up":
        return "Follow-up consultation"
    return "Homeopathy consultation"


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
                exc_info=True,
            ) # codeql[py/clear-text-logging-sensitive-data]
    return None


def _appointment_link_suffix(appointment: dict) -> str:
    """Token-only suffix for WhatsApp button URL.

    Meta URL-encodes the dynamic substitution in template buttons, so any
    "/" or "#" in the value gets mangled. We pass just the magic token (which
    only uses URL-safe chars from secrets.token_urlsafe) and the WhatsApp
    template's static URL points at https://<frontend>/m/{{1}} — a frontend
    redirect page that calls /public/access-by-token to set the cookie and
    forwards the patient to the appointment view.
    """
    token = _resolve_patient_access_token(appointment)
    if not token:
        raise ValueError("Patient access token is required for WhatsApp button links")
    return token


async def send_patient_appointment_confirmation_whatsapp(
    appointment: dict,
    receipt_pdf_url: str | None = None,
    receipt_id: str | None = None,
) -> None:
    await _send_template_message(
        to_phone=appointment["patient_phone"],
        template_name=settings.WHATSAPP_CONFIRMATION_TEMPLATE_NAME,
        body_params=[
            _patient_name(appointment),
            _clinic_name(),
            _appointment_subject(appointment),
            _scheduled_at_date_text(appointment),
            _scheduled_at_time_text(appointment),
        ],
        button_url_suffix=_appointment_link_suffix(appointment),
        document_url=receipt_pdf_url,
        document_filename=f"Receipt-{receipt_id}.pdf" if receipt_id else None,
    )


async def send_patient_appointment_reminder_whatsapp(appointment: dict) -> None:
    await _send_template_message(
        to_phone=appointment["patient_phone"],
        template_name=settings.WHATSAPP_REMINDER_TEMPLATE_NAME,
        body_params=[
            _patient_name(appointment),
            _clinic_name(),
            _scheduled_at_date_text(appointment),
            _scheduled_at_time_text(appointment),
        ],
        button_url_suffix=_appointment_link_suffix(appointment),
    )


async def send_patient_appointment_rescheduled_whatsapp(appointment: dict) -> None:
    await _send_template_message(
        to_phone=appointment["patient_phone"],
        template_name=settings.WHATSAPP_RESCHEDULE_TEMPLATE_NAME,
        body_params=[
            _patient_name(appointment),
            _clinic_name(),
            _scheduled_at_date_text(appointment),
            _scheduled_at_time_text(appointment),
        ],
        button_url_suffix=_appointment_link_suffix(appointment),
    )


async def send_patient_appointment_cancelled_whatsapp(appointment: dict) -> None:
    await _send_template_message(
        to_phone=appointment["patient_phone"],
        template_name=settings.WHATSAPP_CANCELLATION_TEMPLATE_NAME,
        body_params=[
            _patient_name(appointment),
            _scheduled_at_date_text(appointment),
        ],
    )


async def send_patient_prescription_whatsapp(
    appointment: dict,
    prescription_pdf_url: str,
    rx_id: str,
) -> None:
    """Send the appointment_complete template with prescription PDF as header document."""
    scheduled_at = _scheduled_at_local(appointment)
    date_text = scheduled_at.strftime("%d %b %Y") if scheduled_at else "N/A"

    await _send_template_message(
        to_phone=appointment["patient_phone"],
        template_name=settings.WHATSAPP_PRESCRIPTION_TEMPLATE_NAME,
        body_params=[
            _patient_name(appointment),
            date_text,
        ],
        document_url=prescription_pdf_url,
        document_filename=f"Prescription-{rx_id}.pdf",
    )


async def send_patient_appointment_update_whatsapp(
    appointment: dict,
    action: str,
    receipt_pdf_url: str | None = None,
    receipt_id: str | None = None,
) -> None:
    phone = appointment.get("patient_phone")
    if not phone:
        return
    if action in {"booked", "payment_confirmed"}:
        await send_patient_appointment_confirmation_whatsapp(
            appointment,
            receipt_pdf_url=receipt_pdf_url,
            receipt_id=receipt_id,
        )
        return
    if action in {"reminder", "reminder_12hr"}:
        await send_patient_appointment_reminder_whatsapp(appointment)
        return
    if action == "rescheduled":
        await send_patient_appointment_rescheduled_whatsapp(appointment)
        return
    if action == "cancelled":
        await send_patient_appointment_cancelled_whatsapp(appointment)
        return
    raise ValueError(f"Unsupported WhatsApp appointment action: {action}")
