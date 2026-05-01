from typing import Optional
from pydantic import BaseModel


class ReceiptOut(BaseModel):
    """Serialised receipt returned to clients."""

    id: str
    receipt_id: str
    appointment_id: str
    doctor_id: str
    patient_id: Optional[str] = None
    patient_name: str
    patient_email: Optional[str] = None
    patient_phone: str
    doctor_name: str
    doctor_registration_no: Optional[str] = None
    consultation_fee: float
    payment_method: str
    payment_id: Optional[str] = None
    pdf_url: Optional[str] = None
    receipt_date: str
    created_at: str
