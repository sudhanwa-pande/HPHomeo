from pydantic import BaseModel, EmailStr, Field
from typing import Optional
from typing import Literal
from pydantic import field_validator

from app.utils.phone import normalize_phone_e164

class PatientCreate(BaseModel):
    full_name: str = Field(min_length=2, max_length=100)
    email: Optional[EmailStr] = None
    phone: str = Field(..., max_length=20)
    age: int = Field(..., ge=1, le=120)
    sex: Literal["male", "female", "other"]
    notes: Optional[str] = None

    @field_validator("full_name")
    @classmethod
    def clean_full_name(cls, value: str) -> str:
        return value.strip()

    @field_validator("phone")
    @classmethod
    def normalize_phone(cls, value: str) -> str:
        return normalize_phone_e164(value)

    @field_validator("notes")
    @classmethod
    def clean_notes(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

class PatientUpdate(BaseModel):
    full_name: Optional[str] = Field(default=None, min_length=2, max_length=100)
    email: Optional[EmailStr] = None
    phone: Optional[str] = Field(default=None, max_length=20)
    age: Optional[int] = Field(default=None, ge=1, le=120)
    sex: Optional[Literal["male", "female", "other"]] = None
    notes: Optional[str] = None

    @field_validator("full_name")
    @classmethod
    def clean_full_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.strip()

    @field_validator("phone")
    @classmethod
    def normalize_phone(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return normalize_phone_e164(value)

    @field_validator("notes")
    @classmethod
    def clean_notes(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
