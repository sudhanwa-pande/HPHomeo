from typing import Literal

from pydantic import BaseModel, EmailStr, Field
from pydantic import field_validator

from app.utils.phone import normalize_phone_e164

OtpPurpose = Literal["login"]


class PatientRequestOtpIn(BaseModel):
    phone: str = Field(min_length=7, max_length=20)
    purpose: OtpPurpose = "login"

    @field_validator("phone")
    @classmethod
    def normalize_phone(cls, value: str) -> str:
        return normalize_phone_e164(value)


class PatientVerifyOtpIn(BaseModel):
    phone: str = Field(min_length=7, max_length=20)
    code: str = Field(min_length=6, max_length=6)
    purpose: OtpPurpose = "login"

    @field_validator("phone")
    @classmethod
    def normalize_phone(cls, value: str) -> str:
        return normalize_phone_e164(value)


class TokenPairOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class RefreshTokenIn(BaseModel):
    refresh_token: str = Field(min_length=20, max_length=4000)


class PatientMeOut(BaseModel):
    patient_user_id: str
    phone: str
    full_name: str | None = None
    email: str | None = None
    age: int | None = None
    sex: str | None = None


class PatientProfileUpdateIn(BaseModel):
    email: EmailStr | None = None
    full_name: str | None = Field(default=None, min_length=2, max_length=100)
    age: int | None = Field(default=None, ge=1, le=120)
    sex: Literal["male", "female", "other"] | None = None

    @field_validator("full_name")
    @classmethod
    def clean_full_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
