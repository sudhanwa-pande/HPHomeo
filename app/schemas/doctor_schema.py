import re
from datetime import datetime
from typing import Literal

from email_validator import validate_email, EmailNotValidError
from pydantic import BaseModel, EmailStr, Field, field_validator

from app.utils.phone import normalize_phone_e164

Role = Literal["doctor"]
VerificationStatus = Literal["pending", "approved", "rejected"]

PASSWORD_RE = re.compile(r"^(?=.*[A-Z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,128}$")
AADHAAR_RE = re.compile(r"^\d{12}$")
CONTROL_CHAR_RE = re.compile(r"[\x00-\x1f\x7f]")
TOTP_CODE_RE = re.compile(r"^\d{6}$")


def _clean_text(value: str, field_name: str) -> str:
    cleaned = value.strip()
    if CONTROL_CHAR_RE.search(cleaned):
        raise ValueError(f"Invalid characters in {field_name}")
    return cleaned


class DoctorRegister(BaseModel):
    full_name: str = Field(min_length=2, max_length=80)
    email: EmailStr
    phone: str = Field(min_length=7, max_length=20)
    password: str = Field(min_length=8, max_length=128)
    registration_no: str = Field(min_length=3, max_length=50)
    turnstileToken: str | None = None

    @field_validator("password")
    @classmethod
    def validate_strong_password(cls, value: str) -> str:
        if not PASSWORD_RE.fullmatch(value):
            raise ValueError("Password must contain 1 uppercase, 1 number, and 1 special character")
        return value

    @field_validator("registration_no")
    @classmethod
    def validate_registration_no(cls, value: str) -> str:
        return value.strip()

    @field_validator("full_name")
    @classmethod
    def validate_full_name(cls, value: str) -> str:
        return _clean_text(value, "full_name")

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, value: str) -> str:
        return normalize_phone_e164(value)

    @field_validator("email", mode="before")
    @classmethod
    def validate_email_strict(cls, value: str) -> str:
        if not isinstance(value, str):
            return value
        try:
            # check_deliverability=True forces a DNS MX record lookup
            valid = validate_email(value, check_deliverability=True)
            domain = valid.domain.lower()
            
            # Common disposable domains blocklist
            disposable = {
                "mailinator.com", "10minutemail.com", "temp-mail.org", 
                "yopmail.com", "guerrillamail.com", "tempmail.com", "throwawaymail.com"
            }
            if domain in disposable:
                raise ValueError("Disposable email addresses are not allowed")
                
            return valid.normalized
        except EmailNotValidError as e:
            raise ValueError(f"Invalid email: {str(e)}")


class DoctorLogin(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=128)


class LoginStepOut(BaseModel):
    step: Literal["authenticated", "otp_required", "totp_required"]
    temp_token: str | None = None
    expires_in_seconds: int | None = None
    otp_channel: Literal["email"] | None = None
    token_type: str = "bearer"
    access_token: str | None = None
    refresh_token: str | None = None


class LoginOtpVerifyIn(BaseModel):
    temp_token: str = Field(min_length=16, max_length=256)
    code: str = Field(min_length=6, max_length=6)

    @field_validator("code")
    @classmethod
    def validate_otp_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not TOTP_CODE_RE.fullmatch(cleaned):
            raise ValueError("OTP code must be 6 digits")
        return cleaned


class LoginTotpValidateIn(BaseModel):
    temp_token: str = Field(min_length=16, max_length=256)
    code: str = Field(min_length=6, max_length=6)

    @field_validator("code")
    @classmethod
    def validate_totp_login_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not TOTP_CODE_RE.fullmatch(cleaned):
            raise ValueError("TOTP code must be 6 digits")
        return cleaned


class RefreshTokenIn(BaseModel):
    refresh_token: str = Field(min_length=20, max_length=4000)


class TotpEnableIn(BaseModel):
    code: str = Field(min_length=6, max_length=6)

    @field_validator("code")
    @classmethod
    def validate_totp_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not TOTP_CODE_RE.fullmatch(cleaned):
            raise ValueError("TOTP code must be 6 digits")
        return cleaned


class TokenPairOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class AdminSessionOut(BaseModel):
    admin_session_token: str
    token_type: str = "bearer"
    expires_in_seconds: int = 1800


class DoctorRegisterOut(TokenPairOut):
    doctor_id: str
    verification_status: VerificationStatus = "pending"


class ForgotPasswordIn(BaseModel):
    email: EmailStr


class ResetPasswordIn(BaseModel):
    email: EmailStr
    code: str = Field(min_length=6, max_length=6)
    new_password: str = Field(min_length=8, max_length=128)

    @field_validator("code")
    @classmethod
    def validate_reset_code(cls, value: str) -> str:
        cleaned = value.strip()
        if not TOTP_CODE_RE.fullmatch(cleaned):
            raise ValueError("Code must be exactly 6 digits")
        return cleaned

    @field_validator("new_password")
    @classmethod
    def validate_reset_password(cls, value: str) -> str:
        if not PASSWORD_RE.fullmatch(value):
            raise ValueError("Password must contain 1 uppercase, 1 number, and 1 special character")
        return value


class ChangePasswordIn(BaseModel):
    current_password: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=8, max_length=128)

    @field_validator("new_password")
    @classmethod
    def validate_change_password(cls, value: str) -> str:
        if not PASSWORD_RE.fullmatch(value):
            raise ValueError("Password must contain 1 uppercase, 1 number, and 1 special character")
        return value


class DoctorOut(BaseModel):
    id: str
    full_name: str
    email: EmailStr
    phone: str
    registration_no: str
    role: Role = "doctor"
    is_admin: bool = False
    verification_status: VerificationStatus = "pending"
    verified_at: datetime | None = None
    verified_by_admin_id: str | None = None
