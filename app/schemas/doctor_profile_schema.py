from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List, Literal


Gender = Literal["male", "female", "other", "prefer_not_say"]
Mode = Literal["online", "walk_in"]
VerificationStatus = Literal["pending", "approved", "rejected"]


class DoctorProfileOut(BaseModel):
    id: str
    email: Optional[str] = None
    phone: Optional[str] = None
    full_name: Optional[str] = None
    registration_no: Optional[str] = None
    profile_photo: Optional[str] = None
    gender: Optional[Gender] = None
    about: Optional[str] = None
    signature_url: Optional[str] = None
    specialization: Optional[str] = None
    experience_years: Optional[int] = None
    qualifications: List[str] = []
    languages: List[str] = []
    clinic_name: Optional[str] = None
    city: Optional[str] = None
    clinic_address: Optional[str] = None
    clinic_phone: Optional[str] = None
    available_modes: List[Mode] = []
    online_fee: Optional[int] = None
    walkin_fee: Optional[int] = None
    is_admin: bool = False
    verification_status: VerificationStatus = "pending"
    is_suspended: bool = False
    profile_complete: bool = False
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DoctorPhotoOut(BaseModel):
    profile_photo: str


class DoctorSignatureOut(BaseModel):
    signature_url: str


class DoctorProfileUpdate(BaseModel):
    # Identity & Basic Info
    full_name: Optional[str] = Field(default=None, min_length=2, max_length=100)
    profile_photo: Optional[str] = None  # plain string, not HttpUrl
    gender: Optional[Gender] = None
    about: Optional[str] = Field(default=None, max_length=1000)

    # Professional Info
    specialization: Optional[str] = Field(default=None, max_length=120)
    experience_years: Optional[int] = Field(default=None, ge=0, le=80)
    qualifications: Optional[List[str]] = Field(default=None, max_length=10)
    languages: Optional[List[str]] = Field(default=None, max_length=10)

    # Consultation Info
    available_modes: Optional[List[Mode]] = None
    online_fee: Optional[int] = Field(default=None, ge=0, le=100000)
    walkin_fee: Optional[int] = Field(default=None, ge=0, le=100000)

    @field_validator("profile_photo")
    @classmethod
    def validate_photo_url(cls, v):
        if v and not v.startswith(("http://", "https://")):
            raise ValueError("profile_photo must be a valid URL")
        return v

    @model_validator(mode="after")
    def check_fees_match_modes(self):
        if self.available_modes:
            if "online" in self.available_modes and self.online_fee is None:
                raise ValueError("online_fee is required when online mode is set")
            if "walk_in" in self.available_modes and self.walkin_fee is None:
                raise ValueError("walkin_fee is required when walk_in mode is set")
        return self
