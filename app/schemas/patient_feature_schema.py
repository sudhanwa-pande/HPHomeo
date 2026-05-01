"""
Pydantic schemas for patient-facing features:
  - Patient notes (per-appointment)
  - Rating & review (per-appointment, one per patient)
  - Reminder preferences (per-appointment)
"""

from typing import Optional, Literal

from pydantic import BaseModel, Field


# ─────────────────────────────────────────────
# Patient Notes
# ─────────────────────────────────────────────

class PatientNotesUpdateIn(BaseModel):
    notes: str = Field(..., max_length=500)


# ─────────────────────────────────────────────
# Rating & Review
# ─────────────────────────────────────────────

class ReviewCreateIn(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    comment: Optional[str] = Field(default=None, max_length=500)


class ReviewOut(BaseModel):
    rating: int
    comment: Optional[str] = None
    created_at: str


# ─────────────────────────────────────────────
# Reminder Preferences
# ─────────────────────────────────────────────

ReminderTiming = Literal["1h", "1d"]


class ReminderPreferencesUpdateIn(BaseModel):
    email: bool = Field(default=True)
    whatsapp: bool = Field(default=True)
    timing: list[ReminderTiming] = Field(default=["1h", "1d"], max_length=2)
