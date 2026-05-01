from pydantic import BaseModel, Field
from typing import Optional


class BlockCreate(BaseModel):
    start_at: str  # ISO datetime string, parsed in route
    end_at: str    # ISO datetime string, parsed in route
    reason: Optional[str] = Field(default=None, max_length=200)


class ExceptionTimeRange(BaseModel):
    start: str = Field(..., pattern=r"^\d{2}:\d{2}$")
    end: str = Field(..., pattern=r"^\d{2}:\d{2}$")


class AvailabilityExceptionCreate(BaseModel):
    date: str = Field(..., description="YYYY-MM-DD")
    status: str = Field(..., pattern=r"^(blocked|available)$")
    time_slots: list[ExceptionTimeRange] = Field(default_factory=list)
    reason: Optional[str] = Field(default=None, max_length=200)
