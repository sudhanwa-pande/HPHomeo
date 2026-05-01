from pydantic import BaseModel, Field, field_validator
from typing import Dict, List
from zoneinfo import ZoneInfo

WEEKDAY_ALIASES = {
    "mon": "mon",
    "monday": "mon",
    "tue": "tue",
    "tues": "tue",
    "tuesday": "tue",
    "wed": "wed",
    "wednesday": "wed",
    "thu": "thu",
    "thur": "thu",
    "thurs": "thu",
    "thursday": "thu",
    "fri": "fri",
    "friday": "fri",
    "sat": "sat",
    "saturday": "sat",
    "sun": "sun",
    "sunday": "sun",
}


class TimeRange(BaseModel):
    start: str
    end: str

class WeeklyAvailability(BaseModel):
    slot_duration_min: int = 20
    timezone: str = "Asia/Kolkata"
    weekly: Dict[str, List[TimeRange]]

    @field_validator("slot_duration_min")
    @classmethod
    def validate_slot_duration(cls, v):
        allowed = [10, 20, 30]
        if v not in allowed:
            raise ValueError(f"slot_duration_min must be one of {allowed}")
        return v

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str):
        try:
            ZoneInfo(v)
        except Exception:
            raise ValueError("timezone must be a valid IANA timezone, e.g. 'Asia/Kolkata'")
        return v

    @field_validator("weekly")
    @classmethod
    def normalize_weekly_keys(cls, value: Dict[str, List[TimeRange]]):
        normalized: Dict[str, List[TimeRange]] = {}
        for key, ranges in value.items():
            normalized_key = WEEKDAY_ALIASES.get((key or "").strip().lower())
            if not normalized_key:
                raise ValueError(
                    "weekly keys must be weekday names (mon..sun or monday..sunday)"
                )
            normalized.setdefault(normalized_key, []).extend(ranges)
        return normalized
