from typing import List, Optional
from pydantic import BaseModel, Field


class RxItem(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    dosage: Optional[str] = Field(default=None, max_length=120)
    frequency: Optional[str] = Field(default=None, max_length=120)
    duration: Optional[str] = Field(default=None, max_length=120)
    instructions: Optional[str] = Field(default=None, max_length=300)


class PrescriptionUpsert(BaseModel):
    chief_complaints: Optional[str] = Field(default=None, max_length=1000)
    diagnosis: Optional[str] = Field(default=None, max_length=1000)
    advice: Optional[str] = Field(default=None, max_length=2000)
    items: List[RxItem] = Field(default_factory=list)


class PrescriptionTemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    payload: PrescriptionUpsert
