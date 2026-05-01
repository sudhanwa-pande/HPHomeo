from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.core.database import get_db
from app.core.rate_limits import rl
from app.routes.auth_routes import get_current_doctor
from app.utils.time import ensure_utc, utc_now

router = APIRouter(prefix="/doctor", tags=["Doctor Notes"])


class NoteCreate(BaseModel):
    text: str = Field(min_length=1, max_length=500)


class NoteOut(BaseModel):
    id: str
    text: str
    created_at: Optional[str] = None


class NotesListOut(BaseModel):
    notes: List[NoteOut]


class NoteCreatedOut(BaseModel):
    note: NoteOut


class DeletedOut(BaseModel):
    ok: bool


def _note_out(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "text": doc["text"],
        "created_at": ensure_utc(doc.get("created_at")).isoformat() if doc.get("created_at") else None,
    }


@router.get("/notes", dependencies=[rl(60, 60)], response_model=NotesListOut)
async def get_notes(doctor=Depends(get_current_doctor)):
    db = get_db()
    cursor = db.doctor_notes.find(
        {"doctor_id": doctor["_id"]}
    ).sort("created_at", -1).limit(200)
    notes = [_note_out(doc) async for doc in cursor]
    return {"notes": notes}


@router.post("/notes", dependencies=[rl(30, 60)], status_code=201, response_model=NoteCreatedOut)
async def create_note(body: NoteCreate, doctor=Depends(get_current_doctor)):
    db = get_db()
    doc = {
        "doctor_id": doctor["_id"],
        "text": body.text.strip(),
        "created_at": utc_now(),
    }
    result = await db.doctor_notes.insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"note": _note_out(doc)}


@router.delete("/notes/{note_id}", dependencies=[rl(30, 60)], response_model=DeletedOut)
async def delete_note(note_id: str, doctor=Depends(get_current_doctor)):
    db = get_db()
    if not ObjectId.is_valid(note_id):
        raise HTTPException(status_code=422, detail="Invalid note_id")
    result = await db.doctor_notes.delete_one(
        {"_id": ObjectId(note_id), "doctor_id": doctor["_id"]}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Note not found")
    return {"ok": True}
