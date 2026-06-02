import logging
from typing import Any

from bson import ObjectId
from fastapi import HTTPException

logger = logging.getLogger(__name__)


def _oid(x: Any) -> ObjectId:
    try:
        return x if isinstance(x, ObjectId) else ObjectId(str(x))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid id")
