import asyncio
import io
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from PIL import Image, ImageFile, ImageOps, UnidentifiedImageError
from app.core.database import get_db
from app.routes.auth_routes import get_current_doctor
from app.services.cache_service import (
    TTL_10_MINUTES,
    cache_get_json,
    cache_set_json,
    doctor_profile_key,
    invalidate_doctor_cache,
)
from app.schemas.doctor_profile_schema import DoctorProfileOut, DoctorPhotoOut, DoctorSignatureOut, DoctorProfileUpdate
from app.services.cloudinary_service import cloudinary_service
from app.utils.clinic import clinic_profile_fields
from app.utils.security_sanitize import strip_sensitive_fields
from app.utils.time import utc_now

router = APIRouter(prefix="/doctor", tags=["Doctor Profile"])

MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5MB
MAX_IMAGE_PIXELS = 20_000_000
MAX_IMAGE_DIMENSION = 1600
NORMALIZED_IMAGE_FORMAT = "WEBP"
NORMALIZED_IMAGE_EXTENSION = "webp"
ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP"}

ImageFile.LOAD_TRUNCATED_IMAGES = False
Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS


def _is_profile_complete(doc: dict, *, has_availability: bool | None = None) -> bool:
    """
    A doctor's profile is complete when ALL of these are set:
    - specialization
    - available_modes (at least one)
    - online_fee (if 'online' in available_modes)
    - walkin_fee (if 'walk_in' in available_modes)
    - about (bio text)
    - profile_photo
    - signature_url
    - availability schedule (passed via has_availability flag, or ignored if None)
    """
    specialization = (doc.get("specialization") or "").strip()
    available_modes = doc.get("available_modes") or []

    if not specialization or not isinstance(available_modes, list) or not available_modes:
        return False

    has_online = "online" in available_modes
    has_walk_in = "walk_in" in available_modes

    if has_online and doc.get("online_fee") is None:
        return False
    if has_walk_in and doc.get("walkin_fee") is None:
        return False

    # Bio, profile photo, and signature are required
    if not (doc.get("about") or "").strip():
        return False
    if not doc.get("profile_photo"):
        return False
    if not doc.get("signature_url"):
        return False

    # Availability check (when caller provides the flag)
    if has_availability is not None and not has_availability:
        return False

    return True


def _doctor_profile_out(doc: dict) -> dict:
    clinic_fields = clinic_profile_fields()
    return {
        "id": str(doc["_id"]),
        "email": doc.get("email"),
        "phone": doc.get("phone"),

        # Identity & Basic Info
        "full_name": doc.get("full_name"),
        "registration_no": doc.get("registration_no"),
        "profile_photo": doc.get("profile_photo"),
        "gender": doc.get("gender"),
        "about": doc.get("about"),

        # Branding / Prescription assets
        "signature_url": doc.get("signature_url"),

        # Professional Info
        "specialization": doc.get("specialization"),
        "experience_years": doc.get("experience_years"),
        "qualifications": doc.get("qualifications", []),
        "languages": doc.get("languages", []),

        # Clinic Info
        "clinic_name": clinic_fields["clinic_name"],
        "city": clinic_fields["city"],
        "clinic_address": clinic_fields["clinic_address"],
        "clinic_phone": clinic_fields["clinic_phone"],

        # Consultation Info
        "available_modes": doc.get("available_modes", []),
        "online_fee": doc.get("online_fee"),
        "walkin_fee": doc.get("walkin_fee"),

        # Meta
        "is_admin": bool(doc.get("is_admin", False)),
        "verification_status": doc.get("verification_status", "pending"),
        "is_suspended": doc.get("is_suspended", False),
        "profile_complete": _is_profile_complete(doc, has_availability=doc.get("_has_availability")),
        "totp_enabled": bool(doc.get("totp_enabled", False)),
        "created_at": doc.get("created_at").isoformat() if doc.get("created_at") else None,
        "updated_at": doc.get("updated_at").isoformat() if doc.get("updated_at") else None,
    }


def _process_image_sync(data: bytes) -> bytes:
    """CPU-bound PIL work — must be called via asyncio.to_thread."""
    with Image.open(io.BytesIO(data)) as probe:
        actual_format = (probe.format or "").upper()
        if actual_format not in ALLOWED_IMAGE_FORMATS:
            raise ValueError(f"format:{actual_format}")
        probe.verify()

    with Image.open(io.BytesIO(data)) as image:
        image = ImageOps.exif_transpose(image)
        image.load()

        if getattr(image, "n_frames", 1) > 1:
            raise ValueError("animated")

        if image.width <= 0 or image.height <= 0:
            raise ValueError("dimensions")

        if image.width * image.height > MAX_IMAGE_PIXELS:
            raise Image.DecompressionBombError("too_large")

        image.thumbnail(
            (MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION),
            Image.Resampling.LANCZOS,
        )

        normalized = image.convert("RGBA" if "A" in image.getbands() else "RGB")
        output = io.BytesIO()
        normalized.save(
            output,
            format=NORMALIZED_IMAGE_FORMAT,
            quality=78,
            method=6,
        )
        return output.getvalue()


async def _read_and_validate_image(file: UploadFile) -> tuple[bytes, str]:
    if not file:
        raise HTTPException(status_code=400, detail="File is required")

    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    data = await file.read()
    await file.close()

    if not data:
        raise HTTPException(status_code=400, detail="Empty file")

    if len(data) > MAX_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail="Max file size is 5MB")

    try:
        normalized_bytes = await asyncio.to_thread(_process_image_sync, data)
    except Image.DecompressionBombError:
        raise HTTPException(status_code=400, detail="Image dimensions are too large")
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("format:"):
            raise HTTPException(status_code=400, detail="Only JPEG, PNG, and WEBP images are allowed")
        if msg == "animated":
            raise HTTPException(status_code=400, detail="Animated images are not allowed")
        raise HTTPException(status_code=400, detail="Invalid image dimensions")
    except (UnidentifiedImageError, OSError):
        raise HTTPException(status_code=400, detail="Invalid or unsupported image file")

    if not normalized_bytes:
        raise HTTPException(status_code=400, detail="Invalid image data")

    if len(normalized_bytes) > MAX_IMAGE_BYTES:
        raise HTTPException(status_code=400, detail="Compressed image still exceeds 5MB")

    return normalized_bytes, NORMALIZED_IMAGE_EXTENSION


@router.get("/profile", response_model=DoctorProfileOut)
async def get_doctor_profile(doctor=Depends(get_current_doctor)):
    db = get_db()
    cache_key = doctor_profile_key(str(doctor["_id"]))
    cached = await cache_get_json(cache_key)
    if isinstance(cached, dict):
        return cached

    doc = await db.doctors.find_one({"_id": doctor["_id"]})
    avail = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]}, {"_id": 1})
    doc["_has_availability"] = avail is not None
    response = strip_sensitive_fields(_doctor_profile_out(doc))
    await cache_set_json(cache_key, response, TTL_10_MINUTES)
    return response


@router.put("/profile", response_model=DoctorProfileOut)
async def update_doctor_profile(
    payload: DoctorProfileUpdate,
    doctor=Depends(get_current_doctor),
):
    db = get_db()

    update_data = payload.model_dump(exclude_none=True)
    fixed_clinic_fields = clinic_profile_fields()

    # Normalize lists
    if "qualifications" in update_data:
        update_data["qualifications"] = [
            q.strip() for q in update_data["qualifications"] if q and q.strip()
        ]

    if "languages" in update_data:
        update_data["languages"] = [
            l.strip() for l in update_data["languages"] if l and l.strip()
        ]

    if "available_modes" in update_data:
        # remove duplicates, keep order
        seen = set()
        cleaned = []
        for m in update_data["available_modes"]:
            if m not in seen:
                seen.add(m)
                cleaned.append(m)
        update_data["available_modes"] = cleaned

    for key in ("clinic_name", "city", "clinic_address", "clinic_phone"):
        update_data.pop(key, None)

    update_data.update(fixed_clinic_fields)

    update_data["updated_at"] = utc_now()

    await db.doctors.update_one({"_id": doctor["_id"]}, {"$set": update_data})
    doc = await db.doctors.find_one({"_id": doctor["_id"]})
    avail = await db.doctor_availability.find_one({"doctor_id": doctor["_id"]}, {"_id": 1})
    doc["_has_availability"] = avail is not None
    response = strip_sensitive_fields(_doctor_profile_out(doc))
    await invalidate_doctor_cache(str(doctor["_id"]), invalidate_list=True)
    await cache_set_json(doctor_profile_key(str(doctor["_id"])), response, TTL_10_MINUTES)
    return response


async def _upload_doctor_media(
    *,
    doctor: dict,
    file: UploadFile,
    field_name: str,
    public_id: str,
):
    db = get_db()
    data, extension = await _read_and_validate_image(file)

    doctor_id = str(doctor["_id"])
    current_url = doctor.get(field_name)
    desired_public_id = f"{cloudinary_service.folder_root}/doctors/{doctor_id}/{public_id}"
    current_public_id = cloudinary_service.extract_public_id_from_url(current_url or "")

    url = await asyncio.to_thread(
        cloudinary_service.upload_image_bytes,
        data=data,
        filename=f"{public_id}.{extension}",
        folder=f"doctors/{doctor_id}",
        public_id=public_id,
    )

    if current_public_id and current_public_id != desired_public_id:
        await asyncio.to_thread(
            cloudinary_service.delete_asset,
            public_id=current_public_id,
            resource_type="image",
        )

    await db.doctors.update_one(
        {"_id": doctor["_id"]},
        {"$set": {field_name: url, **clinic_profile_fields(), "updated_at": utc_now()}},
    )
    await invalidate_doctor_cache(str(doctor["_id"]), invalidate_list=True)
    return url





@router.post("/profile/photo", response_model=DoctorPhotoOut)
async def upload_doctor_profile_photo(
    file: UploadFile = File(...),
    doctor=Depends(get_current_doctor),
):
    """Upload profile photo."""
    url = await _upload_doctor_media(
        doctor=doctor,
        file=file,
        field_name="profile_photo",
        public_id="profile-photo",
    )
    return {"profile_photo": url}


@router.post("/profile/signature", response_model=DoctorSignatureOut)
async def upload_doctor_signature(
    file: UploadFile = File(...),
    doctor=Depends(get_current_doctor),
):
    url = await _upload_doctor_media(
        doctor=doctor,
        file=file,
        field_name="signature_url",
        public_id="signature",
    )
    return {"signature_url": url}
