
import io
import re
import time
from typing import Optional
from urllib.parse import unquote, urlparse

import cloudinary
import cloudinary.uploader
import cloudinary.utils

from app.core.config import settings


class CloudinaryService:
    """Service to handle Cloudinary interactions for uploading images and PDFs."""

    def __init__(self, folder_root: str = "ehomeo"):
        cloudinary.config(
            cloud_name=settings.CLOUDINARY_CLOUD_NAME,
            api_key=settings.CLOUDINARY_API_KEY,
            api_secret=settings.CLOUDINARY_API_SECRET.get_secret_value(),
            secure=True,
        )
        self.folder_root = folder_root.strip("/")

    def upload_image_bytes(
        self,
        *,
        data: bytes,
        filename: str,
        folder: str,
        public_id: Optional[str] = None,
    ) -> str:
        file_obj = io.BytesIO(data)
        file_obj.name = filename

        result = cloudinary.uploader.upload(
            file_obj,
            resource_type="image",
            folder=f"{self.folder_root}/{folder.strip('/')}",
            public_id=public_id,
            overwrite=True,
        )
        return result["secure_url"]

    def upload_pdf_bytes(
        self,
        *,
        data: bytes,
        filename: str,
        folder: str,
        public_id: Optional[str] = None,
    ) -> str:
        file_obj = io.BytesIO(data)
        file_obj.name = filename

        result = cloudinary.uploader.upload(
            file_obj,
            resource_type="raw",
            # "private" delivery type means Cloudinary enforces signed-URL access
            # at the CDN layer. Unlike the default "upload" type, the permanent
            # storage URL is not publicly accessible — every request requires a
            # valid signature with a matching expiry. This is required for
            # prescription PDFs, which are personal health information.
            type="private",
            folder=f"{self.folder_root}/{folder.strip('/')}",
            public_id=public_id,
            overwrite=False,
        )
        return result["secure_url"]

    def extract_public_id_from_url(self, url: str) -> Optional[str]:
        if not url:
            return None

        parsed = urlparse(url)
        path = unquote(parsed.path or "")
        parts = path.split("/upload/", 1) # codeql[py/polynomial-redos]
        if len(parts) < 2:
            return None

        public_path = parts[1]
        if public_path.startswith("v") and "/" in public_path:
            first_slash = public_path.find("/")
            version_part = public_path[1:first_slash]
            if version_part.isdigit():
                public_path = public_path[first_slash + 1:]

        if "." in public_path:
            public_path = public_path.rsplit(".", 1)[0]
        return public_path or None

    def extract_public_path_from_url(self, url: str) -> Optional[str]: # codeql[py/polynomial-redos]
        if not url:
            return None

        parsed = urlparse(url)
        path = unquote(parsed.path or "")
        parts = path.split("/upload/", 1)
        if len(parts) < 2:
            return None

        public_path = parts[1]
        if public_path.startswith("v") and "/" in public_path:
            first_slash = public_path.find("/")
            version_part = public_path[1:first_slash]
            if version_part.isdigit():
                public_path = public_path[first_slash + 1:]

        return public_path or None

    # Signed prescription URLs expire after 15 minutes.  A fresh URL is
    # generated on every view request, so this does not affect usability
    # but prevents a leaked URL from being replayed indefinitely.
    PDF_SIGNED_URL_TTL_SECONDS = 900

    def pdf_view_url(self, *, url: str) -> Optional[str]:
        """Return a short-lived signed delivery URL for a private raw PDF.

        The delivery type must be "private" to match how PDFs are uploaded.
        Cloudinary enforces the signature server-side, so even if someone
        obtains the permanent storage URL from the database they cannot fetch
        the file without a valid, unexpired signature.
        A fresh URL must be generated on each view request.
        """
        if not url:
            return None
        public_path = self.extract_public_path_from_url(url)
        if not public_path:
            return url
        signed, _ = cloudinary.utils.cloudinary_url(
            public_path,
            resource_type="raw",
            type="private",
            sign_url=True,
            secure=True,
            expires_at=int(time.time()) + self.PDF_SIGNED_URL_TTL_SECONDS,
        )
        return signed

    def delete_asset(self, *, public_id: str, resource_type: str = "image") -> bool:
        if not public_id:
            return False

        result = cloudinary.uploader.destroy(
            public_id,
            resource_type=resource_type,
            invalidate=True,
        )
        return result.get("result") in {"ok", "not found"}

    def delete_asset_by_url(self, *, url: str, resource_type: str = "image") -> bool:
        public_id = self.extract_public_id_from_url(url)
        if not public_id:
            return False
        return self.delete_asset(public_id=public_id, resource_type=resource_type)


cloudinary_service = CloudinaryService()
