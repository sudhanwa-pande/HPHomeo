"""
CSRF protection via Origin header verification.

Why this is sufficient for a modern SaaS app:

1. SameSite=Lax cookies  — browser refuses to send auth cookies on
   cross-origin form POSTs.  This alone blocks classic form-based CSRF.

2. CORS with credentials   — browser blocks cross-origin AJAX that carries
   cookies unless the server explicitly allows that origin.

3. Content-Type: application/json — this is NOT a CORS "simple" content
   type, so any cross-origin AJAX automatically triggers a preflight
   OPTIONS request.  If the origin isn't allowed, the request never fires.

This middleware adds a fourth layer (defense-in-depth): on every state-
changing request it verifies that the Origin (or Referer) header matches
one of the allowed origins.  Unlike double-submit cookies, this approach
has zero moving parts on the frontend — no tokens, no cookies to read,
no timing edge-cases.

Webhooks are exempt because they authenticate via their own HMAC
signatures (e.g. Razorpay webhook secret).
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.core.config import settings

logger = logging.getLogger(__name__)

_STATE_CHANGING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# Paths that receive callbacks from external services (their own auth).
_EXEMPT_PREFIXES: tuple[str, ...] = (
    "/webhooks/",
)


def _is_exempt(path: str) -> bool:
    return any(path.startswith(p) for p in _EXEMPT_PREFIXES)


def _allowed_origins() -> set[str]:
    """Build the set of origins that are permitted to make state-changing requests."""
    origins: set[str] = set()

    # The app's own base URL (e.g. https://hphomeo.com)
    if settings.APP_BASE_URL:
        parsed = urlparse(settings.APP_BASE_URL)
        origins.add(f"{parsed.scheme}://{parsed.netloc}")

    # Always allow local development origins
    is_dev = settings.ENV.lower() not in {"prod", "production", "staging"}
    if is_dev:
        origins.update({
            "http://localhost:3000",
            "http://localhost:5173",
        })

    return origins


def _get_request_origin(request: Request) -> str | None:
    """Extract the origin from the Origin header, falling back to Referer."""
    origin = request.headers.get("origin")
    if origin and origin != "null":
        return origin

    referer = request.headers.get("referer")
    if referer:
        parsed = urlparse(referer)
        return f"{parsed.scheme}://{parsed.netloc}"

    return None


class CSRFMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, **kwargs):
        super().__init__(app, **kwargs)
        self._allowed = _allowed_origins()
        self._is_prod = settings.ENV.lower() in {"prod", "production", "staging"}

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if (
            self._is_prod
            and request.method in _STATE_CHANGING_METHODS
            and not _is_exempt(request.url.path)
        ):
            origin = _get_request_origin(request)

            # Reject if Origin is missing (shouldn't happen from browsers)
            # or doesn't match an allowed origin.
            if not origin or origin not in self._allowed:
                logger.warning(
                    "csrf_blocked origin=%s path=%s",
                    origin or "(missing)",
                    request.url.path,
                )
                return Response(
                    content='{"detail":"Origin not allowed"}',
                    status_code=403,
                    media_type="application/json",
                )

        return await call_next(request)
