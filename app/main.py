import logging
import uuid
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi_limiter import FastAPILimiter
from sentry_sdk.integrations.fastapi import FastApiIntegration

from app.core.config import settings
from app.core.csrf import CSRFMiddleware
from app.core.logging import setup_logging
from app.core.database import connect_db, close_db, get_db
from app.core.redis import close_redis, connect_redis, get_redis
from app.services.admin_bootstrap_service import bootstrap_admin_if_needed

from app.routes.health import router as health_router
from app.routes.auth_routes import router as auth_router
from app.routes.patient_routes import router as patient_router
from app.routes.availability_routes import router as availability_router
from app.routes.public_slots_routes import router as public_slots_router
from app.routes.public_booking_routes import router as public_booking_router
from app.routes.doctor_appointments_routes import router as doctor_appt_router
from app.routes.doctor_blocks_routes import router as doctor_blocks_router
from app.routes.doctor_appointment_actions_routes import router as doctor_appt_actions_router
from app.routes.public_doctors_routes import router as public_doctors_router
from app.routes.public_appointment_actions_routes import router as public_appt_actions_router
from app.routes.doctor_profile_routes import router as doctor_profile_router
from app.routes.patient_auth_routes import router as patient_auth_routes
from app.routes.patient_appointments_routes import router as patient_appointments_routes
from app.routes.prescription_routes import router as prescription_routes
from app.routes.receipt_routes import router as receipt_routes
from app.routes.doctor_notes_routes import router as doctor_notes_router
from app.routes.webhook_routes import router as webhook_router
from app.routes.livekit_webhook_routes import router as livekit_webhook_router
from app.routes.admin_auth_routes import router as admin_auth_router
from app.routes.admin_doctor_routes import router as admin_doctor_router
from app.routes.admin_appointment_routes import router as admin_appointment_router
from app.routes.patient_feature_routes import router as patient_feature_router
from app.routes.sse_routes import router as sse_router

setup_logging()
logger = logging.getLogger(__name__)

API_PREFIX = "/api/v1"
_is_dev = settings.ENV.lower() not in {"prod", "production", "staging"}

if settings.SENTRY_DSN:
    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.SENTRY_ENVIRONMENT,
        integrations=[FastApiIntegration()],
        traces_sample_rate=0.2,
        send_default_pii=False,
    )
    logger.info("Sentry initialized for environment=%s", settings.SENTRY_ENVIRONMENT)
else:
    logger.info("Sentry disabled (SENTRY_DSN is empty)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    from app.services.stuck_payments_cron import start_stuck_payment_recovery_cron
    
    await connect_db()
    await bootstrap_admin_if_needed(get_db())
    await connect_redis()
    await FastAPILimiter.init(get_redis(), prefix="rate", http_callback=_rate_limit_callback)
    
    cron_task = asyncio.create_task(start_stuck_payment_recovery_cron())
    
    # Boot-time call reconciliation check
    from app.services.call_state_machine import startup_reconcile_calls
    asyncio.create_task(startup_reconcile_calls())
    
    yield
    cron_task.cancel()
    
    await FastAPILimiter.close()
    await close_redis()
    await close_db()


app = FastAPI(
    title=getattr(settings, "APP_NAME", "HPHomeo API"),
    version=getattr(settings, "APP_VERSION", "1.0.0"),
    description=getattr(settings, "APP_DESCRIPTION", "HPHomeo backend API"),
    docs_url=f"{API_PREFIX}/docs" if _is_dev else None,
    redoc_url=f"{API_PREFIX}/redoc" if _is_dev else None,
    openapi_url=f"{API_PREFIX}/openapi.json" if _is_dev else None,
    lifespan=lifespan,
)


def _dev_origin_regex() -> str:
    # Allow local development hosts, common private LAN ranges, and ngrok tunnels.
    host = r"(localhost|127\.0\.0\.1|192\.168\.\d{1,3}\.\d{1,3}|10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}|[a-zA-Z0-9-]+\.ngrok-free\.app|[a-zA-Z0-9-]+\.ngrok\.app)"
    port = r"(?::\d{1,5})?"
    return rf"^https?://{host}{port}$"


# In production, set APP_BASE_URL to your frontend origin (e.g. https://hphomeo.com)
_cors_origins = (
    ["http://localhost:3000", "http://localhost:5173"]
    if _is_dev
    else [settings.APP_BASE_URL] if settings.APP_BASE_URL else []
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_origin_regex=_dev_origin_regex() if _is_dev else None,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
)

# CSRF origin verification — enforced in production only.
app.add_middleware(CSRFMiddleware)


@app.middleware("http")
async def request_id_and_security_headers(request: Request, call_next):
    import time
    from loguru import logger
    from app.core.logging import (
        request_id_context,
        request_path_context,
        request_method_context,
        request_duration_context,
    )

    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    request.state.request_id = request_id

    start_time = time.time()
    token_id = request_id_context.set(request_id)
    token_path = request_path_context.set(request.url.path)
    token_method = request_method_context.set(request.method)

    try:
        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000
        request_duration_context.set(duration_ms)
        logger.info("request_completed status_code=%s duration_ms=%s", response.status_code, round(duration_ms, 2))
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        request_duration_context.set(duration_ms)
        logger.error("request_failed error=%s duration_ms=%s", str(e), round(duration_ms, 2))
        raise e
    finally:
        request_id_context.reset(token_id)
        request_path_context.reset(token_path)
        request_method_context.reset(token_method)

    response.headers["X-Request-ID"] = request_id
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(self), camera=(self)"

    # Allow same-origin iframes for PDF preview endpoints; block framing elsewhere
    path = request.url.path
    if "/pdf/view" in path:
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
    else:
        response.headers["X-Frame-Options"] = "DENY"
    if not _is_dev:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

    return response


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    content = {"detail": "Not found"}
    if _is_dev:
        content["path"] = request.url.path
    return JSONResponse(status_code=404, content=content)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # In production, only expose which fields failed — not internal type/ctx details
    # that could aid enumeration or reveal schema internals.
    if _is_dev:
        encoded_errors = jsonable_encoder(exc.errors())
    else:
        encoded_errors = [
            {"field": " → ".join(str(loc) for loc in e.get("loc", [])), "message": e.get("msg", "invalid")}
            for e in exc.errors()
        ]
    return JSONResponse(
        status_code=422,
        content={
            "detail": "Validation error",
            "errors": encoded_errors,
        },
    )

app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Keep webhooks unversioned for provider callbacks.
app.include_router(webhook_router)
app.include_router(livekit_webhook_router)

# Version all app-facing APIs.
app.include_router(health_router, prefix=API_PREFIX)
app.include_router(auth_router, prefix=API_PREFIX)
app.include_router(patient_router, prefix=API_PREFIX)
app.include_router(availability_router, prefix=API_PREFIX)
app.include_router(public_slots_router, prefix=API_PREFIX)
app.include_router(public_booking_router, prefix=API_PREFIX)
app.include_router(doctor_appt_router, prefix=API_PREFIX)
app.include_router(doctor_blocks_router, prefix=API_PREFIX)
app.include_router(doctor_appt_actions_router, prefix=API_PREFIX)
app.include_router(public_doctors_router, prefix=API_PREFIX)
app.include_router(public_appt_actions_router, prefix=API_PREFIX)
app.include_router(doctor_profile_router, prefix=API_PREFIX)
app.include_router(patient_auth_routes, prefix=API_PREFIX)
app.include_router(patient_appointments_routes, prefix=API_PREFIX)
app.include_router(prescription_routes, prefix=API_PREFIX)
app.include_router(receipt_routes, prefix=API_PREFIX)
app.include_router(doctor_notes_router, prefix=API_PREFIX)
app.include_router(admin_auth_router, prefix=API_PREFIX)
app.include_router(admin_doctor_router, prefix=API_PREFIX)
app.include_router(admin_appointment_router, prefix=API_PREFIX)
app.include_router(patient_feature_router, prefix=API_PREFIX)
app.include_router(sse_router, prefix=API_PREFIX)

@app.get("/")
async def home():
    return {
        "app": getattr(settings, "APP_NAME", "HPHomeo API"),
        "version": getattr(settings, "APP_VERSION", "1.0.0"),
        "docs": f"{API_PREFIX}/docs",
        "status": "running",
    }


async def _rate_limit_callback(request: Request, response: Response, pexpire: int):
    ttl_ms = max(0, int(pexpire))
    retry_after = str(max(1, (ttl_ms + 999) // 1000))
    route = request.url.path
    ip = request.client.host if request.client else "unknown"
    logger.warning("rate_limited route=%s ip=%s ttl_ms=%s", route, ip, ttl_ms)
    raise HTTPException(
        status_code=429,
        detail="Rate limit exceeded",
        headers={"Retry-After": retry_after},
    )
