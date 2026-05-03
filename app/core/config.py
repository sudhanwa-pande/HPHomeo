import logging
from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal

_config_logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    ENV: str = "dev"
    APP_NAME: str = "HPhomeo"
    APP_VERSION: str = "1.0.0"
    APP_DESCRIPTION: str = "Homeopathic appointment booking platform"

    # Database
    MONGODB_URI: SecretStr
    MONGODB_DB: str = "ehomeo"
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_MAX_CONNECTIONS: int = 20
    REDIS_SOCKET_TIMEOUT: float = 2.0
    REDIS_SOCKET_CONNECT_TIMEOUT: float = 2.0
    REDIS_RETRY_ON_TIMEOUT: bool = True
    CELERY_BROKER_URL: str = "redis://redis:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/2"
    CELERY_TASK_ALWAYS_EAGER: bool = False

    #sentry
    SENTRY_DSN: str | None = None
    SENTRY_ENVIRONMENT: str = "development"

    # Auth & Security
    JWT_SECRET: SecretStr
    JWT_ALGO: str = "HS256"
    JWT_EXPIRE_MIN: int = 15
    JWT_REFRESH_EXPIRE_DAYS: int = 30
    OTP_SECRET: SecretStr
    OTP_TTL_MINUTES: int = 5
    OTP_RESEND_COOLDOWN_SECONDS: int = 60
    TOTP_ENCRYPTION_KEY: SecretStr
    AUTH_REDIS_ENABLED: bool = True
    AUTH_2STEP_ENABLED: bool = True
    BLACKLIST_BACKEND: Literal["mongo", "redis", "dual"] = "redis"

    # Rate limits
    RL_ADMIN_READ_TIMES: int = 60
    RL_ADMIN_READ_SECONDS: int = 60
    RL_ADMIN_WRITE_TIMES: int = 20
    RL_ADMIN_WRITE_SECONDS: int = 60
    RL_ADMIN_LOGOUT_TIMES: int = 30
    RL_ADMIN_LOGOUT_SECONDS: int = 60

    RL_DOCTOR_REGISTER_TIMES: int = 5
    RL_DOCTOR_REGISTER_SECONDS: int = 60
    RL_DOCTOR_LOGIN_TIMES: int = 5
    RL_DOCTOR_LOGIN_SECONDS: int = 900
    RL_DOCTOR_VERIFY_OTP_TIMES: int = 15
    RL_DOCTOR_VERIFY_OTP_SECONDS: int = 60
    RL_DOCTOR_VERIFY_TOTP_TIMES: int = 20
    RL_DOCTOR_VERIFY_TOTP_SECONDS: int = 60
    RL_DOCTOR_REFRESH_TIMES: int = 30
    RL_DOCTOR_REFRESH_SECONDS: int = 60
    RL_DOCTOR_TOTP_SETUP_TIMES: int = 20
    RL_DOCTOR_TOTP_SETUP_SECONDS: int = 60
    RL_DOCTOR_TOTP_ENABLE_TIMES: int = 20
    RL_DOCTOR_TOTP_ENABLE_SECONDS: int = 60
    RL_DOCTOR_LOGOUT_TIMES: int = 30
    RL_DOCTOR_LOGOUT_SECONDS: int = 60
    RL_DOCTOR_ME_TIMES: int = 60
    RL_DOCTOR_ME_SECONDS: int = 60
    RL_DOCTOR_READ_TIMES: int = 60
    RL_DOCTOR_READ_SECONDS: int = 60
    RL_DOCTOR_STATS_TIMES: int = 60
    RL_DOCTOR_STATS_SECONDS: int = 60
    RL_DOCTOR_DAILY_STATS_TIMES: int = 30
    RL_DOCTOR_DAILY_STATS_SECONDS: int = 60
    RL_DOCTOR_VIDEO_JOIN_TIMES: int = 10
    RL_DOCTOR_VIDEO_JOIN_SECONDS: int = 60
    RL_DOCTOR_WAITING_TIMES: int = 120
    RL_DOCTOR_WAITING_SECONDS: int = 60
    RL_DOCTOR_VIDEO_END_TIMES: int = 10
    RL_DOCTOR_VIDEO_END_SECONDS: int = 60

    RL_PATIENT_OTP_PHONE_TIMES: int = 3
    RL_PATIENT_OTP_PHONE_SECONDS: int = 600
    RL_PATIENT_OTP_GENERAL_TIMES: int = 20
    RL_PATIENT_OTP_GENERAL_SECONDS: int = 60
    RL_PATIENT_VERIFY_OTP_TIMES: int = 15
    RL_PATIENT_VERIFY_OTP_SECONDS: int = 60
    RL_PATIENT_REFRESH_TIMES: int = 30
    RL_PATIENT_REFRESH_SECONDS: int = 60
    RL_PATIENT_LOGOUT_TIMES: int = 30
    RL_PATIENT_LOGOUT_SECONDS: int = 60
    RL_PATIENT_ME_TIMES: int = 60
    RL_PATIENT_ME_SECONDS: int = 60
    RL_PATIENT_PROFILE_UPDATE_TIMES: int = 30
    RL_PATIENT_PROFILE_UPDATE_SECONDS: int = 60
    RL_PATIENT_READ_TIMES: int = 60
    RL_PATIENT_READ_SECONDS: int = 60
    RL_PATIENT_VIDEO_JOIN_TIMES: int = 10
    RL_PATIENT_VIDEO_JOIN_SECONDS: int = 60
    RL_PATIENT_WRITE_TIMES: int = 30
    RL_PATIENT_WRITE_SECONDS: int = 60
    RL_PATIENT_MUTATION_TIMES: int = 20
    RL_PATIENT_MUTATION_SECONDS: int = 60

    RL_PUBLIC_READ_TIMES: int = 600
    RL_PUBLIC_READ_SECONDS: int = 60
    RL_PUBLIC_BOOKING_TIMES: int = 20
    RL_PUBLIC_BOOKING_SECONDS: int = 60
    RL_PUBLIC_APPOINTMENT_READ_TIMES: int = 300
    RL_PUBLIC_APPOINTMENT_READ_SECONDS: int = 60
    RL_PUBLIC_PAYMENT_CREATE_TIMES: int = 5
    RL_PUBLIC_PAYMENT_CREATE_SECONDS: int = 60
    RL_PUBLIC_VIDEO_JOIN_TIMES: int = 120
    RL_PUBLIC_VIDEO_JOIN_SECONDS: int = 60
    RL_PUBLIC_ACTION_TIMES: int = 60
    RL_PUBLIC_ACTION_SECONDS: int = 60

    RL_PRESCRIPTION_GENERATE_TIMES: int = 10
    RL_PRESCRIPTION_GENERATE_SECONDS: int = 60

    RL_DOCTOR_FORGOT_PASSWORD_TIMES: int = 3
    RL_DOCTOR_FORGOT_PASSWORD_SECONDS: int = 600
    RL_DOCTOR_RESET_PASSWORD_TIMES: int = 5
    RL_DOCTOR_RESET_PASSWORD_SECONDS: int = 60
    RL_DOCTOR_CHANGE_PASSWORD_TIMES: int = 5
    RL_DOCTOR_CHANGE_PASSWORD_SECONDS: int = 60

    # Cookie settings (httpOnly auth cookies)
    COOKIE_DOMAIN: str | None = None
    COOKIE_SECURE: bool = True
    COOKIE_SAMESITE: Literal["lax", "strict", "none"] = "lax"
    PUBLIC_PATIENT_ACCESS_COOKIE_NAME: str = "public_patient_access_token"
    # Path "/" so the cookie is sent on BOTH /api/v1/public/* (direct API) AND
    # /api/sse/* (the SSE streaming proxy on the frontend). Narrower paths break
    # SSE because the browser drops the cookie on path mismatch.
    PUBLIC_PATIENT_ACCESS_COOKIE_PATH: str = "/"
    
    # Business Logic
    PAYMENT_HOLD_MINUTES: int = 10
    CANCEL_WINDOW_HOURS: int = 2
    FOLLOW_UP_DAYS: int = 7
    BOOKING_WINDOW_DAYS: int = 7

    # Razorpay Credentials
    RAZORPAY_KEY_ID: str
    RAZORPAY_KEY_SECRET: SecretStr
    RAZORPAY_WEBHOOK_SECRET: SecretStr

    # Email (Resend)
    RESEND_API_KEY: str | None = None
    EMAIL_FROM: str = "HPHomeo <noreply@noreply.hphomeo.com>"
    APP_BASE_URL: str  # e.g. https://hphomeo.com

    # WhatsApp (Meta Cloud API)
    WHATSAPP_ENABLED: bool = False
    WHATSAPP_API_BASE_URL: str = "https://graph.facebook.com"
    WHATSAPP_API_VERSION: str = "v22.0"
    WHATSAPP_PHONE_NUMBER_ID: str | None = None
    WHATSAPP_ACCESS_TOKEN: SecretStr | None = None
    WHATSAPP_OTP_TEMPLATE_NAME: str = "something"
    WHATSAPP_CONFIRMATION_TEMPLATE_NAME: str = "appointment_confirmation_1"
    WHATSAPP_REMINDER_TEMPLATE_NAME: str = "appointment_reminder"
    WHATSAPP_RESCHEDULE_TEMPLATE_NAME: str = "appointment_reschedule_1"
    WHATSAPP_CANCELLATION_TEMPLATE_NAME: str = "appointment_cancel"
    WHATSAPP_PRESCRIPTION_TEMPLATE_NAME: str = "appointment_complete"
    WHATSAPP_TEMPLATE_LANG: str = "en_US"
    WHATSAPP_PUBLIC_APPOINTMENT_PATH_PREFIX: str = ""
    WHATSAPP_REMINDER_LEAD_HOURS: int = 12

    # Cloudinary Credentials
    CLOUDINARY_CLOUD_NAME: str
    CLOUDINARY_API_KEY: str
    CLOUDINARY_API_SECRET: SecretStr

    LIVEKIT_URL: str = ""
    LIVEKIT_API_KEY: str = ""
    LIVEKIT_API_SECRET: SecretStr = SecretStr("")
    LIVEKIT_TOKEN_TTL_SECONDS: int = 3600
    LIVEKIT_WEBHOOK_SECRET: str = ""  # defaults to LIVEKIT_API_SECRET if empty
    VIDEO_JOIN_EARLY_MINUTES: int = 10
    VIDEO_JOIN_LATE_GRACE_MINUTES: int = 30
    VIDEO_ENABLED: bool = True
    CALL_DISCONNECT_TIMEOUT_SECONDS: int = 300  # 5 minutes
    CALL_HEARTBEAT_INTERVAL_SECONDS: int = 15
    CALL_HEARTBEAT_TTL_SECONDS: int = 45

    # Optional startup bootstrap for first admin user (recommended for dev only).
    ADMIN_BOOTSTRAP_ENABLED: bool = False
    ADMIN_BOOTSTRAP_EMAIL: str | None = None
    ADMIN_BOOTSTRAP_PHONE: str | None = None
    ADMIN_BOOTSTRAP_PASSWORD: str | None = None
    ADMIN_BOOTSTRAP_FULL_NAME: str | None = None
    ADMIN_BOOTSTRAP_REGISTRATION_NO: str | None = None


    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def _enforce_production_security(self):
        """Safety net: force secure defaults when running in production."""
        is_prod = self.ENV.lower() in {"prod", "production", "staging"}
        if is_prod and not self.COOKIE_SECURE:
            _config_logger.warning(
                "COOKIE_SECURE was false in a production environment — forcing to true"
            )
            self.COOKIE_SECURE = True
        if is_prod and self.JWT_EXPIRE_MIN > 30:
            _config_logger.warning(
                "JWT_EXPIRE_MIN=%d is too long for production — clamping to 15",
                self.JWT_EXPIRE_MIN,
            )
            self.JWT_EXPIRE_MIN = 15
        if is_prod and self.RAZORPAY_KEY_ID.startswith("rzp_test_"):
            raise ValueError(
                "RAZORPAY_KEY_ID must be a live key (rzp_live_...) in production/staging. "
                "Test keys do not charge real money."
            )
        if self.VIDEO_ENABLED and not (
            self.LIVEKIT_URL
            and self.LIVEKIT_API_KEY
            and self.LIVEKIT_API_SECRET.get_secret_value()
        ):
            raise ValueError(
                "VIDEO_ENABLED=true requires LIVEKIT_URL, LIVEKIT_API_KEY, and LIVEKIT_API_SECRET to be set."
            )
        return self

# Instantiate the settings
settings = Settings()
