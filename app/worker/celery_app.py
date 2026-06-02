from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_process_init
from kombu import Exchange, Queue

from app.core.config import settings


@worker_process_init.connect
def reset_motor_client_on_fork(**kwargs):
    """
    Celery prefork clones the parent process after the first Motor client is created.
    Forked children inherit the parent's file descriptors and event loop reference, but
    each process has its own event loop — so the inherited Motor client is unusable and
    any DB call silently fails or raises 'Event loop is closed'.
    Resetting the globals here forces each worker process to open its own fresh connection.
    """
    import app.worker.db as wdb
    wdb._client = None
    wdb._db = None

DLQ_QUEUE_NAME = "dlq_v1"

celery_app = Celery(
    "ehomeo",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.worker.tasks.appointment_tasks",
        "app.worker.tasks.prescription_tasks",
        "app.worker.tasks.cache_tasks",
        "app.worker.tasks.dlq_tasks",
    ],
)

default_exchange = Exchange("default", type="direct")
dlq_exchange = Exchange("dlq", type="direct")

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Kolkata",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    result_expires=3600,
    task_always_eager=settings.CELERY_TASK_ALWAYS_EAGER,
    task_queues=(
        Queue("default", exchange=default_exchange, routing_key="default"),
        Queue("payments", exchange=default_exchange, routing_key="payments"),
        Queue("notifications", exchange=default_exchange, routing_key="notifications"),
        Queue(DLQ_QUEUE_NAME, exchange=dlq_exchange, routing_key="dlq"),
    ),
    task_default_queue="default",
    task_default_exchange="default",
    task_default_exchange_type="direct",
    task_default_routing_key="default",
    task_routes={
        "app.worker.tasks.dlq_tasks.*": {
            "queue": DLQ_QUEUE_NAME,
            "exchange": "dlq",
            "routing_key": "dlq",
        },
        "app.worker.tasks.appointment_tasks.process_refund_for_appointment": {
            "queue": "payments",
            "routing_key": "payments",
        },
        "app.worker.tasks.appointment_tasks.process_pending_refunds": {
            "queue": "payments",
        },
        "app.worker.tasks.appointment_tasks.reconcile_captured_payments": {
            "queue": "payments",
        },
        "app.worker.tasks.appointment_tasks.send_24hr_reminder_emails": {
            "queue": "notifications",
        },
        "app.worker.tasks.appointment_tasks.send_12hr_whatsapp_reminders": {
            "queue": "notifications",
        },
        "app.worker.tasks.appointment_tasks.enforce_disconnect_timeout": {
            "queue": "default",
        },
        "app.worker.tasks.*": {
            "queue": "default",
            "exchange": "default",
            "routing_key": "default",
        },
    },
)

celery_app.conf.beat_schedule = {
    "expire-pending-payments": {
        "task": "app.worker.tasks.appointment_tasks.expire_pending_payments",
        "schedule": 300.0,
    },
    "auto-no-show": {
        "task": "app.worker.tasks.appointment_tasks.auto_no_show",
        "schedule": 900.0,
    },
    "process-refunds": {
        "task": "app.worker.tasks.appointment_tasks.process_pending_refunds",
        "schedule": 1800.0,
    },
    "stale-video-cleanup": {
        "task": "app.worker.tasks.appointment_tasks.cleanup_stale_video_rooms",
        "schedule": 1800.0,
    },
    "send-24hr-reminders": {
        "task": "app.worker.tasks.appointment_tasks.send_24hr_reminder_emails",
        "schedule": crontab(hour=9, minute=0),
    },
    "send-12hr-whatsapp-reminders": {
        "task": "app.worker.tasks.appointment_tasks.send_12hr_whatsapp_reminders",
        "schedule": 900.0,
    },
    "warm-doctor-cache": {
        "task": "app.worker.tasks.cache_tasks.warm_doctor_cache",
        "schedule": crontab(hour=7, minute=0),
    },
    "expire-followup-eligibility": {
        "task": "app.worker.tasks.appointment_tasks.expire_followup_eligibility",
        "schedule": crontab(hour=2, minute=0),
    },
    "cleanup-prescription-drafts": {
        "task": "app.worker.tasks.prescription_tasks.cleanup_prescription_drafts",
        "schedule": crontab(hour=3, minute=0),
    },
    "cleanup-stuck-generation-locks": {
        # Runs every 15 minutes. Resets generation_in_progress flags that were
        # never cleared because the worker process crashed mid-PDF-generation.
        # Safe to run frequently: only touches documents where updated_at is
        # already more than 10 minutes old, so live generations are never touched.
        "task": "app.worker.tasks.prescription_tasks.cleanup_stuck_generation_locks",
        "schedule": 900.0,
    },
    "reconcile-captured-payments": {
        # Polls Razorpay for appointments that never received a payment.captured
        # webhook. Runs every 30 minutes; only targets orders whose hold window
        # expired at least 30 minutes ago, so there is no overlap with live orders.
        "task": "app.worker.tasks.appointment_tasks.reconcile_captured_payments",
        "schedule": 1800.0,
    },
    "write-beat-heartbeat": {
        "task": "app.worker.tasks.cache_tasks.write_beat_heartbeat",
        "schedule": 300.0,
    }
}
