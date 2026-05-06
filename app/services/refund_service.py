import logging
import sentry_sdk

logger = logging.getLogger(__name__)


def enqueue_refund_processing(appointment_id: str) -> bool:
    if not appointment_id:
        return False

    try:
        from app.worker.tasks.appointment_tasks import process_refund_for_appointment

        process_refund_for_appointment.apply_async(
            args=[appointment_id],
            task_id=f"refund_{appointment_id}"
        )
        return True
    except Exception as e:
        logger.error(
            "CRITICAL: Failed to enqueue refund processing",
            extra={"appointment_id": appointment_id},
            exc_info=True,
        )
        sentry_sdk.capture_exception(e)
        return False
