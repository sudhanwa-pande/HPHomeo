import logging

logger = logging.getLogger(__name__)


def enqueue_refund_processing(appointment_id: str) -> None:
    if not appointment_id:
        return

    try:
        from app.worker.tasks.appointment_tasks import process_refund_for_appointment

        process_refund_for_appointment.delay(appointment_id)
    except Exception:
        logger.warning(
            "Failed to enqueue immediate refund processing for appointment_id=%s",
            appointment_id,
            exc_info=True,
        )
