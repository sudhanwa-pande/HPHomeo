import asyncio
import logging
from datetime import timedelta
from app.core.database import get_db
from app.utils.time import utc_now
from app.services.razorpay_service import client
from app.routes.webhook_routes import _handle_payment_captured

logger = logging.getLogger(__name__)

async def start_stuck_payment_recovery_cron():
    logger.info("Starting stuck payment recovery cron task (runs every 10m).")
    while True:
        try:
            await _recover_stuck_payments()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Error in stuck payment recovery cron: %s", str(e), exc_info=True)
        
        # Run every 10 minutes
        await asyncio.sleep(600)

async def _recover_stuck_payments():
    db = get_db()
    if db is None:
        return
        
    now = utc_now()
    five_mins_ago = now - timedelta(minutes=5)
    
    # Find appointments that are pending_payment for > 5 minutes but have an order
    # Note: Using status instead of payment_status if it's stored that way, or just payment_status.
    cursor = db.appointments.find({
        "payment_status": "pending",
        "payment_order_id": {"$exists": True, "$ne": None},
        "created_at": {"$lt": five_mins_ago}
    })
    
    async for appt in cursor:
        order_id = appt.get("payment_order_id")
        appt_id = str(appt["_id"])
        
        if not order_id:
            continue
            
        try:
            # Fetch payments for the order from Razorpay
            payments_res = await asyncio.to_thread(client.order.payments, order_id)
            
            captured_payment = None
            if payments_res and "items" in payments_res:
                for p in payments_res["items"]:
                    if p.get("status") == "captured":
                        captured_payment = p
                        break
                        
            if captured_payment:
                logger.warning(
                    "Stuck payment recovery: Found captured payment %s for appointment %s. Re-triggering webhook logic.",
                    captured_payment["id"], appt_id
                )
                
                payload = {
                    "event": "payment.captured",
                    "payload": {
                        "payment": {
                            "entity": captured_payment
                        }
                    }
                }
                
                # Resubmit to webhook handler
                await _handle_payment_captured(payload)
                
        except Exception as e:
            logger.error(
                "Stuck payment recovery: Failed to fetch/process order %s for appointment %s: %s",
                order_id, appt_id, str(e)
            )
