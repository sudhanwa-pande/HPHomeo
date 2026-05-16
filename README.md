# hpHomeo — Scalable Clinic Backend

<<<<<<< HEAD
**🔗 [View the Frontend Next.js Repository Here](https://github.com/sudhanwa-pande/HPHomeo-webapp)**

## 🚀 Overview
=======
## Overview
>>>>>>> 5d05c97bf9d9831c63cf3481d3fb3ada60bcaf08

A production-grade, highly concurrent FastAPI backend engineered specifically for homeopathic doctors—bridging the gap in the market for a dedicated, specialized telemedicine platform. Built to handle complex scheduling rules, race-condition-proof booking, LiveKit WebRTC video consultations, and resilient payment processing. Features an event-driven architecture utilizing Celery and Redis to decouple background operations from the synchronous critical path.

---

## Architecture

<p align="center">
  <img src="app/Architecture_diagram/hpHomeo%20%E2%80%94%20Architecture%20v5%20(Hierarchy).png" alt="Architecture Diagram" width="850">
</p>

**Decoupled Event-Driven System:** The API layer offloads non-critical path operations (notifications, PDF generation) to Celery workers via Redis, while maintaining synchronous database consistency and streaming real-time updates over SSE.

---

## Booking Flow

<p align="center">
  <img src="app/Architecture_diagram/hpHomeo%20%E2%80%94%201.%20Booking%20Flow%20(v2).png" alt="Booking Sequence Diagram" width="850">
</p>

**Race-Condition-Proof Scheduling:** Utilizes database-level partial unique constraints for atomic slot conflict prevention. Successful bookings instantly generate scoped magic tokens and initialize payment orders.

---

## Payment Lifecycle

<p align="center">
  <img src="app/Architecture_diagram/hpHomeo%20%E2%80%94%202.%20Payment%20Webhook%20Flow%20(v2).png" alt="Payment Webhook Flow" width="850">
</p>

**Resilient Webhook Processing:** Webhooks are guarded by strict idempotency checks against the database state, triggering asynchronous background workers for final reconciliation and external notifications upon successful capture.

---

## Tech Stack

*   **Backend:** FastAPI, Python
*   **Database:** MongoDB (Motor)
*   **Queue / Workers:** Redis, Celery (Beat)
*   **Payments:** Razorpay
*   **Realtime / Video:** Server-Sent Events (SSE), LiveKit (WebRTC)
*   **Storage / Messaging:** Cloudinary, WhatsApp API, Resend

---

## Design Principles

*   **Idempotency (Webhooks):** All webhook handlers enforce state-based conditional updates to safely absorb duplicate or retried external events without data corruption.
*   **Async-First Architecture:** Native asynchronous IO maximizes throughput for concurrent operations like schedule rendering and LiveKit video room provisioning.
*   **Separation of Concerns:** Rigid boundaries between public routes and admin APIs, with synchronous critical paths fully isolated from long-running background tasks.
*   **Failure Handling (DLQ, Retries):** Background workers utilize exponential backoff and explicit retries, routing terminal failures to a Dead Letter Queue (DLQ) to ensure zero lost events.
*   **Security (JWT, Magic Tokens):** Medical professionals authenticate via short-lived JWTs, while patients receive tightly-scoped, auto-expiring magic tokens stored in HTTPOnly cookies for frictionless access.

---

## Getting Started

```bash
# Clone repository
git clone https://github.com/sudhanwa-pande/HPHomeo.git
cd HPHomeo

# Set up environment variables
cp .env.example .env

# Spin up infrastructure (MongoDB, Redis, Celery)
docker-compose up -d

# Run application
pip install -r requirements.txt
fastapi dev app/main.py
```

---

## Limitations & Future Roadmap

*   **Current Constraint (NoSQL):** The system currently relies on MongoDB. While highly scalable and fast for denormalized reads, NoSQL lacks strict relational integrity and makes complex financial/transactional auditing more challenging.
*   **Future Improvement (PostgreSQL):** We are planning a full migration to **PostgreSQL**. This will bring robust ACID transactions, strict relational data modeling, Alembic-based schema migrations, and improved data consistency guarantees for our core billing schemas.
*   **Observability Gap:** Currently, the system relies on standard logging, we are using sentry for error tracking. We plan to implement distributed tracing (e.g. **OpenTelemetry**) to trace requests across asynchronous boundaries (FastAPI -> Redis -> Celery) for better debugging.
*   **Worker Graceful Shutdowns:** Long-running Celery tasks require more robust graceful shutdown handling during CI/CD deployments to ensure zero dropped jobs.
