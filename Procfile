web: uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 2 --proxy-headers --forwarded-allow-ips="*"
worker: celery -A app.worker.celery_app worker --loglevel=info --concurrency=2
beat: celery -A app.worker.celery_app beat --loglevel=info --schedule=/tmp/celerybeat-schedule
