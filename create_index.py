import asyncio
from app.core.database import get_db, connect_to_mongo

async def main():
    await connect_to_mongo()
    db = get_db()
    await db.appointments.create_index([("call_status", 1), ("updated_at", 1)])
    print("Index created successfully")

asyncio.run(main())
