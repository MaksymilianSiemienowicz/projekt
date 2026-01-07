import os
from fastapi import FastAPI, HTTPException, Request, Query
from motor.motor_asyncio import AsyncIOMotorClient
from aiokafka import AIOKafkaProducer
from bson import ObjectId, errors

app = FastAPI()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "mydb")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
db = None
producer: AIOKafkaProducer | None = None

@app.on_event("startup")
async def startup_event():
    global db, producer

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[MONGO_DB]
    print(f"MongoDB initialized: {MONGO_URI}/{MONGO_DB}")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        linger_ms=5,
    )
    await producer.start()
    print(f"Kafka producer started: {KAFKA_BOOTSTRAP}")


@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()
        print("Kafka producer stopped")

def validate_object_id(id_str: str) -> ObjectId:
    id_str = id_str.strip()
    try:
        return ObjectId(id_str)
    except errors.InvalidId:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid ObjectId: {id_str}"
        )

@app.post("/{flow_id}")
async def ingest(
    flow_id: str,
    request: Request,
    API_KEY: str = Query(..., description="User API key")
):
    flow_oid = validate_object_id(flow_id)

    user = await db.users.find_one({"API_KEY": API_KEY})
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Invalid API_KEY"
        )

    user_oid = user["_id"]

    flow = await db.flows.find_one({
        "_id": flow_oid,
        "user_id": user_oid
    })
    if not flow:
        raise HTTPException(
            status_code=404,
            detail="Flow not found or does not belong to this user"
        )

    body_bytes = await request.body()

    topic_name = f"{flow_id}_input"
    await producer.send_and_wait(
        topic=topic_name,
        value=body_bytes,
        key=str(flow_oid).encode("utf-8")
    )

    return {
        "status": "ok",
        "flow_id": flow_id,
        "user_id": str(user_oid),
        "sent_to_topic": topic_name
    }
