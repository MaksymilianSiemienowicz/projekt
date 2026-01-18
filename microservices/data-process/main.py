import os
import sys
import json
import asyncio
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from bson import ObjectId
import logging

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "mydb")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka1.projekt.home:9092")
FLOW_ID = os.getenv("FLOW_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not FLOW_ID:
    raise ValueError("FLOW_ID must be set as env variable")

flow_oid = ObjectId(FLOW_ID)

INPUT_TOPIC = f"{FLOW_ID}_input"
OUTPUT_TOPIC = f"{FLOW_ID}_output"

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[MONGO_DB]

async def save_error(message: str):
    existing = await db.errors.find_one({
        "flow_id": FLOW_ID,
        "message": message
    })

    if existing:
        return

    error_doc = {
        "message": message,
        "flow_id": FLOW_ID,
        "date": datetime.utcnow()
    }
    await db.errors.insert_one(error_doc)

USER_PACKAGES_DIR = "/opt/user-packages"
os.makedirs(USER_PACKAGES_DIR, exist_ok=True)
sys.path.insert(0, USER_PACKAGES_DIR)

async def install_user_packages():
    """Instaluje pakiety użytkownika PRZED startem konsumera Kafki."""
    flow = await db.flows.find_one({"_id": flow_oid})
    if not flow:
        msg = f"Flow {FLOW_ID} not found during package install"
        logger.error("Error:", msg)
        await save_error(msg)
        raise RuntimeError(msg)

    packages = flow.get("packages") or []

    if not packages:
        logger.info("No user packages to install")
        return

    logger.info("Installing user packages: %s", packages)
    logger.info("Kafka consumer will start AFTER installation completes...")

    cmd = [
        sys.executable,
        "-m", "pip",
        "install",
        "--no-cache-dir",
        "--target", USER_PACKAGES_DIR,
    ] + packages

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        msg = f"pip install failed (exit code {proc.returncode}): {stderr.decode()}"
        logger.error("Error: %s", msg)
        await save_error(msg)
        raise RuntimeError(msg)

    logger.info(f"Packages installed successfully")
    if stdout:
        logger.info("[PIP]: %s",stdout.decode())

async def process_message(producer, msg_bytes, user_id):
    flow = await db.flows.find_one({"_id": flow_oid})
    if not flow:
        msg = f"Flow {FLOW_ID} not found during message processing"
        logger.error("Error: %s",msg)
        await save_error(msg)
        return

    func_code = flow.get("function")
    if not func_code:
        msg = f"Flow {FLOW_ID} has no function"
        logger.warning("Message: %s",msg)
        await save_error(msg)
        return

    local_vars = {
        "data": msg_bytes,
        "result": None
    }

    try:
        exec(func_code, {}, local_vars)
        processed = local_vars.get("result")

        await producer.send_and_wait(
            OUTPUT_TOPIC,
            value=json.dumps(processed).encode("utf-8")
        )

    except Exception as e:
        msg = f"Function execution error: {str(e)}"
        logger.error("Error: %s",msg)
        await save_error(msg)

async def main():
    await install_user_packages()
    
    logger.info("Package installation complete, starting Kafka consumer...")

    consumer = AIOKafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"flow-{FLOW_ID}-group",
        auto_offset_reset="earliest"
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP
    )

    await producer.start()
    await consumer.start()
    logger.info("Listening to: %s", INPUT_TOPIC)

    try:
        async for msg in consumer:
            flow_doc = await db.flows.find_one({"_id": flow_oid})
            user_id = flow_doc.get("user_id") if flow_doc else None
            await process_message(producer, msg.value, user_id)
    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())

