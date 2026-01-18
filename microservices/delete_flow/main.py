import asyncio
import os
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient
import clickhouse_connect
import logging
import re

KAFKA_SERVERS = os.environ.get("KAFKA_SERVERS", "localhost:9092")

kafka_client = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def delete_topic(topic_name: str):
    global kafka_client
    try:
        fs = kafka_client.delete_topics([topic_name])
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"[Kafka] Topic %s has been deleted", topic)
            except Exception as e:
                logger.error("[Kafka] Error while deleteing %s: %s", topic, e)
        return True
    except Exception as e:
        logger.error("Global error: %s", e)
        return False


def delete_clickhouse_tables(user_id: str, flow_id: str):
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "")
    )
    
    OBJECT_ID_RE = re.compile(r"^[a-f0-9]{24}$")
    if not OBJECT_ID_RE.fullmatch(user_id):
        raise ValueError(f"{user_id} - user_id is invalid")

    if not OBJECT_ID_RE.fullmatch(flow_id):
        raise ValueError(f"{flow_id} - flow_id is invalid")

    db_name = f"ch_{user_id}_db"
    local_table = f"output_{flow_id}_local"
    mt_table = f"output_{flow_id}_mt"
    distributed_table = f"output_{flow_id}"
    materialized_view = f"mv_{flow_id}"

    results = {}
    try:
        client.command(f"DROP TABLE IF EXISTS {db_name}.{materialized_view}")
        results["materialized_view"] = "dropped"
    except Exception as e:
        results["materialized_view"] = f"error: {e}"
    try:
        client.command(f"DROP TABLE IF EXISTS {db_name}.{local_table}")
        results["local_table"] = "dropped"
    except Exception as e:
        results["local_table"] = f"error: {e}"
    try:
        client.command(f"DROP TABLE IF EXISTS {db_name}.{mt_table} ON CLUSTER default")
        results["mt_table"] = "dropped"
    except Exception as e:
        results["mt_table"] = f"error: {e}"
    try:
        client.command(f"DROP TABLE IF EXISTS {db_name}.{distributed_table} ON CLUSTER default")
        results["distributed_table"] = "dropped"
    except Exception as e:
        results["distributed_table"] = f"error: {e}"

    return results

class UserRequest(BaseModel):
    user_id: str
    flow_id: str

async def init_kafka():
    global kafka_client
    kafka_client = AdminClient({"bootstrap.servers": KAFKA_SERVERS})

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_kafka()
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/delete_user_flow")
async def delete_user_flow(data: UserRequest):
    user_id = data.user_id
    flow_id = data.flow_id

    ch_results = delete_clickhouse_tables(user_id, flow_id)

    input_topic = f"{flow_id}_input"
    output_topic = f"{flow_id}_output"
    await delete_topic(input_topic)
    await delete_topic(output_topic)

    return {
        "clickhouse_tables_deleted": ch_results,
        "kafka_topics_deleted": [input_topic, output_topic]
    }
