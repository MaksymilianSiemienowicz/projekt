import asyncio
import os
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from confluent_kafka.admin import AdminClient, NewTopic
import clickhouse_connect
import time 

MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
KAFKA_SERVERS = os.environ.get("KAFKA_SERVERS", "localhost:9092")
MONGO_DB = os.environ.get("MONGO_DB","mydb")

mongo_client = None
kafka_client = None

async def create_topic(topic_name: str):
    global kafka_client

    try:
        new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)

        fs = kafka_client.create_topics([new_topic])

        for topic, f in fs.items():
            try:
                f.result() 
                print(f"Topic {topic} utworzony!")
            except Exception as e:
                print(f"Błąd przy tworzeniu topicu {topic}: {e}")
                return False

        return True

    except Exception as e:
        print("Błąd:", e)
        return False

def create_clickhouse_raw_tables(user_id: str, flow_id: str):
    client = clickhouse_connect.get_client(
        host="db2.projekt.home",
        port=9000,
        username="default",
        password=''
    )
    db_name = f"ch_{user_id}_db"
    local_table = f"output_{flow_id}_local"
    mt_table = f"output_{flow_id}_mt"
    distributed_table = f"output_{flow_id}"
    kafka_topic = f"{flow_id}_output"
    kafka_brokers = "kafka1.projekt.home:9092,kafka2.projekt.home:9092,kafka3.projekt.home:9092"

    results = {"database": False, "local_table": False, "mt_table": False, 
               "materialized_view": False, "distributed_table": False, "grants": []}

    try:
        print(f"[ClickHouse] Tworzenie bazy: {db_name}")
        client.command(f"CREATE DATABASE IF NOT EXISTS {db_name} ON CLUSTER default")
        results["database"] = True

        print(f"[ClickHouse] Tworzenie local table: {local_table}")
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{local_table} (
                message String
            ) ENGINE = Kafka(
                '{kafka_brokers}',
                '{kafka_topic}',
                '{user_id}_group',
                'RawBLOB'
            )
        """)
        results["local_table"] = True

        client.command(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{mt_table}
            ON CLUSTER default
        (
            message String,
            created_at DateTime DEFAULT now()
        )
        ENGINE = ReplicatedMergeTree(
            '/clickhouse/tables/{db_name}/{mt_table}',
            '{{replica}}'
        )
        ORDER BY tuple()
        """)
        results["mt_table"] = True

        print(f"[ClickHouse] Tworzenie materialized view: mv_{flow_id}")
        client.command(f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS {db_name}.mv_{flow_id}
            TO {db_name}.{mt_table}
            AS SELECT message, now() AS created_at FROM {db_name}.{local_table}
        """)
        results["materialized_view"] = True
        time.sleep(1)

        print(f"[ClickHouse] Tworzenie distributed table: {distributed_table}")
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{distributed_table} ON CLUSTER default 
            (
                message String,
                created_at DateTime
            )
            ENGINE = Distributed(
                'default',
                '{db_name}',
                '{mt_table}',
                rand()
            )
        """)
        results["distributed_table"] = True

        client.command(f"SYSTEM RELOAD DISTRIBUTED TABLE {db_name}.{distributed_table}")

        for table in [local_table, mt_table, distributed_table]:
            grant_cmd = f"GRANT SELECT, INSERT ON {db_name}.{table} TO ch_{user_id} ON CLUSTER default"
            try:
                print(f"[ClickHouse] Grant: {grant_cmd}")
                client.command(grant_cmd)
                results["grants"].append({table: "OK"})
            except Exception as e:
                print(f"[ClickHouse] Błąd przy grant dla {table}: {e}")
                results["grants"].append({table: f"FAIL: {e}"})

        print("[ClickHouse] Wszystkie operacje zakończone")
    except Exception as e:
        print(f"[ClickHouse] Błąd ogólny: {e}")
        results["error"] = str(e)

    return results

class UserRequest(BaseModel):
    user_id: str
    flow_id: str


async def init_mongo():
    global mongo_client
    mongo_client = AsyncIOMotorClient(MONGO_URL)


async def init_kafka():
    global kafka_client
    kafka_client = AdminClient({"bootstrap.servers": KAFKA_SERVERS})

async def find_user_by_id(user_id: str):
    global mongo_client
    try:
        db = mongo_client[MONGO_DB]
        users = db.users

        oid = ObjectId(user_id)
        return await users.find_one({"_id": oid})

    except Exception as e:
        print("Błąd:", e)
        return None


async def find_flow_by_id(flow_id: str):
    global mongo_client
    try:
        db = mongo_client[MONGO_DB]
        flows = db.flows

        oid = ObjectId(flow_id)
        return await flows.find_one({"_id": oid})

    except Exception as e:
        print("Błąd:", e)
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_mongo()
    await init_kafka()
    yield
    if mongo_client:
        mongo_client.close()

app = FastAPI(lifespan=lifespan)
@app.post("/flows")
async def get_user(data: UserRequest):
    print("REQUEST RECEIVED:", data)
    user = await find_user_by_id(data.user_id)
    flow = await find_flow_by_id(data.flow_id)
    print("FIND USER:", user)
    print("FIND FLOW:", flow)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    user["_id"] = str(user["_id"])
    flow["_id"] = str(flow["_id"])
    flow["user_id"] = str(flow["user_id"])

    input_topic_name = f"{flow['_id']}_input"
    output_topic_name = f"{flow['_id']}_output"

    input_created = await create_topic(input_topic_name)
    output_created = await create_topic(output_topic_name)
    clickhouse_results = create_clickhouse_raw_tables(user["_id"], flow["_id"])
    print("CLICKHOUSE RESULTS:", clickhouse_results)

    return {
        "mongo_user": user,
        "flow": flow,
        "clickhouse_results": clickhouse_results
    }
