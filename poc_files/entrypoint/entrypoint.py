from fastapi import FastAPI, Path, Request, HTTPException
from pymongo import MongoClient
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from bson import ObjectId
app = FastAPI()

# --- DB ---
client = MongoClient("mongodb://localhost:27017")
db = client["project"]
users = db["users"]
flows = db["flows"]

# --- Kafka / Schema Registry ---
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

def to_dict(obj, ctx):
    print("➡️ SERIALIZER dostał:", obj, type(obj))
    if isinstance(obj, dict):
        return obj
    return obj.__dict__

def get_producer(schema_name: str):
    avro_schema_str = schema_registry_client.get_latest_version(schema_name).schema.schema_str
    avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str, to_dict)
    producer_conf = {
        "bootstrap.servers": "localhost:9092",
        "value.serializer": avro_serializer
    }
    return SerializingProducer(producer_conf)

# --- Helpers ---
def getUser(api_key):
    return users.find_one({"API_KEY": api_key})

def getFlow(flow_id):
    return flows.find_one({"_id": ObjectId(flow_id)})
def validateUser(user, flow):
    return user and flow and user["_id"] == flow["user_id"]

# --- Endpoint ---
@app.post("/{api_key}/{flow_id}")
async def handle_request(
    api_key: str = Path(..., description="User api key"),
    flow_id: str = Path(..., description="User flow id"),
    request: Request = None
):
    data = await request.json()

    user = getUser(api_key)
    flow = getFlow(flow_id)

    if not user:
        return {"No user found"} 
    if not flow:
        return {"No flow found"}
    if not validateUser(user, flow):
        return {"Incorrect validation"}
    topic_name = f"input_{flow_id}"
    producer = get_producer(flow_id)
    producer.produce(topic=topic_name, value=data)
    producer.flush()

    return {
        "api_key": api_key,
        "flow_id": flow_id,
        "body": data,
        "status": "sent to kafka"
    }

