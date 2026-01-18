import asyncio
import functools
import os
from contextlib import asynccontextmanager
from bson import ObjectId
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
import clickhouse_connect
import re
import logging


MONGO_URL = os.environ.get("MONGO_URL", "mongodb://localhost:27017")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
MONGO_DB = os.environ.get("MONGO_DB", "mydb")
MONGO_COLLECTION = "users"
mongo_client: AsyncIOMotorClient | None = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UserRequest(BaseModel):
    user_id: str

async def init_mongo():
    global mongo_client
    logger.info(">>> CONNECTING TO MONGO: %s", MONGO_URL)
    mongo_client = AsyncIOMotorClient(MONGO_URL)

async def run_in_executor(func, *args):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(func, *args))

def clickhouse_create_user(username: str, password: str):
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

    OBJECT_ID_RE = re.compile(r"^ch_[a-f0-9]{24}$")

    if not OBJECT_ID_RE.fullmatch(username):
        raise ValueError(f"{username} is invalid.")

    
    client.command(f"CREATE USER IF NOT EXISTS {username} ON CLUSTER default IDENTIFIED BY '{password}'")
    client.command(f"CREATE DATABASE IF NOT EXISTS {username}_db ON CLUSTER default")
    client.command(f"GRANT ALL ON {username}_db.* TO {username} ON CLUSTER default")
    client.close()


async def create_clickhouse_user(mongo_user: dict):
    username = f"ch_{mongo_user['_id']}"
    password = "default_pass"

    await run_in_executor(
        clickhouse_create_user,
        username,
        password,
    )

    return {
        "clickhouse_user": username,
        "database": f"{username}_db",
        "password": password,
    }

async def find_user_by_id(user_id: str):
    if not ObjectId.is_valid(user_id):
        logger.error(">>> INVALID ObjectId:", user_id)
        return None

    oid = ObjectId(user_id)

    db = mongo_client[MONGO_DB]
    users = db[MONGO_COLLECTION]

    user = await users.find_one({"_id": oid})
    return user


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_mongo()
    yield
    if mongo_client:
        mongo_client.close()

app = FastAPI(lifespan=lifespan)

@app.post("/users")
async def get_user(data: UserRequest):
    user = await find_user_by_id(data.user_id)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    user["_id"] = str(user["_id"])

    ch_info = await create_clickhouse_user(user)

    return {
        "mongo_user": user,
        "clickhouse_account_created": ch_info,
    }
