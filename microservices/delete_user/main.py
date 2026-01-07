import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "db2.projekt.home")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )

app = FastAPI()

class DropDBRequest(BaseModel):
    user_id: str

@app.post("/drop_user_db")
def drop_user_db(request: DropDBRequest):
    if not request.user_id.isalnum():
        raise HTTPException(status_code=400, detail="Invalid user_id")
    
    db_name = f"ch_{request.user_id}_db"
    
    try:
        client = get_clickhouse_client()
        client.command(f"DROP DATABASE IF EXISTS {db_name} ON CLUSTER default")
        return {"status": "success", "message": f"Database {db_name} dropped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
