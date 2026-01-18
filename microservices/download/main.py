import os
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pymongo import MongoClient
from bson import ObjectId
import clickhouse_connect
import csv
import io
import re
from datetime import datetime

app = FastAPI()

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://admin:admin@localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "SBTG_data")
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CH_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "default")

if not MONGO_URI or not CH_HOST:
    raise RuntimeError("MONGO_URI and CLICKHOUSE_HOST environment variables must be set")

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB]

ch_client = clickhouse_connect.get_client(
    host=CH_HOST,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE
)

@app.get("/flows/{flow_id}/data")
def get_flow_data(
    flow_id: str,
    API_KEY: str = Query(...),
    start_date: str | None = None,
    end_date: str | None = None,
):
    user = mongo_db.users.find_one({"API_KEY": API_KEY})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid API_KEY")

    user_id = user["_id"]

    flow = mongo_db.flows.find_one({
        "_id": ObjectId(flow_id),
        "user_id": user_id
    })

    if not flow:
        raise HTTPException(
            status_code=403,
            detail="Flow does not belong to this user"
        )
    
    OBJECT_ID_RE = re.compile(r"^[a-f0-9]{24}$")
    if not OBJECT_ID_RE.fullmatch(str(user_id)):
        raise ValueError(f"{user_id} - user_id is invalid")

    if not OBJECT_ID_RE.fullmatch(str(flow_id)):
        raise ValueError(f"{flow_id} - flow_id is invalid")
    if start_date or end_date: 
        try:
            datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise HTTPException(status_code=403, detail="Invalid dates were provided")

    ch_db = f"ch_{user_id}_db"
    ch_table = f"output_{flow_id}"
    
    where_clauses = []
    if start_date:
        where_clauses.append(f"created_at >= '{start_date}'")
    if end_date:
        where_clauses.append(f"created_at <= '{end_date}'")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        SELECT *
        FROM {ch_db}.{ch_table}
        {where_sql}
        ORDER BY created_at
    """

    try:
        result = ch_client.query(query)
        data = result.result_rows
        column_names = result.column_names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    def csv_generator():
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        writer.writerow(column_names)
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for row in data:
            writer.writerow(row)
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    filename = f"flow_{flow_id}.csv"

    return StreamingResponse(
        csv_generator(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )

