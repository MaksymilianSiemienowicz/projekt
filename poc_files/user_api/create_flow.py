from pymongo import MongoClient
import sys
import json
from confluent_kafka.admin import AdminClient, NewTopic
import requests

def create_topic(input_or_output, flow_id):
    admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092" 
    })  
    topic = NewTopic(str(input_or_output) +"_"+ str(flow_id), num_partitions=3, replication_factor=1)
    fs = admin_client.create_topics([topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} utworzony!")
        except Exception as e:
            print(f"Błąd przy tworzeniu topicu {topic}: {e}")

def create_schema(schema_str, flow_id):
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    schema = json.loads(schema_str)
    payload = {"schema": json.dumps(schema)}
    resp = requests.post(
    f"{SCHEMA_REGISTRY_URL}/subjects/{flow_id}/versions",
    headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    data=json.dumps(payload),
    )
    if resp.status_code == 200:
        print("✅ Schemat zarejestrowany:", resp.json())
        return(resp.json()["id"])
    else:
        print("❌ Błąd:", resp.status_code, resp.text)
        return(Null)

if __name__ == '__main__':
    if len(sys.argv) < 5:
        print("Usage: python3 pipeline.py <api_key> <import_schema> <convert_function> <libs>")
        sys.exit(1)

    api_key = sys.argv[1]
    import_data_structure = sys.argv[2]  # np. '{"type": "record", "name": "Input", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}]}'
    convert_function = sys.argv[3]       # np. 'def convert(a, b): return a + b'
    libs = sys.argv[4]                   # np. '["pandas", "numpy"]'

    # połączenie z MongoDB
    client = MongoClient('mongodb://localhost:27017')
    db = client['project']
    users = db["users"]
    flows = db["flows"]

    print("Connected to MongoDB...")

    user = users.find_one({"API_KEY": api_key})
    if not user:
        print("User not found")
        sys.exit(1)

    print(f"Found user with id: {user['_id']}")


    # przygotowanie dokumentu do inserta
    flow = {
        "user_id": user["_id"],
        "import_data_structure": json.loads(import_data_structure),
        "convert_function": convert_function,
        "libs": json.loads(libs)
    }
    result = flows.insert_one(flow)
    schema_id = create_schema(import_data_structure,result.inserted_id)

    create_topic("input",result.inserted_id)
    create_topic("output",result.inserted_id)
    
    print(f"Flow inserted with id: {result.inserted_id}")
