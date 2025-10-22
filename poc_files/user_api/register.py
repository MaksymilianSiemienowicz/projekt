from pymongo import MongoClient
import sys
from uuid import uuid4
import bcrypt
from clickhouse_driver import Client

def createUserDb(userid):
    client = Client(host='localhost', user='default', password='password123')
    query = f"""CREATE DATABASE {userid}"""
    client.execute(query)

if __name__ == '__main__':
    
    client = MongoClient('mongodb://localhost:27017')
    db = client['project']

    print("Connected to mongodb...")
    users = db["users"]
    username = sys.argv[1]
    password = sys.argv[2]
    retype = sys.argv[3]
        
    if password != retype:
        print("Passwords doesnt match")
        exit(1)

    password_bytes = password.encode("utf-8")

    salt = bcrypt.gensalt()
    password_bcrypt = bcrypt.hashpw(password_bytes, salt) 
    user = {
            "API_KEY":  str(uuid4()),
            "username":  username,
            "password":  password_bcrypt.decode('utf-8')
          }
    result = users.insert_one(user)
    createUserDb(result.inserted_id)
    print("User inserted with _id:", result.inserted_id)

    


