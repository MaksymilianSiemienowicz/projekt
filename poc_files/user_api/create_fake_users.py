from faker import Faker
import os 
import subprocess
faker = Faker()

for i in range(10000):
    name = faker.name()
    password = faker.password()
    result = subprocess.run(["/home/maks/inzynierka/venv/bin/python3", "/home/maks/inzynierka/user_api/register.py", name, password, password])
    
