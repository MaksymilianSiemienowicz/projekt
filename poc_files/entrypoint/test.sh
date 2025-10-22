import requests
from concurrent.futures import ThreadPoolExecutor

URL = "http://127.0.0.1:8000/f985850e-1e80-44dc-b65a-fad1da4c10f9/68b781165cb2edd5bbf5d440"
BODY = {"a": 5, "b": 10}
HEADERS = {"Content-Type": "application/json"}

# liczba requestów i wątków
TOTAL_REQUESTS = 1000000
MAX_WORKERS = 100  # liczba wątków równoległych

def send_post(_):
    try:
        resp = requests.post(URL, json=BODY, headers=HEADERS, timeout=5)
        print(resp.status_code)
    except Exception as e:
        print("❌ Błąd:", e)

# użycie ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(send_post, range(TOTAL_REQUESTS))

