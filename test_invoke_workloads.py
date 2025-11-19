import requests
import time
import json

# Middleware API URL
BASE_URL = "http://localhost:5000"

# Wait for middleware to become available
def wait_for_server(timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(f"{BASE_URL}/status")
            if r.status_code == 200:
                print("Middleware is alive.")
                return True
        except requests.exceptions.ConnectionError:
            pass
        print("Waiting for middleware...")
        time.sleep(1)
    print("Timed out waiting for server.")
    return False

# Load tasks from JSON files and post them to /invoke
def invoke_tasks_from_file(json_file):
    with open(json_file, "r") as f:
        tasks = json.load(f)
        for task in tasks:
            print(f"Invoking task: {task['name']}")
            response = requests.post(f"{BASE_URL}/invoke", json=task)
            print(f"Response: {response.status_code} - {response.text}")
            time.sleep(0.2)  # Slight delay between invocations

if __name__ == "__main__":
    if wait_for_server():
        # List your workload JSON files here
        workload_files = [
            "Scheduler/burst_traffic.json",
            "Scheduler/mixed_load.json",
            "Scheduler/deadline_sensitive.json"
        ]

        for wf in workload_files:
            print(f"\n=== Invoking workload: {wf} ===")
            invoke_tasks_from_file(wf)
    else:
        print("Middleware not reachable. Abort testing.")
