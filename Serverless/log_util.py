import os
from datetime import datetime

# Always resolve to the project's Loggings folder
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(PROJECT_ROOT, "Loggings")
os.makedirs(LOG_DIR, exist_ok=True)

def log_event(msg):
    filename = os.path.join(LOG_DIR, "invocation_logs.txt")
    with open(filename, "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")

'''if __name__ == "__main__":
    log_event("Manual test log entry.")'''