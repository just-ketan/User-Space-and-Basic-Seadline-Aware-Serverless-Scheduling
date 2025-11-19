import os
import csv
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Loggings")
os.makedirs(LOG_DIR, exist_ok=True)
PERF_LOG_FILE = os.path.join(LOG_DIR, "performance_log.csv")

def init_perf_log():
    # Initialize CSV with headers if not exists
    if not os.path.exists(PERF_LOG_FILE):
        with open(PERF_LOG_FILE, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Timestamp", "TaskName", "EnqueueTime", "StartTime", "EndTime", "WaitTime", "ExecDuration", "Deadline", "DeadlineStatus"])

def log_performance(task_name, enqueue_time, start_time, end_time, deadline, deadline_status):
    wait_time = start_time - enqueue_time if enqueue_time else None
    exec_duration = end_time - start_time if start_time and end_time else None
    timestamp = datetime.now().isoformat()

    with open(PERF_LOG_FILE, "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, task_name, enqueue_time, start_time, end_time, wait_time, exec_duration, deadline, deadline_status])
