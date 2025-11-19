import os
import sys
import json
from datetime import datetime
import subprocess
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'Serverless'))
from log_util import log_event
from task_metadata import Task
from queue import TaskQueue
from perf_logger import init_perf_log, log_performance


class Scheduler:
    def __init__(self):
        self.task_queue = TaskQueue()

    def add_task(self, task):
        # Store enqueue time for performance logging
        task.enqueue_time = time.time()
        self.task_queue.enqueue(task)

    def pop_next(self):
        return self.task_queue.dequeue()

    def run_next(self):
        next_task = self.pop_next()
        if next_task:
            now = time.time()
            missed = now > next_task.deadline
            status = "missed" if missed else "on-time"
            log_event(f"Task: {next_task.name}, Deadline: {next_task.deadline:.2f}, "
                      f"Started: {now:.2f}, Status: {status}")
            print(f"Running: {next_task}")
            script_path = next_task.script_path
            if not os.path.isabs(script_path):
                project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                script_path = os.path.join(project_root, script_path)

            start_time = time.time()
            subprocess.run(["python", script_path] + next_task.args)
            end_time = time.time()

            log_event(f"Task: {next_task.name} finished.")

            # Log performance with enqueue_time
            enqueue_time = getattr(next_task, 'enqueue_time', None)
            log_performance(next_task.name, enqueue_time, start_time, end_time, next_task.deadline, status)

        else:
            print("No tasks to run.")


def load_tasks_from_json(filename):
    tasks = []
    now = time.time()
    with open(filename, "r") as f:
        data = json.load(f)
        for entry in data:
            if "deadline" in entry:
                dt = datetime.fromisoformat(entry["deadline"])
                deadline = dt.timestamp()
                deadline_str = entry["deadline"]
            else:
                deadline = now + entry["deadline_offset"]
                deadline_str = None
            task = Task(
                entry["name"],
                entry["script_path"],
                deadline,
                entry["est_runtime"],
                entry.get("args", []),
                deadline_str
            )
            tasks.append(task)
    return tasks


if __name__ == "__main__":
    init_perf_log()
    scheduler = Scheduler()
    tasks = load_tasks_from_json(os.path.join(os.path.dirname(__file__), "task_batch.json"))

    for task in tasks:
        scheduler.add_task(task)
    while not scheduler.task_queue.is_empty():
        scheduler.run_next()
