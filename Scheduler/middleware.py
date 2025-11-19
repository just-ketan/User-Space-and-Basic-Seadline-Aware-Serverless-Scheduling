from flask import Flask, request, jsonify
import threading
import time
from scheduler import Scheduler, load_tasks_from_json, Task
import os

app = Flask(__name__)
scheduler = Scheduler()

# Load initial tasks
task_file = os.path.join(os.path.dirname(__file__), "task_batch.json")
initial_tasks = load_tasks_from_json(task_file)
for t in initial_tasks:
    scheduler.add_task(t)

# Run scheduler continuously in a background thread
def scheduler_runner():
    while True:
        if not scheduler.task_queue.is_empty():
            scheduler.run_next()
        else:
            time.sleep(0.5)  # Wait before next check if no tasks queued

threading.Thread(target=scheduler_runner, daemon=True).start()

@app.route("/invoke", methods=["POST"])
def invoke_function():
    data = request.json
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400
    now = time.time()
    deadline = now + data.get("deadline_offset", 10)
    task = Task(
        data["name"],
        data["script_path"],
        deadline,
        data["est_runtime"],
        data.get("args", [])
    )
    scheduler.add_task(task)
    return jsonify({"message": f"Task {task.name} queued successfully."})

@app.route("/status", methods=["GET"])
def status():
    # Return current tasks in queue
    queue_info = [
        {
            "name": t.name,
            "deadline": t.deadline,
            "est_runtime": t.est_runtime
        } for t in scheduler.task_queue.peek_all()
    ]
    return jsonify({"queued_tasks": queue_info})

@app.route("/peek", methods=["GET"])
def peek():
    # This endpoint is now identical to /status, so can remove it or keep as alias
    queue_info = [
        {
            "name": t.name,
            "deadline": t.deadline,
            "est_runtime": t.est_runtime
        } for t in scheduler.task_queue.peek_all()
    ]
    return jsonify({"queued_tasks": queue_info})

if __name__ == "__main__":
    app.run(port=5000)
