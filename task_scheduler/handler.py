import json
import subprocess
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from log_util import log_event
    from perf_logger import log_performance
    HAS_LOGGING = True
except ImportError:
    HAS_LOGGING = False

def handle_execute_task(event, context):
    """
    Serverless function handler compatible with serverless-sim.
    Executes a single task and returns metrics.
    """
    
    # Extract task metadata from event payload
    task_payload = event.get('payload', {})
    task_name = task_payload.get('name', 'UnknownTask')
    script_path = task_payload.get('script_path')
    est_runtime = task_payload.get('est_runtime', 0)
    args = task_payload.get('args', [])
    
    # Timing information
    arrival_time = event.get('arrival_time', time.time())
    deadline = event.get('deadline')
    execution_id = context.get('execution_id', f"exec_{int(time.time()*1000)}")
    
    # Compute deadline status AT INVOCATION TIME
    enqueue_time = time.time()
    deadline_missed = (enqueue_time > deadline) if deadline else False
    deadline_status = "missed" if deadline_missed else "on-time"
    
    if HAS_LOGGING:
        log_event(f"[{execution_id}] START {task_name} | Deadline: {deadline} | Status: {deadline_status}")
    
    # Resolve and execute script
    if not os.path.isabs(script_path):
        script_path = os.path.join(os.path.dirname(__file__), script_path)
    
    start_time = time.time()
    try:
        result = subprocess.run(
            ["python3", script_path] + args,
            capture_output=True,
            text=True,
            timeout=int(est_runtime * 2.5)
        )
        output = result.stdout
        error = result.stderr
        success = (result.returncode == 0)
    except subprocess.TimeoutExpired:
        output = ""
        error = "Timeout exceeded"
        success = False
    except Exception as e:
        output = ""
        error = str(e)
        success = False
    
    end_time = time.time()
    
    # Log performance metrics (same format as your current system)
    if HAS_LOGGING:
        log_event(f"[{execution_id}] END {task_name} | Duration: {end_time - start_time:.3f}s")
        log_performance(
            task_name=task_name,
            enqueue_time=enqueue_time,
            start_time=start_time,
            end_time=end_time,
            deadline=deadline,
            deadline_status=deadline_status
        )
    
    return {
        "execution_id": execution_id,
        "task_name": task_name,
        "status": "success" if success else "failed",
        "execution_time": end_time - start_time,
        "deadline_met": not deadline_missed,
        "deadline_status": deadline_status
    }

def handle(event, context):
    """Main entry point for serverless-sim"""
    return handle_execute_task(event, context)

