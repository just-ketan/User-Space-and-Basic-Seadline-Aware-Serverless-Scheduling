import time
import random
import sys
import os
from log_util import log_event

def run(task_name, min_duration=1, max_duration=5, deadline=None):
    # Convert string to float for timestamp comparison if needed
    if deadline:
        deadline = float(deadline)

    exec_time = random.uniform(float(min_duration), float(max_duration))
    start = time.time()
    log_event(f"{task_name} Started - Start time: {start:.2f} - Est. Duration: {exec_time:.2f} s - Deadline: {deadline}")
    print(f"{task_name} Started at {start:.2f}, will run for {exec_time:.2f}s, Deadline: {deadline}")
    time.sleep(exec_time)
    end = time.time()
    duration = end - start
    log_event(f"{task_name} Ended - End time: {end:.2f} - Actual Duration: {duration:.2f} s")
    print(f"{task_name} Ended at {end:.2f}, Duration: {duration:.2f} s")

if __name__ == "__main__":
    args = sys.argv
    task_name = args[1] if len(args) > 1 else "GenericTask"
    min_dur = args[2] if len(args) > 2 else 1
    max_dur = args[3] if len(args) > 3 else 5
    deadline = args[4] if len(args) > 4 else None
    run(task_name, min_dur, max_dur, deadline)
