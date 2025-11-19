import json
import time
import sys
from datetime import datetime

def convert_to_serverless_sim(input_file, output_file):
    """
    Convert task_batch.json to serverless-sim format.
    
    Mapping:
    - task.deadline → event.deadline
    - task.est_runtime → function timeout
    - task.name, script_path, args → event.payload
    - Priority (deadline, est_runtime) → scheduling_policy: "deadline_fcfs"
    """
    
    with open(input_file, 'r') as f:
        tasks = json.load(f)
    
    config = {
        "functions": [
            {
                "name": "task_executor",
                "memory": 256,
                "timeout": 60,
                "language": "python",
                "handler": "handler.handle"
            }
        ],
        "workload": [],
        "simulation": {
            "scheduling_policy": "deadline_fcfs",
            "container_reuse": False,
            "metrics": ["arrival_time", "queue_time", "execution_time", "deadline_met"]
        }
    }
    
    now = time.time()
    
    for idx, task_def in enumerate(tasks):
        # Parse deadline (same as your scheduler.py)
        if "deadline" in task_def:
            dt = datetime.fromisoformat(task_def["deadline"])
            deadline = dt.timestamp()
        else:
            deadline = now + task_def.get("deadline_offset", 10)
        
        arrival_time = now + (idx * 0.1)  # Stagger arrivals by 100ms
        
        config["workload"].append({
            "id": f"task_{idx}",
            "function_name": "task_executor",
            "arrival_time": arrival_time,
            "deadline": deadline,
            "payload": {
                "name": task_def["name"],
                "script_path": task_def["script_path"],
                "est_runtime": task_def["est_runtime"],
                "args": task_def.get("args", [])
            }
        })
    
    with open(output_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"✓ Converted {len(tasks)} tasks")
    print(f"✓ Output: {output_file}")

if __name__ == "__main__":
    input_file = sys.argv[1] if len(sys.argv) > 1 else "task_batch.json"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "run.json"
    convert_to_serverless_sim(input_file, output_file)

