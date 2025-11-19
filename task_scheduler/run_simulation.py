#!/usr/bin/env python3
import json
import subprocess
import sys
import os

def run_simulation():
    """Execute serverless-sim with your configuration"""
    
    # Generate config if needed
    if not os.path.exists("run.json"):
        print("Generating run.json...")
        from generate_sim_config import convert_to_serverless_sim
        convert_to_serverless_sim("task_batch.json", "run.json")
    
    # Load and display config
    with open("run.json", "r") as f:
        config = json.load(f)
    
    print("\n" + "="*70)
    print("SERVERLESS-SIM SIMULATION")
    print("="*70)
    print(f"Tasks: {len(config['workload'])}")
    print(f"Policy: {config['simulation']['scheduling_policy']}")
    print()
    
    # Run serverless-sim
    try:
        with open("run.json", "r") as f:
            config_json = f.read()
        
        result = subprocess.run(
            ["python3", "serverless-sim/run.py"],
            input=config_json,
            text=True,
            capture_output=True,
            timeout=300
        )
        
        print(result.stdout)
        if result.stderr:
            print("ERRORS:", result.stderr)
        
        print("="*70)
        print("Results in Loggings/performance_log.csv")
        print("="*70)
        
        return 0
    except Exception as e:
        print(f"ERROR: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(run_simulation())

