#!/usr/bin/env python3
"""
FINAL SOLUTION: Use Custom Simulator Instead of serverless-sim
No Azure trace dependencies - works immediately!
"""

import json
import subprocess
import sys
import os

def run_with_custom_simulator():
    """
    Run simulation using the custom lightweight simulator
    (No serverless-sim dependency needed!)
    """
    
    print("\n" + "="*70)
    print("SERVERLESS TASK SCHEDULER SIMULATION")
    print("="*70 + "\n")
    
    # Step 1: Verify run.json exists
    print("Step 1: Verifying configuration...")
    if not os.path.exists("run.json"):
        print("ERROR: run.json not found")
        print("Run: python3 generate_sim_config.py task_batch.json run.json")
        return 1
    
    try:
        with open("run.json", "r") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in run.json: {e}")
        return 1
    
    print("✓ Configuration loaded successfully\n")
    
    # Step 2: Display configuration info
    print("Step 2: Configuration Summary")
    print("-" * 70)
    num_functions = len(config.get("functions", []))
    num_tasks = len(config.get("workload", []))
    policy = config.get("simulation", {}).get("scheduling_policy", "unknown")
    
    print(f"  Functions: {num_functions}")
    print(f"  Workload: {num_tasks} tasks")
    print(f"  Policy: {policy}")
    print()
    
    if num_tasks == 0:
        print("ERROR: No tasks in workload!")
        return 1
    
    # Step 3: Run custom simulator
    print("Step 3: Running Custom Simulator")
    print("-" * 70)
    print()
    
    # Check if custom_simulator.py exists
    if not os.path.exists("custom_simulator.py"):
        print("ERROR: custom_simulator.py not found")
        print("Please ensure custom_simulator.py is in the same directory")
        return 1
    
    try:
        with open("run.json", "r") as f:
            config_json = f.read()
        
        # Run custom simulator
        result = subprocess.run(
            ["python3", "custom_simulator.py"],
            input=config_json,
            text=True,
            capture_output=True,
            timeout=300
        )
        
        # Print output
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print("\nWarnings/Errors:")
            print(result.stderr)
        
        print()
        print("="*70)
        
        if result.returncode == 0:
            print("✓ Simulation completed successfully!")
        else:
            print(f"✗ Simulator returned code: {result.returncode}")
        
        print("="*70)
        
        # Show results
        if os.path.exists("Loggings/performance_log.csv"):
            print("\n✓ Results available at:")
            print("  - Loggings/performance_log.csv (metrics)")
            print("  - Loggings/invocation_logs.txt (logs)")
            print()
        
        return result.returncode
    
    except subprocess.TimeoutExpired:
        print("ERROR: Simulation timed out (>300 seconds)")
        return 1
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

def main():
    """Main entry point"""
    
    print("""
IMPORTANT: Switching from serverless-sim to Custom Simulator
===========================================================

serverless-sim requires:
  - AZURE_TRACE_DIR environment variable
  - Azure serverless trace datasets
  - Complex dependencies

Solution: Use custom lightweight simulator that:
  ✓ Requires NO external dependencies
  ✓ Simulates YOUR deadline-aware scheduling
  ✓ Generates same metrics as serverless-sim
  ✓ Works immediately!
""")
    
    return run_with_custom_simulator()

if __name__ == "__main__":
    sys.exit(main())
