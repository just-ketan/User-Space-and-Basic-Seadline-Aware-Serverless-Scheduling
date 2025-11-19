#!/usr/bin/env python3
"""
QUICK SETUP: Install required dependencies and test the optimized simulator

Run this first to ensure everything works properly.
"""

import subprocess
import sys
import os

def install_dependencies():
    """Install required Python packages"""
    
    print("Installing required dependencies...")
    
    try:
        # Install psutil for memory monitoring
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'psutil'])
        print("✓ psutil installed successfully")
        
        # Test import
        import psutil
        memory = psutil.virtual_memory()
        print(f"✓ Memory monitoring works: {memory.available // (1024**3)} GB available")
        
    except Exception as e:
        print(f"❌ Error installing dependencies: {e}")
        return False
    
    return True

def create_test_workload():
    """Create a small test workload for validation"""
    
    print("\nCreating test workload...")
    
    test_config = {
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
    
    import time
    now = time.time()
    
    # Create 10 simple test tasks
    for i in range(10):
        arrival = now + (i * 0.1)
        deadline = arrival + 30  # 30 second deadline
        
        test_config["workload"].append({
            "id": f"test_task_{i}",
            "function_name": "task_executor",
            "arrival_time": arrival,
            "deadline": deadline,
            "payload": {
                "name": f"TestTask{i}",
                "script_path": "tasks/task_generic_serverless.py",
                "est_runtime": 1 + (i * 0.5),  # 1-5 seconds
                "args": [f"TestTask{i}"]
            },
            "metadata": {
                "app_id": 0,
                "trigger": "HTTP" if i % 2 == 0 else "Queue",
                "memory_mb": 256,
                "created_at": "2025-10-31T13:25:00.000000"
            }
        })
    
    # Save test config
    import json
    with open("test_workload.json", "w") as f:
        json.dump(test_config, f, indent=2)
    
    print("✓ Test workload created: test_workload.json")
    print(f"  - {len(test_config['workload'])} tasks")
    print(f"  - Mix of HTTP and Queue triggers")
    print(f"  - Execution times: 1-5 seconds")
    
    return True

def test_optimized_simulator():
    """Test the optimized simulator with small workload"""
    
    print("\nTesting optimized simulator...")
    
    if not os.path.exists("optimized_simulator.py"):
        print("❌ optimized_simulator.py not found")
        print("   Please ensure the file is in the current directory")
        return False
    
    if not os.path.exists("test_workload.json"):
        print("❌ test_workload.json not found")
        return False
    
    try:
        # Run simulator with test workload
        with open("test_workload.json", "r") as f:
            result = subprocess.run(
                [sys.executable, "optimized_simulator.py"],
                input=f.read(),
                text=True,
                capture_output=True,
                timeout=30
            )
        
        if result.returncode == 0:
            print("✓ Simulator test passed!")
            print("✓ Output files should be in Loggings/")
            
            # Check if output files were created
            if os.path.exists("Loggings/performance_log.csv"):
                print("✓ performance_log.csv created")
                
                # Show first few lines
                with open("Loggings/performance_log.csv", "r") as f:
                    lines = f.readlines()
                    print(f"✓ Contains {len(lines)-1} result rows")
            
            return True
        else:
            print("❌ Simulator test failed:")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return False
    
    except subprocess.TimeoutExpired:
        print("❌ Simulator test timed out")
        return False
    except Exception as e:
        print(f"❌ Error testing simulator: {e}")
        return False

def test_workload_inspector():
    """Test the workload inspector"""
    
    print("\nTesting workload inspector...")
    
    if not os.path.exists("workload_inspector.py"):
        print("❌ workload_inspector.py not found")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, "workload_inspector.py", "test_workload.json"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            print("✓ Workload inspector test passed!")
            return True
        else:
            print("❌ Workload inspector test failed:")
            print("STDERR:", result.stderr)
            return False
    
    except Exception as e:
        print(f"❌ Error testing workload inspector: {e}")
        return False

def main():
    """Main setup and test routine"""
    
    print("="*70)
    print("OPTIMIZED SIMULATOR SETUP & TEST")
    print("="*70 + "\n")
    
    success = True
    
    # Step 1: Install dependencies
    if not install_dependencies():
        success = False
    
    # Step 2: Create test workload
    if success and not create_test_workload():
        success = False
    
    # Step 3: Test optimized simulator
    if success and not test_optimized_simulator():
        success = False
    
    # Step 4: Test workload inspector
    if success and not test_workload_inspector():
        success = False
    
    print("\n" + "="*70)
    if success:
        print("✅ SETUP COMPLETE - All tests passed!")
        print("="*70)
        print("\nNext steps:")
        print("1. Generate Azure workload:")
        print("   python3 azure_workload_generator.py --tasks 1000")
        print()
        print("2. Inspect workload:")
        print("   python3 workload_inspector.py run.json")
        print()
        print("3. Run simulation:")
        print("   python3 optimized_simulator.py < run.json")
        print()
        print("4. For large workloads, limit first:")
        print("   python3 workload_inspector.py run.json --limit 10000 --output small.json")
        print("   python3 optimized_simulator.py < small.json")
    else:
        print("❌ SETUP FAILED - Some tests did not pass")
        print("="*70)
        print("\nPlease check the error messages above and:")
        print("1. Ensure all Python files are in the current directory")
        print("2. Check that Python 3.6+ is installed")
        print("3. Verify pip works for installing packages")
    
    print("="*70 + "\n")
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())