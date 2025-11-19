#!/usr/bin/env python3
"""
QUICK FIX: Run simulations with one simple command

This script handles everything:
1. Generates/validates Azure workload if needed
2. Runs optimized simulator
3. Shows results

No setup needed - just run it!
"""

import json
import sys
import os
import subprocess
from pathlib import Path

def check_files():
    """Check if required files exist"""
    required = {
        'optimized_simulator.py': 'Core simulator',
        'azure_workload_generator.py': 'Workload generator'
    }
    
    missing = []
    for file, desc in required.items():
        if not os.path.exists(file):
            missing.append(f"  ✗ {file} ({desc})")
    
    if missing:
        print("Missing required files:")
        for m in missing:
            print(m)
        print("\nPlease ensure all Python scripts are in the current directory")
        return False
    
    return True

def run_azure_generator(num_tasks=5000, output_file='run.json'):
    """Generate Azure workload"""
    
    print(f"\nGenerating {num_tasks:,} Azure tasks...")
    
    try:
        result = subprocess.run(
            [sys.executable, 'azure_workload_generator.py',
             '--tasks', str(num_tasks),
             '--output', output_file,
             '--verbose'],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print(f"✓ Generated {num_tasks:,} tasks")
            print(f"✓ Saved to: {output_file}")
            return True
        else:
            print(f"❌ Generation failed:")
            print(result.stderr)
            return False
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def run_simulator(workload_file='run.json', max_tasks=None, batch_size=1000):
    """Run optimized simulator"""
    
    # Check file exists
    if not os.path.exists(workload_file):
        print(f"❌ File not found: {workload_file}")
        return False
    
    # Get file size
    try:
        with open(workload_file, 'r') as f:
            config = json.load(f)
        
        num_tasks = len(config.get('workload', []))
        if num_tasks == 0:
            print("❌ No tasks in workload")
            return False
        
        print(f"\nRunning simulation on {num_tasks:,} tasks...")
    
    except Exception as e:
        print(f"❌ Error reading workload: {e}")
        return False
    
    # Build command
    cmd = [sys.executable, 'optimized_simulator.py']
    
    if max_tasks:
        cmd.extend(['--max-tasks', str(max_tasks)])
    
    if batch_size != 1000:
        cmd.extend(['--batch-size', str(batch_size)])
    
    # Run simulator
    try:
        with open(workload_file, 'r') as f:
            result = subprocess.run(
                cmd,
                stdin=f,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
        
        if result.returncode == 0:
            print(result.stdout)
            print("✓ Simulation completed successfully!")
            
            # Check results
            if os.path.exists('Loggings/performance_log.csv'):
                with open('Loggings/performance_log.csv', 'r') as rf:
                    lines = rf.readlines()
                print(f"✓ Results: {len(lines)-1} tasks recorded")
            
            return True
        else:
            print("❌ Simulation failed:")
            print(result.stdout)
            if result.stderr:
                print("STDERR:", result.stderr)
            return False
    
    except subprocess.TimeoutExpired:
        print("❌ Simulation timed out (>10 minutes)")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main entry point"""
    
    print("="*70)
    print("SERVERLESS SIMULATOR - QUICK START")
    print("="*70)
    
    # Check files
    if not check_files():
        return 1
    
    # Check if run.json exists
    has_workload = os.path.exists('run.json')
    
    if has_workload:
        # Check workload size
        try:
            with open('run.json', 'r') as f:
                config = json.load(f)
            num_tasks = len(config.get('workload', []))
            
            print(f"\n✓ Found existing workload: {num_tasks} tasks")
            
            # If very small, offer to regenerate
            if num_tasks < 100:
                print(f"⚠️  Workload is very small ({num_tasks} tasks)")
                print("\nOptions:")
                print("1. Run simulation on existing tasks")
                print("2. Generate new Azure workload (1000 tasks)")
                print("3. Generate new Azure workload (10000 tasks)")
                
                choice = input("\nChoice (1-3, default=1): ").strip() or "1"
                
                if choice == "2":
                    if not run_azure_generator(1000, 'run.json'):
                        return 1
                elif choice == "3":
                    if not run_azure_generator(10000, 'run.json'):
                        return 1
                # choice == "1" - use existing
        
        except Exception as e:
            print(f"❌ Error reading run.json: {e}")
            return 1
    
    else:
        print("\nNo run.json found. Generating Azure workload...")
        if not run_azure_generator(1000, 'run.json'):
            print("Failed to generate workload")
            return 1
    
    # Run simulator
    if not run_simulator('run.json'):
        return 1
    
    print("\n" + "="*70)
    print("✅ SUCCESS!")
    print("="*70)
    print("\nResults saved to:")
    print("  - Loggings/performance_log.csv (metrics)")
    print("  - Loggings/invocation_logs.txt (logs)")
    print("\nNext steps:")
    print("  cat Loggings/performance_log.csv  # View metrics")
    print("  tail -50 Loggings/invocation_logs.txt  # View logs")
    print()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())