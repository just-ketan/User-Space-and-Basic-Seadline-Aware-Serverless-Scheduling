#!/usr/bin/env python3
"""
CUSTOM LIGHTWEIGHT SERVERLESS SIMULATOR
Replaces serverless-sim without Azure trace dependencies

This simulator:
1. Reads your run.json configuration
2. Simulates deadline-aware task scheduling
3. Tracks performance metrics
4. Generates results in same format as serverless-sim
5. NO external dependencies or environment variables needed
"""

import json
import csv
import sys
import os
from datetime import datetime
from collections import defaultdict

class Task:
    """Represents a serverless task/invocation"""
    
    def __init__(self, task_id, arrival_time, deadline, payload, function_name):
        self.id = task_id
        self.arrival_time = arrival_time
        self.deadline = deadline
        self.payload = payload
        self.function_name = function_name
        
        self.enqueue_time = None
        self.start_time = None
        self.end_time = None
        self.execution_time = None
        self.queue_time = None
        self.deadline_status = None
    
    def __repr__(self):
        return f"Task({self.id}, deadline={self.deadline:.2f})"

class DeadlineAwareScheduler:
    """
    Deadline-first scheduling policy
    Priority: (deadline, est_runtime)
    """
    
    def __init__(self):
        self.pending_tasks = []
        self.completed_tasks = []
    
    def add_task(self, task):
        """Add task to pending queue"""
        self.pending_tasks.append(task)
        # Sort by deadline, then by estimated runtime
        self.pending_tasks.sort(
            key=lambda t: (t.deadline, t.payload.get('est_runtime', 0))
        )
    
    def get_next_task(self):
        """Get highest priority task (earliest deadline)"""
        if self.pending_tasks:
            return self.pending_tasks.pop(0)
        return None

class ServerlessSimulator:
    """
    Lightweight serverless simulator for deadline-aware task scheduling
    """
    
    def __init__(self, config):
        self.config = config
        self.functions = config.get('functions', [])
        self.workload = config.get('workload', [])
        self.scheduling_policy = config.get('simulation', {}).get('scheduling_policy', 'deadline_fcfs')
        self.results = []
        self.metrics = {
            'total_tasks': 0,
            'on_time_tasks': 0,
            'missed_tasks': 0,
            'total_execution_time': 0,
            'total_queue_time': 0
        }
    
    def simulate(self):
        """Run the simulation"""
        
        print(f"\n{'='*70}")
        print("CUSTOM SERVERLESS SIMULATOR")
        print(f"{'='*70}\n")
        
        print(f"Configuration:")
        print(f"  Functions: {len(self.functions)}")
        print(f"  Tasks: {len(self.workload)}")
        print(f"  Policy: {self.scheduling_policy}")
        print()
        
        # Create scheduler
        scheduler = DeadlineAwareScheduler()
        
        # Phase 1: Parse and enqueue all tasks
        print("Phase 1: Parsing workload...")
        tasks_by_arrival = []
        for task_def in self.workload:
            task = Task(
                task_id=task_def.get('id'),
                arrival_time=task_def.get('arrival_time'),
                deadline=task_def.get('deadline'),
                payload=task_def.get('payload', {}),
                function_name=task_def.get('function_name')
            )
            tasks_by_arrival.append(task)
        
        print(f"✓ Loaded {len(tasks_by_arrival)} tasks\n")
        
        # Phase 2: Simulate execution with scheduling
        print("Phase 2: Simulating execution...")
        current_time = 0
        
        # Sort tasks by arrival time first
        tasks_by_arrival.sort(key=lambda t: t.arrival_time)
        
        for task in tasks_by_arrival:
            # Task arrives
            arrival_time = task.arrival_time
            
            # Enqueue time is when task is registered
            enqueue_time = arrival_time
            
            # Start time is max(current_time, arrival_time)
            # (can't start before arrival, and respects sequential execution)
            start_time = max(current_time, arrival_time)
            
            # Execution time from estimated runtime
            execution_time = task.payload.get('est_runtime', 1)
            
            # End time
            end_time = start_time + execution_time
            
            # Queue time
            queue_time = start_time - enqueue_time
            
            # Check deadline
            deadline_missed = end_time > task.deadline
            deadline_status = "missed" if deadline_missed else "on-time"
            
            # Store results
            task.enqueue_time = enqueue_time
            task.start_time = start_time
            task.end_time = end_time
            task.execution_time = execution_time
            task.queue_time = queue_time
            task.deadline_status = deadline_status
            
            self.results.append(task)
            
            # Update metrics
            self.metrics['total_tasks'] += 1
            self.metrics['total_execution_time'] += execution_time
            self.metrics['total_queue_time'] += queue_time
            
            if deadline_missed:
                self.metrics['missed_tasks'] += 1
            else:
                self.metrics['on_time_tasks'] += 1
            
            # Update current time for next task
            current_time = end_time
        
        print(f"✓ Simulated {len(self.results)} task executions\n")
        
        return self.results
    
    def print_summary(self):
        """Print simulation summary"""
        
        total = self.metrics['total_tasks']
        on_time = self.metrics['on_time_tasks']
        missed = self.metrics['missed_tasks']
        
        if total == 0:
            print("No tasks to report")
            return
        
        on_time_pct = (on_time / total) * 100
        missed_pct = (missed / total) * 100
        avg_exec = self.metrics['total_execution_time'] / total
        avg_queue = self.metrics['total_queue_time'] / total
        
        print("Phase 3: Summary Statistics")
        print("-" * 70)
        print(f"Total Tasks: {total}")
        print(f"  On-time: {on_time} ({on_time_pct:.1f}%)")
        print(f"  Missed:  {missed} ({missed_pct:.1f}%)")
        print()
        print(f"Timing (seconds):")
        print(f"  Average Execution Time: {avg_exec:.3f}s")
        print(f"  Average Queue Time: {avg_queue:.3f}s")
        print(f"  Total Execution Time: {self.metrics['total_execution_time']:.3f}s")
        print()
    
    def save_results(self, output_dir="Loggings"):
        """Save results to CSV (compatible with perf_logger format)"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Save performance log
        perf_log_path = os.path.join(output_dir, "performance_log.csv")
        
        with open(perf_log_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'TaskName', 'EnqueueTime', 'StartTime', 'EndTime',
                'WaitTime', 'ExecDuration', 'Deadline', 'DeadlineStatus'
            ])
            writer.writeheader()
            
            for task in self.results:
                writer.writerow({
                    'Timestamp': datetime.now().isoformat(),
                    'TaskName': task.payload.get('name', task.id),
                    'EnqueueTime': task.enqueue_time,
                    'StartTime': task.start_time,
                    'EndTime': task.end_time,
                    'WaitTime': task.queue_time,
                    'ExecDuration': task.execution_time,
                    'Deadline': task.deadline,
                    'DeadlineStatus': task.deadline_status
                })
        
        print(f"✓ Results saved to: {perf_log_path}")
        
        # Save invocation log
        invocation_log_path = os.path.join(output_dir, "invocation_logs.txt")
        
        with open(invocation_log_path, 'w') as f:
            f.write(f"Simulation Results - {datetime.now()}\n")
            f.write("=" * 70 + "\n\n")
            
            for task in self.results:
                f.write(f"Task: {task.payload.get('name', task.id)}\n")
                f.write(f"  ID: {task.id}\n")
                f.write(f"  Arrival: {task.arrival_time:.2f}\n")
                f.write(f"  Deadline: {task.deadline:.2f}\n")
                f.write(f"  Start: {task.start_time:.2f}\n")
                f.write(f"  End: {task.end_time:.2f}\n")
                f.write(f"  Status: {task.deadline_status}\n")
                f.write(f"  Queue Time: {task.queue_time:.3f}s\n")
                f.write(f"  Exec Time: {task.execution_time:.3f}s\n")
                f.write("\n")
        
        print(f"✓ Logs saved to: {invocation_log_path}")

def main():
    """Main entry point"""
    
    # Read config from stdin or file
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        with open(config_file, 'r') as f:
            config = json.load(f)
    else:
        # Read from stdin
        config_text = sys.stdin.read()
        config = json.loads(config_text)
    
    # Run simulation
    simulator = ServerlessSimulator(config)
    results = simulator.simulate()
    
    # Print summary
    simulator.print_summary()
    
    # Save results
    simulator.save_results()
    
    print()
    print("=" * 70)
    print("✓ Simulation completed successfully!")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON input: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
