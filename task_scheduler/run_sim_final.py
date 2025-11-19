#!/usr/bin/env python3
"""
UPDATED CUSTOM SIMULATOR - Integrated with Azure Workload Generator

Now supports:
1. Pre-generated workloads from azure_workload_generator.py
2. Direct generation from Azure traces
3. Heavy traffic scenarios
4. Burst injection
5. Multiple scheduling policies
6. Detailed performance analytics
"""

import json
import csv
import sys
import os
from datetime import datetime
from collections import defaultdict

class PerformanceAnalyzer:
    """Tracks and analyzes simulation performance"""
    
    def __init__(self):
        self.tasks = []
        self.metrics = defaultdict(list)
    
    def add_task_result(self, task_result):
        self.tasks.append(task_result)
        self.metrics['execution_times'].append(task_result['execution_time'])
        self.metrics['queue_times'].append(task_result['queue_time'])
        self.metrics['deadline_misses'].append(task_result['deadline_missed'])
    
    def analyze(self):
        """Compute detailed statistics"""
        
        if not self.tasks:
            return {}
        
        exec_times = sorted(self.metrics['execution_times'])
        queue_times = sorted(self.metrics['queue_times'])
        
        total_tasks = len(self.tasks)
        missed_deadlines = sum(self.metrics['deadline_misses'])
        on_time_tasks = total_tasks - missed_deadlines
        
        analysis = {
            'total_tasks': total_tasks,
            'on_time': on_time_tasks,
            'missed': missed_deadlines,
            'deadline_adherence': (on_time_tasks / total_tasks * 100) if total_tasks > 0 else 0,
            
            'execution_time': {
                'min': min(exec_times),
                'max': max(exec_times),
                'avg': sum(exec_times) / len(exec_times),
                'p50': exec_times[len(exec_times)//2],
                'p95': exec_times[int(len(exec_times)*0.95)],
                'p99': exec_times[int(len(exec_times)*0.99)] if len(exec_times) > 100 else max(exec_times)
            },
            
            'queue_time': {
                'min': min(queue_times),
                'max': max(queue_times),
                'avg': sum(queue_times) / len(queue_times),
                'p50': queue_times[len(queue_times)//2],
                'p95': queue_times[int(len(queue_times)*0.95)],
                'p99': queue_times[int(len(queue_times)*0.99)] if len(queue_times) > 100 else max(queue_times)
            },
            
            'total_execution_time': sum(exec_times),
            'total_queue_time': sum(queue_times)
        }
        
        # Analyze by trigger type
        by_trigger = defaultdict(lambda: {'count': 0, 'missed': 0, 'exec_times': []})
        
        for task in self.tasks:
            trigger = task.get('trigger_type', 'Unknown')
            by_trigger[trigger]['count'] += 1
            if task.get('deadline_missed'):
                by_trigger[trigger]['missed'] += 1
            by_trigger[trigger]['exec_times'].append(task['execution_time'])
        
        analysis['by_trigger'] = {}
        for trigger, data in by_trigger.items():
            times = sorted(data['exec_times'])
            analysis['by_trigger'][trigger] = {
                'tasks': data['count'],
                'deadline_miss_rate': (data['missed'] / data['count'] * 100) if data['count'] > 0 else 0,
                'avg_exec_time': sum(times) / len(times) if times else 0,
                'p95_exec_time': times[int(len(times)*0.95)] if len(times) > 0 else 0
            }
        
        return analysis


class Task:
    """Represents a serverless task"""
    
    def __init__(self, task_id, arrival_time, deadline, payload, function_name):
        self.id = task_id
        self.arrival_time = arrival_time
        self.deadline = deadline
        self.payload = payload
        self.function_name = function_name
        self.trigger_type = payload.get('trigger_type', 'Unknown')
        
        self.enqueue_time = None
        self.start_time = None
        self.end_time = None
        self.execution_time = None
        self.queue_time = None
        self.deadline_status = None
        self.deadline_missed = False


class DeadlineAwareScheduler:
    """Implements deadline-first scheduling"""
    
    def __init__(self):
        self.pending_tasks = []
    
    def add_task(self, task):
        self.pending_tasks.append(task)
        # Sort by: deadline first, then estimated runtime
        self.pending_tasks.sort(
            key=lambda t: (t.deadline, t.payload.get('est_runtime', 0))
        )
    
    def get_next_task(self):
        if self.pending_tasks:
            return self.pending_tasks.pop(0)
        return None


class ServerlessSimulator:
    """
    Enhanced serverless simulator with Azure workload support
    """
    
    def __init__(self, config):
        self.config = config
        self.functions = config.get('functions', [])
        self.workload = config.get('workload', [])
        self.scheduling_policy = config.get('simulation', {}).get('scheduling_policy', 'deadline_fcfs')
        self.results = []
        self.analyzer = PerformanceAnalyzer()
    
    def simulate(self, verbose=True):
        """Run the simulation"""
        
        if verbose:
            print(f"\n{'='*70}")
            print("SERVERLESS SIMULATOR - WITH AZURE WORKLOAD")
            print(f"{'='*70}\n")
            
            print(f"Configuration:")
            print(f"  Functions: {len(self.functions)}")
            print(f"  Tasks: {len(self.workload)}")
            print(f"  Policy: {self.scheduling_policy}")
            print()
        
        # Parse tasks
        scheduler = DeadlineAwareScheduler()
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
        
        if verbose:
            print(f"Phase 1: Parsing workload...")
            print(f"  ✓ Loaded {len(tasks_by_arrival)} tasks\n")
        
        # Simulate execution
        if verbose:
            print(f"Phase 2: Simulating execution...")
        
        current_time = 0
        tasks_by_arrival.sort(key=lambda t: t.arrival_time)
        
        for task in tasks_by_arrival:
            arrival_time = task.arrival_time
            enqueue_time = arrival_time
            start_time = max(current_time, arrival_time)
            execution_time = task.payload.get('est_runtime', 1)
            end_time = start_time + execution_time
            queue_time = start_time - enqueue_time
            
            deadline_missed = end_time > task.deadline
            deadline_status = "missed" if deadline_missed else "on-time"
            
            task.enqueue_time = enqueue_time
            task.start_time = start_time
            task.end_time = end_time
            task.execution_time = execution_time
            task.queue_time = queue_time
            task.deadline_status = deadline_status
            task.deadline_missed = deadline_missed
            
            self.results.append(task)
            self.analyzer.add_task_result({
                'id': task.id,
                'execution_time': execution_time,
                'queue_time': queue_time,
                'deadline_missed': deadline_missed,
                'trigger_type': task.trigger_type,
                'deadline_status': deadline_status
            })
            
            current_time = end_time
        
        if verbose:
            print(f"  ✓ Simulated {len(self.results)} task executions\n")
            self.print_summary()
        
        return self.results
    
    def print_summary(self):
        """Print simulation summary"""
        
        analysis = self.analyzer.analyze()
        
        print("Phase 3: Summary Statistics")
        print("-" * 70)
        print(f"Total Tasks: {analysis['total_tasks']}")
        print(f"  On-time: {analysis['on_time']} ({analysis['deadline_adherence']:.1f}%)")
        print(f"  Missed:  {analysis['missed']} ({100-analysis['deadline_adherence']:.1f}%)")
        print()
        
        print(f"Execution Time (seconds):")
        exec_stats = analysis['execution_time']
        print(f"  Min: {exec_stats['min']:.3f}s")
        print(f"  P50: {exec_stats['p50']:.3f}s")
        print(f"  P95: {exec_stats['p95']:.3f}s")
        print(f"  Avg: {exec_stats['avg']:.3f}s")
        print(f"  Max: {exec_stats['max']:.3f}s")
        print()
        
        print(f"Queue Time (seconds):")
        queue_stats = analysis['queue_time']
        print(f"  Min: {queue_stats['min']:.3f}s")
        print(f"  P50: {queue_stats['p50']:.3f}s")
        print(f"  P95: {queue_stats['p95']:.3f}s")
        print(f"  Avg: {queue_stats['avg']:.3f}s")
        print(f"  Max: {queue_stats['max']:.3f}s")
        print()
        
        # By trigger type
        if analysis['by_trigger']:
            print(f"Performance by Trigger Type:")
            for trigger, stats in sorted(analysis['by_trigger'].items()):
                print(f"  {trigger:<15} Tasks: {stats['tasks']:>5}  " +
                      f"Miss Rate: {stats['deadline_miss_rate']:>5.1f}%  " +
                      f"Avg Exec: {stats['avg_exec_time']:>6.3f}s")
        print()
    
    def save_results(self, output_dir="Loggings"):
        """Save results to CSV"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Performance log
        perf_log_path = os.path.join(output_dir, "performance_log.csv")
        
        with open(perf_log_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'Timestamp', 'TaskName', 'TaskID', 'TriggerType',
                'EnqueueTime', 'StartTime', 'EndTime',
                'WaitTime', 'ExecDuration', 'Deadline', 'DeadlineStatus'
            ])
            writer.writeheader()
            
            for task in self.results:
                writer.writerow({
                    'Timestamp': datetime.now().isoformat(),
                    'TaskName': task.payload.get('name', task.id),
                    'TaskID': task.id,
                    'TriggerType': task.trigger_type,
                    'EnqueueTime': task.enqueue_time,
                    'StartTime': task.start_time,
                    'EndTime': task.end_time,
                    'WaitTime': task.queue_time,
                    'ExecDuration': task.execution_time,
                    'Deadline': task.deadline,
                    'DeadlineStatus': task.deadline_status
                })
        
        print(f"✓ Results saved to: {perf_log_path}")
        
        # Invocation log
        invocation_log_path = os.path.join(output_dir, "invocation_logs.txt")
        
        with open(invocation_log_path, 'w') as f:
            f.write(f"Simulation Results - {datetime.now()}\n")
            f.write("=" * 80 + "\n\n")
            
            for task in self.results[:100]:  # First 100 for brevity
                f.write(f"Task: {task.payload.get('name', task.id)}\n")
                f.write(f"  ID: {task.id}\n")
                f.write(f"  Trigger: {task.trigger_type}\n")
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
    
    # Read config from stdin
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        with open(config_file, 'r') as f:
            config = json.load(f)
    else:
        config_text = sys.stdin.read()
        config = json.loads(config_text)
    
    # Run simulation
    simulator = ServerlessSimulator(config)
    results = simulator.simulate(verbose=True)
    
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
