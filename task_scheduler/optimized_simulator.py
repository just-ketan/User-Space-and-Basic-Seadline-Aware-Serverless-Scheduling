#!/usr/bin/env python3
"""
OPTIMIZED SERVERLESS SIMULATOR - Handles Large Azure Workloads Efficiently

Fixes for freezing issues:
1. Batch processing (1000 tasks at a time)
2. Progress reporting with ETA
3. Streaming CSV output (no memory buildup)
4. Workload size validation and limits
5. Memory usage monitoring
6. User can specify max tasks to process
7. Early termination on memory issues
"""

import json
import csv
import sys
import os
import psutil
import time
from datetime import datetime, timedelta
from collections import defaultdict
import concurrent.futures
import random


class ProgressReporter:
    """Reports simulation progress with ETA"""
    
    def __init__(self, total_tasks):
        self.total_tasks = total_tasks
        self.processed = 0
        self.start_time = time.time()
        self.last_report = 0
    
    def update(self, processed_count):
        self.processed = processed_count
        current_time = time.time()
        
        # Report every 1000 tasks or every 5 seconds, whichever comes first
        if (self.processed - self.last_report >= 1000 or 
            current_time - self.last_report >= 5):
            
            elapsed = current_time - self.start_time
            if elapsed > 0:
                rate = self.processed / elapsed
                if rate > 0:
                    eta_seconds = (self.total_tasks - self.processed) / rate
                    eta = timedelta(seconds=int(eta_seconds))
                else:
                    eta = "Unknown"
            else:
                eta = "Calculating..."
            
            percent = (self.processed / self.total_tasks) * 100
            memory_mb = psutil.virtual_memory().used / (1024 * 1024)
            
            print(f"Progress: {self.processed:>8}/{self.total_tasks:<8} "
                  f"({percent:>5.1f}%) | "
                  f"ETA: {str(eta):<12} | "
                  f"Memory: {memory_mb:>6.0f} MB")
            
            self.last_report = current_time
    
    def finish(self):
        elapsed = time.time() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        print(f"✓ Completed {self.processed} tasks in {elapsed:.1f}s "
              f"({rate:.1f} tasks/sec)")


class OptimizedTask:
    """Lightweight task representation"""
    
    def __init__(self, task_data):
        self.id = task_data.get('id')
        self.arrival_time = task_data.get('arrival_time')
        self.deadline = task_data.get('deadline')
        self.payload = task_data.get('payload', {})
        self.function_name = task_data.get('function_name')
        self.metadata = task_data.get('metadata', {})
        self.trigger_type = self.metadata.get('trigger', 'Unknown')
        
        # Computed during simulation
        self.enqueue_time = None
        self.start_time = None
        self.end_time = None
        self.execution_time = None
        self.queue_time = None
        self.deadline_missed = False
        self.deadline_status = None


class StreamingCSVWriter:
    """Streams results to CSV file as they're computed"""
    
    def __init__(self, output_path):
        self.output_path = output_path
        self.file = None
        self.writer = None
        self.rows_written = 0
    
    def __enter__(self):
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self.file = open(self.output_path, 'w', newline='')
        self.writer = csv.DictWriter(self.file, fieldnames=[
            'Timestamp', 'TaskName', 'TaskID', 'TriggerType',
            'EnqueueTime', 'StartTime', 'EndTime',
            'WaitTime', 'ExecDuration', 'Deadline', 'DeadlineStatus'
        ])
        self.writer.writeheader()
        return self
    
    def write_result(self, task):
        """Write single task result immediately"""
        self.writer.writerow({
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
        self.rows_written += 1
        
        # Flush every 100 rows to ensure data is written
        if self.rows_written % 100 == 0:
            self.file.flush()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()


class OptimizedServerlessSimulator:
    """
    Memory-efficient serverless simulator for large Azure workloads
    """
    
    def __init__(self, config, max_tasks=None, concurrency=1,
             cold_start_ms=100, container_reuse=True, reuse_ttl=60,
             enable_cost_model=True):
        self.config = config
        self.functions = config.get('functions', [])
        self.workload = config.get('workload', [])
        self.scheduling_policy = config.get('simulation', {}).get('scheduling_policy', 'deadline_fcfs')
        self.max_tasks = max_tasks

        # --- New parameters ---
        self.concurrency = concurrency
        self.cold_start_ms = cold_start_ms / 1000.0
        self.container_reuse = container_reuse
        self.reuse_ttl = reuse_ttl
        self.enable_cost_model = enable_cost_model

        # Limit workload size if specified
        if self.max_tasks and len(self.workload) > self.max_tasks:
            print(f"⚠️  Limiting workload to {self.max_tasks} tasks (was {len(self.workload)})")
            self.workload = self.workload[:self.max_tasks]

        self.stats = defaultdict(int)
        self.trigger_stats = defaultdict(lambda: {'count': 0, 'missed': 0})

        # --- Container cache ---
        self.container_cache = {}
        self.cost_total = 0.0

    def validate_workload(self):
        """Check workload size and warn about potential issues"""
        
        total_tasks = len(self.workload)
        
        print(f"\n{'='*70}")
        print("WORKLOAD VALIDATION")
        print(f"{'='*70}")
        
        print(f"Total tasks: {total_tasks:,}")
        
        if total_tasks == 0:
            print("❌ ERROR: No tasks in workload")
            return False
        
        # Memory estimation
        estimated_memory_mb = (total_tasks * 1.5) / 1000  # ~1.5KB per task
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        
        print(f"Estimated memory usage: {estimated_memory_mb:.0f} MB")
        print(f"Available memory: {available_memory_mb:.0f} MB")
        
        if total_tasks > 100000:
            print(f"⚠️  WARNING: Very large workload ({total_tasks:,} tasks)")
            print(f"   - Estimated processing time: {total_tasks/10000:.1f} minutes")
            print(f"   - Consider using --max-tasks to limit size")
            
            # Ask for confirmation if running interactively
            if sys.stdin.isatty():
                response = input(f"Continue with {total_tasks:,} tasks? (y/N): ")
                if response.lower() != 'y':
                    print("Aborted by user")
                    return False
        
        elif total_tasks > 50000:
            print(f"⚠️  Large workload ({total_tasks:,} tasks)")
            print(f"   - Estimated processing time: {total_tasks/20000:.1f} minutes")
        
        elif total_tasks > 10000:
            print(f"✓ Moderate workload ({total_tasks:,} tasks)")
        else:
            print(f"✓ Small workload ({total_tasks:,} tasks)")
        
        print()
        return True
    def _get_container_delay(self, function_name, current_time):
        """Return startup delay (cold start vs reused container)."""
        if not self.container_reuse:
            return self.cold_start_ms

        container_info = self.container_cache.get(function_name)
        if not container_info:
            # cold start
            self.container_cache[function_name] = current_time
            return self.cold_start_ms

        last_used = container_info
        if current_time - last_used > self.reuse_ttl:
            # expired container
            self.container_cache[function_name] = current_time
            return self.cold_start_ms

        # warm start (reuse)
        self.container_cache[function_name] = current_time
        return 0.0


    def _compute_cost(self, task):
        """Compute simple cost model (per-invocation pricing)."""
        if not self.enable_cost_model:
            return 0.0

        memory_mb = 256
        gb_seconds = (memory_mb / 1024.0) * task.execution_time
        cost = gb_seconds * 0.00001667  # AWS-like pricing
        return cost + 0.0000002  # invocation overhead

    
    def simulate_batch(self, tasks, current_time, csv_writer):
        """Simulate a batch of tasks efficiently with concurrency & reuse."""
        tasks.sort(key=lambda t: t.arrival_time)

        def execute_task(task):
            nonlocal current_time
            arrival_time = task.arrival_time
            enqueue_time = arrival_time
            start_time = max(current_time, arrival_time)

            # Cold / warm start delay
            cold_delay = self._get_container_delay(task.function_name, start_time)
            start_time += cold_delay

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
            task.deadline_missed = deadline_missed
            task.deadline_status = deadline_status

            # Update cost
            cost = self._compute_cost(task)
            return task, end_time, cost

        # Execute tasks concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [executor.submit(execute_task, t) for t in tasks]
            for future in concurrent.futures.as_completed(futures):
                task, end_time, cost = future.result()
                csv_writer.write_result(task)
                self.stats['total_tasks'] += 1
                if task.deadline_missed:
                    self.stats['missed_tasks'] += 1
                else:
                    self.stats['on_time_tasks'] += 1

                trig = task.trigger_type
                self.trigger_stats[trig]['count'] += 1
                if task.deadline_missed:
                    self.trigger_stats[trig]['missed'] += 1

                self.cost_total += cost
                current_time = max(current_time, end_time)

        return current_time

    
    def simulate(self, batch_size=1000):
        """Run simulation with batch processing"""
        
        # Validate workload first
        if not self.validate_workload():
            return None
        
        total_tasks = len(self.workload)
        
        print(f"SIMULATION CONFIGURATION")
        print("-" * 70)
        print(f"  Functions: {len(self.functions)}")
        print(f"  Tasks: {total_tasks:,}")
        print(f"  Policy: {self.scheduling_policy}")
        print(f"  Batch size: {batch_size:,}")
        print()
        
        # Initialize progress reporting
        progress = ProgressReporter(total_tasks)
        
        # Create output file
        output_path = os.path.join("Loggings", "performance_log.csv")
        
        print(f"Starting simulation...")
        print(f"Results streaming to: {output_path}")
        print()
        
        current_time = 0
        processed_tasks = 0
        
        with StreamingCSVWriter(output_path) as csv_writer:
            # Process workload in batches
            for i in range(0, total_tasks, batch_size):
                # Check memory usage
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > 85:
                    print(f"⚠️  High memory usage ({memory_percent:.1f}%) - consider reducing batch size")
                
                # Get batch
                batch_end = min(i + batch_size, total_tasks)
                batch_data = self.workload[i:batch_end]
                
                # Convert to OptimizedTask objects
                batch_tasks = [OptimizedTask(task_data) for task_data in batch_data]
                
                # Process batch
                current_time = self.simulate_batch(batch_tasks, current_time, csv_writer)
                
                processed_tasks = batch_end
                progress.update(processed_tasks)
                
                # Clear batch from memory
                del batch_tasks
                del batch_data
        
        progress.finish()
        
        return self.stats
    
    def print_summary(self):
        """Print final simulation summary"""
        
        total = self.stats['total_tasks']
        on_time = self.stats['on_time_tasks']
        missed = self.stats['missed_tasks']
        
        if total == 0:
            print("No tasks processed")
            return
        
        print()
        print("="*70)
        print("SIMULATION SUMMARY")
        print("="*70)
        
        print(f"Total Tasks: {total:,}")
        print(f"  On-time: {on_time:,} ({100*on_time/total:.1f}%)")
        print(f"  Missed:  {missed:,} ({100*missed/total:.1f}%)")
        print()
        
        # Trigger type breakdown
        if self.trigger_stats:
            print("Performance by Trigger Type:")
            print("-" * 50)
            for trigger, stats in sorted(self.trigger_stats.items(), 
                                       key=lambda x: -x[1]['count']):
                count = stats['count']
                missed = stats['missed']
                miss_rate = (missed / count * 100) if count > 0 else 0
                print(f"  {trigger:<15} {count:>8,} tasks  "
                      f"Miss rate: {miss_rate:>5.1f}%")
        
        print()
        print("✓ Results saved to: Loggings/performance_log.csv")
        print("="*70)
        if self.enable_cost_model:
            print()
            print(f"💰 Estimated Total Cost: ${self.cost_total:.6f}")
            if self.stats['total_tasks'] > 0:
                print(f"   Avg per task: ${self.cost_total / self.stats['total_tasks']:.8f}")

def main():
    """Main entry point with command-line options"""
        
        # Parse command-line arguments
    # Parse command-line arguments
    max_tasks = None
    batch_size = 1000
    concurrency = 1
    cold_start_ms = 100
    container_reuse = True
    reuse_ttl = 60
    enable_cost_model = True

    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == '--max-tasks' and i + 1 < len(sys.argv):
            max_tasks = int(sys.argv[i + 1])
        elif arg == '--batch-size' and i + 1 < len(sys.argv):
            batch_size = int(sys.argv[i + 1])
        elif arg == '--concurrency' and i + 1 < len(sys.argv):
            concurrency = int(sys.argv[i + 1])
        elif arg == '--cold-start-ms' and i + 1 < len(sys.argv):
            cold_start_ms = float(sys.argv[i + 1])
        elif arg == '--no-container-reuse':
            container_reuse = False
        elif arg == '--reuse-ttl' and i + 1 < len(sys.argv):
            reuse_ttl = float(sys.argv[i + 1])
        elif arg == '--no-cost-model':
            enable_cost_model = False

    # Read config
    try:
        config_text = sys.stdin.read()
        config = json.loads(config_text)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON input: {e}")
        return 1
    except Exception as e:
        print(f"ERROR reading input: {e}")
        return 1
    
    # Run simulation
    try:
        simulator = OptimizedServerlessSimulator(
            config,
            max_tasks=max_tasks,
            concurrency=concurrency,
            cold_start_ms=cold_start_ms,
            container_reuse=container_reuse,
            reuse_ttl=reuse_ttl,
            enable_cost_model=enable_cost_model
        )
        results = simulator.simulate(batch_size=batch_size)
        
        if results:
            simulator.print_summary()
        
        return 0
    
    except KeyboardInterrupt:
        print("\n❌ Simulation interrupted by user")
        return 1
    except MemoryError:
        print("\n❌ Out of memory - try reducing --max-tasks or --batch-size")
        return 1
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())