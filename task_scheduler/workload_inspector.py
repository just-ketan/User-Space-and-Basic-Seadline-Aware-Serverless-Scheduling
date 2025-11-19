#!/usr/bin/env python3
"""
FIXED WORKLOAD INSPECTOR - Now always writes output file!

Use this tool to:
1. Check the size of generated Azure workloads
2. Extract a subset of tasks for testing
3. Analyze workload characteristics before simulation
4. Create smaller workloads for quick testing
"""

import json
import sys
import argparse
from collections import defaultdict
from datetime import datetime

def inspect_workload(config_file):
    """Load and inspect workload characteristics"""
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except Exception as e:
        print(f"ERROR reading {config_file}: {e}")
        return None
    
    workload = config.get('workload', [])
    functions = config.get('functions', [])
    simulation = config.get('simulation', {})
    
    return {
        'config': config,
        'workload': workload,
        'functions': functions,
        'simulation': simulation,
        'total_tasks': len(workload)
    }

def print_workload_stats(data, detailed=False):
    """Print workload statistics"""
    
    workload = data['workload']
    total = len(workload)
    
    print(f"\n{'='*70}")
    print("WORKLOAD ANALYSIS")
    print(f"{'='*70}")
    
    print(f"Total tasks: {total:,}")
    print(f"Functions: {len(data['functions'])}")
    print(f"Scheduling policy: {data['simulation'].get('scheduling_policy', 'unknown')}")
    
    if total == 0:
        print("❌ No tasks in workload")
        return
    
    # Analyze triggers
    trigger_counts = defaultdict(int)
    exec_times = []
    deadlines = []
    memory_usage = []
    
    for task in workload:
        # Extract metadata
        metadata = task.get('metadata', {})
        trigger = metadata.get('trigger', 'Unknown')
        trigger_counts[trigger] += 1
        
        # Extract timing info
        payload = task.get('payload', {})
        exec_time = payload.get('est_runtime', 0)
        exec_times.append(exec_time)
        
        # Calculate deadline range
        arrival = task.get('arrival_time', 0)
        deadline = task.get('deadline', 0)
        deadline_range = deadline - arrival
        deadlines.append(deadline_range)
        
        # Memory usage
        memory = metadata.get('memory_mb', 0)
        if memory > 0:
            memory_usage.append(memory)
    
    # Size estimates
    estimated_memory_mb = (total * 2) / 1000  # ~2KB per task
    estimated_time_minutes = total / 10000  # ~10k tasks per minute
    
    print(f"\nPerformance Estimates:")
    print(f"  Memory usage: ~{estimated_memory_mb:.0f} MB")
    print(f"  Processing time: ~{estimated_time_minutes:.1f} minutes")
    
    if total > 100000:
        print(f"  ⚠️  VERY LARGE: Consider limiting to 10k-50k tasks")
    elif total > 50000:
        print(f"  ⚠️  LARGE: May take several minutes")
    elif total > 10000:
        print(f"  ✓ MODERATE: Should complete quickly")
    else:
        print(f"  ✓ SMALL: Very fast")
    
    if detailed:
        print(f"\nTrigger Distribution:")
        for trigger, count in sorted(trigger_counts.items(), key=lambda x: -x[1]):
            pct = (count / total) * 100
            print(f"  {trigger:<15} {count:>8,} tasks ({pct:>5.1f}%)")
        
        if exec_times:
            exec_times_sorted = sorted(exec_times)
            print(f"\nExecution Time Distribution (seconds):")
            print(f"  Min:    {min(exec_times):.3f}s")
            print(f"  P50:    {exec_times_sorted[len(exec_times)//2]:.3f}s")
            print(f"  P95:    {exec_times_sorted[int(len(exec_times)*0.95)]:.3f}s")
            print(f"  Max:    {max(exec_times):.3f}s")
            print(f"  Mean:   {sum(exec_times)/len(exec_times):.3f}s")
        
        if deadlines:
            deadline_mins = [d/60 for d in deadlines]
            deadline_mins_sorted = sorted(deadline_mins)
            print(f"\nDeadline Range Distribution (minutes):")
            print(f"  Min:    {min(deadline_mins):.1f} min")
            print(f"  P50:    {deadline_mins_sorted[len(deadline_mins)//2]:.1f} min")
            print(f"  P95:    {deadline_mins_sorted[int(len(deadline_mins)*0.95)]:.1f} min")
            print(f"  Max:    {max(deadline_mins):.1f} min")
        
        if memory_usage:
            print(f"\nMemory Usage Distribution (MB):")
            memory_sorted = sorted(memory_usage)
            print(f"  Min:    {min(memory_usage):.0f} MB")
            print(f"  P50:    {memory_sorted[len(memory_usage)//2]:.0f} MB")
            print(f"  P95:    {memory_sorted[int(len(memory_usage)*0.95)]:.0f} MB")
            print(f"  Max:    {max(memory_usage):.0f} MB")
    
    print(f"{'='*70}\n")

def limit_workload(data, limit, output_file=None, strategy='first'):
    """Create a limited version of the workload - ALWAYS WRITES FILE IF OUTPUT SPECIFIED"""
    
    workload = data['workload']
    total = len(workload)
    
    # FIXED: Always create output if specified, even if <= limit
    if output_file is None:
        if total <= limit:
            print(f"Workload already has {total} tasks (≤ {limit})")
            return data['config']
        else:
            print(f"Limiting workload from {total:,} to {limit:,} tasks...")
    else:
        # OUTPUT FILE SPECIFIED - ALWAYS WRITE IT
        print(f"Processing workload: {total:,} tasks...")
    
    if total <= limit:
        limited_workload = workload
    else:
        if strategy == 'first':
            limited_workload = workload[:limit]
        elif strategy == 'random':
            import random
            limited_workload = random.sample(workload, limit)
        elif strategy == 'representative':
            # Take proportional samples from each trigger type
            trigger_groups = defaultdict(list)
            for task in workload:
                trigger = task.get('metadata', {}).get('trigger', 'Unknown')
                trigger_groups[trigger].append(task)
            
            limited_workload = []
            for trigger, tasks in trigger_groups.items():
                # Take proportional sample
                proportion = len(tasks) / total
                sample_size = int(limit * proportion)
                sample_size = max(1, min(sample_size, len(tasks)))
                
                limited_workload.extend(tasks[:sample_size])
            
            # If we didn't get enough, fill with remaining tasks
            if len(limited_workload) < limit:
                remaining = limit - len(limited_workload)
                all_remaining = [t for t in workload if t not in limited_workload]
                limited_workload.extend(all_remaining[:remaining])
    
    # Create limited config
    limited_config = data['config'].copy()
    limited_config['workload'] = limited_workload[:limit]
    
    # Update metadata
    limited_config.setdefault('metadata', {})
    limited_config['metadata']['limited_from'] = total
    limited_config['metadata']['limited_to'] = len(limited_config['workload'])
    limited_config['metadata']['limited_at'] = datetime.now().isoformat()
    limited_config['metadata']['strategy'] = strategy
    
    # FIXED: ALWAYS WRITE OUTPUT FILE IF SPECIFIED
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(limited_config, f, indent=2)
        print(f"✓ Workload saved to: {output_file}")
        print(f"  - Tasks: {len(limited_config['workload']):,}")
        print(f"  - Strategy: {strategy}")
        return limited_config
    
    return limited_config

def get_recommendations(total_tasks):
    """Get recommendations based on workload size"""
    
    recommendations = []
    
    if total_tasks > 500000:
        recommendations.extend([
            "❌ CRITICAL: Workload too large for most systems",
            "   → Limit to 10k-50k tasks for testing",
            "   → Use: --limit 10000",
            "   → Consider running on high-memory system"
        ])
    elif total_tasks > 100000:
        recommendations.extend([
            "⚠️  WARNING: Very large workload",
            "   → Limit to 50k tasks for faster testing",
            "   → Use: --limit 50000",
            "   → Monitor memory usage during simulation"
        ])
    elif total_tasks > 50000:
        recommendations.extend([
            "⚠️  Large workload - may take time",
            "   → Consider limiting to 10k for quick tests",
            "   → Use: --limit 10000"
        ])
    elif total_tasks > 10000:
        recommendations.extend([
            "✓ Moderate workload size",
            "   → Should run without issues"
        ])
    else:
        recommendations.extend([
            "✓ Small workload - very fast",
            "   → Perfect for testing"
        ])
    
    return recommendations

def main():
    parser = argparse.ArgumentParser(
        description='Inspect and limit Azure workloads before simulation'
    )
    
    parser.add_argument('config_file', 
                       help='Input configuration file (run.json)')
    parser.add_argument('--limit', type=int,
                       help='Limit workload to N tasks')
    parser.add_argument('--output', type=str,
                       help='Output file for limited workload (ALWAYS WRITES IF SPECIFIED)')
    parser.add_argument('--strategy', choices=['first', 'random', 'representative'],
                       default='first',
                       help='Strategy for limiting (default: first)')
    parser.add_argument('--stats', action='store_true',
                       help='Show detailed statistics')
    
    args = parser.parse_args()
    
    # Load and inspect workload
    data = inspect_workload(args.config_file)
    if not data:
        return 1
    
    # Print statistics
    print_workload_stats(data, detailed=args.stats)
    
    # Show recommendations
    recommendations = get_recommendations(data['total_tasks'])
    if recommendations:
        print("RECOMMENDATIONS:")
        print("-" * 70)
        for rec in recommendations:
            print(rec)
        print()
    
    # Limit workload if requested (FIXED: Always writes if --output specified)
    if args.limit or args.output:
        if args.limit is None:
            args.limit = data['total_tasks']  # Use all tasks
        
        limited_config = limit_workload(
            data, 
            args.limit, 
            args.output,
            args.strategy
        )
        
        # Print stats for limited workload
        limited_data = {
            'workload': limited_config['workload'],
            'functions': limited_config['functions'],
            'simulation': limited_config['simulation'],
            'config': limited_config
        }
        
        print("\nLIMITED WORKLOAD:")
        print_workload_stats(limited_data, detailed=False)
    
    # Show usage examples if no output specified
    if not args.output:
        print("USAGE EXAMPLES:")
        print("-" * 70)
        print(f"# Limit to 1,000 tasks (quick test)")
        print(f'python3 workload_inspector.py {args.config_file} --limit 1000 --output test_run.json')
        print()
        print(f"# Limit to 10,000 tasks (moderate test)")
        print(f'python3 workload_inspector.py {args.config_file} --limit 10000 --output run_10k.json')
        print()
        print(f"# Representative sample (maintains trigger distribution)")
        print(f'python3 workload_inspector.py {args.config_file} --limit 5000 --strategy representative --output run_5k.json')
        print()
        print(f"# Then run simulation:")
        print(f'python3 optimized_simulator.py < test_run.json')
        print()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
