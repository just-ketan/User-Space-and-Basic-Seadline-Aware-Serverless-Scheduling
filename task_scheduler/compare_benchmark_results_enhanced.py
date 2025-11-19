#!/usr/bin/env python3

"""
BENCHMARK COMPARISON ANALYZER (Enhanced with CSV Export)

Compares results between proposed system and FCFS baseline.

NOW SAVES ALL METRICS TO CSV FOR VISUALIZATION!

Output Files Created:
1. comparison_metrics_by_scenario.csv - Detailed per-scenario metrics
2. comparison_metrics_summary.csv - Overall summary statistics
3. comparison_improvement_metrics.csv - Improvement percentages
4. comparison_detailed_stats.csv - All statistical measures (min, max, mean, median, etc)
"""

import csv
import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

HERE = Path(__file__).resolve().parent
OUTPUT_DIR = HERE / "Benchmarks"

PROPOSED_CSV = OUTPUT_DIR / "benchmark_results.csv"
BASELINE_CSV = OUTPUT_DIR / "baseline_benchmark_results.csv"

# Output CSV files
COMPARISON_BY_SCENARIO = OUTPUT_DIR / "comparison_metrics_by_scenario.csv"
COMPARISON_SUMMARY = OUTPUT_DIR / "comparison_metrics_summary.csv"
COMPARISON_IMPROVEMENT = OUTPUT_DIR / "comparison_improvement_metrics.csv"
COMPARISON_DETAILED = OUTPUT_DIR / "comparison_detailed_stats.csv"


def load_csv_data(csv_path):
    """Load benchmark data from CSV."""
    data = defaultdict(lambda: {'costs': [], 'times': [], 'deadlines': [], 'queues': [], 'exec_times': []})
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        return None
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('notes') != 'SUCCESS':
                continue
            
            scenario = row['scenario']
            try:
                cost = float(row['total_cost'])
                time_s = float(row['wall_time_seconds'])
                deadline = float(row['deadline_met_rate'])
                queue = float(row['queue_time_avg'])
                exec_time = float(row['exec_time_avg'])
                
                data[scenario]['costs'].append(cost)
                data[scenario]['times'].append(time_s)
                data[scenario]['deadlines'].append(deadline)
                data[scenario]['queues'].append(queue)
                data[scenario]['exec_times'].append(exec_time)
            except ValueError:
                pass
    
    return data if any(data.values()) else None


def compute_stats(values):
    """Compute comprehensive statistics."""
    if not values:
        return {}
    
    values_sorted = sorted(values)
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    std_dev = variance ** 0.5
    
    return {
        'min': min(values),
        'max': max(values),
        'mean': mean,
        'median': values_sorted[n // 2],
        'std_dev': std_dev,
        'p25': values_sorted[int(n * 0.25)],
        'p50': values_sorted[int(n * 0.50)],
        'p75': values_sorted[int(n * 0.75)],
        'p95': values_sorted[int(n * 0.95)],
        'p99': values_sorted[int(n * 0.99)] if n > 100 else values_sorted[-1],
        'count': n,
        'range': max(values) - min(values),
        'sum': sum(values),
    }


def export_metrics_by_scenario(proposed_data, baseline_data):
    """Export per-scenario comparison metrics to CSV."""
    
    with open(COMPARISON_BY_SCENARIO, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'scenario',
            'metric_type',
            'proposed_mean', 'proposed_std',
            'baseline_mean', 'baseline_std',
            'improvement_pct', 'improvement_value',
            'proposed_count', 'baseline_count'
        ])
        
        scenarios = ['Small', 'Medium', 'Large', 'VeryLarge']
        
        for scenario in scenarios:
            if scenario not in proposed_data and scenario not in baseline_data:
                continue
            
            p_data = proposed_data.get(scenario, {})
            b_data = baseline_data.get(scenario, {})
            
            # Cost metrics
            p_cost_stats = compute_stats(p_data.get('costs', []))
            b_cost_stats = compute_stats(b_data.get('costs', []))
            
            if p_cost_stats and b_cost_stats:
                improvement = ((b_cost_stats['mean'] - p_cost_stats['mean']) / b_cost_stats['mean'] * 100)
                writer.writerow([
                    scenario, 'cost',
                    p_cost_stats['mean'], p_cost_stats['std_dev'],
                    b_cost_stats['mean'], b_cost_stats['std_dev'],
                    improvement, b_cost_stats['mean'] - p_cost_stats['mean'],
                    p_cost_stats['count'], b_cost_stats['count']
                ])
            
            # Deadline metrics
            p_deadline_stats = compute_stats(p_data.get('deadlines', []))
            b_deadline_stats = compute_stats(b_data.get('deadlines', []))
            
            if p_deadline_stats and b_deadline_stats:
                improvement = p_deadline_stats['mean'] - b_deadline_stats['mean']
                writer.writerow([
                    scenario, 'deadline_met_rate',
                    p_deadline_stats['mean'], p_deadline_stats['std_dev'],
                    b_deadline_stats['mean'], b_deadline_stats['std_dev'],
                    improvement, improvement,  # Same for absolute
                    p_deadline_stats['count'], b_deadline_stats['count']
                ])
            
            # Queue time metrics
            p_queue_stats = compute_stats(p_data.get('queues', []))
            b_queue_stats = compute_stats(b_data.get('queues', []))
            
            if p_queue_stats and b_queue_stats:
                improvement = ((b_queue_stats['mean'] - p_queue_stats['mean']) / b_queue_stats['mean'] * 100)
                writer.writerow([
                    scenario, 'queue_time_avg',
                    p_queue_stats['mean'], p_queue_stats['std_dev'],
                    b_queue_stats['mean'], b_queue_stats['std_dev'],
                    improvement, b_queue_stats['mean'] - p_queue_stats['mean'],
                    p_queue_stats['count'], b_queue_stats['count']
                ])
            
            # Execution time metrics
            p_exec_stats = compute_stats(p_data.get('exec_times', []))
            b_exec_stats = compute_stats(b_data.get('exec_times', []))
            
            if p_exec_stats and b_exec_stats:
                improvement = ((b_exec_stats['mean'] - p_exec_stats['mean']) / b_exec_stats['mean'] * 100)
                writer.writerow([
                    scenario, 'exec_time_avg',
                    p_exec_stats['mean'], p_exec_stats['std_dev'],
                    b_exec_stats['mean'], b_exec_stats['std_dev'],
                    improvement, b_exec_stats['mean'] - p_exec_stats['mean'],
                    p_exec_stats['count'], b_exec_stats['count']
                ])
            
            # Wall time metrics
            p_wall_stats = compute_stats(p_data.get('times', []))
            b_wall_stats = compute_stats(b_data.get('times', []))
            
            if p_wall_stats and b_wall_stats:
                improvement = ((b_wall_stats['mean'] - p_wall_stats['mean']) / b_wall_stats['mean'] * 100)
                writer.writerow([
                    scenario, 'wall_time',
                    p_wall_stats['mean'], p_wall_stats['std_dev'],
                    b_wall_stats['mean'], b_wall_stats['std_dev'],
                    improvement, b_wall_stats['mean'] - p_wall_stats['mean'],
                    p_wall_stats['count'], b_wall_stats['count']
                ])
    
    print(f"✅ Exported: {COMPARISON_BY_SCENARIO}")


def export_summary_metrics(proposed_data, baseline_data):
    """Export overall summary metrics."""
    
    with open(COMPARISON_SUMMARY, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'metric',
            'proposed_value', 'baseline_value',
            'absolute_improvement', 'percent_improvement',
            'unit'
        ])
        
        # Aggregate all data
        all_p_costs = []
        all_b_costs = []
        all_p_deadlines = []
        all_b_deadlines = []
        all_p_queues = []
        all_b_queues = []
        all_p_exec = []
        all_b_exec = []
        
        for scenario in proposed_data:
            all_p_costs.extend(proposed_data[scenario]['costs'])
            all_p_deadlines.extend(proposed_data[scenario]['deadlines'])
            all_p_queues.extend(proposed_data[scenario]['queues'])
            all_p_exec.extend(proposed_data[scenario]['exec_times'])
        
        for scenario in baseline_data:
            all_b_costs.extend(baseline_data[scenario]['costs'])
            all_b_deadlines.extend(baseline_data[scenario]['deadlines'])
            all_b_queues.extend(baseline_data[scenario]['queues'])
            all_b_exec.extend(baseline_data[scenario]['exec_times'])
        
        # Write metrics
        if all_p_costs and all_b_costs:
            p_cost = sum(all_p_costs) / len(all_p_costs)
            b_cost = sum(all_b_costs) / len(all_b_costs)
            abs_improvement = b_cost - p_cost
            pct_improvement = (abs_improvement / b_cost * 100) if b_cost != 0 else 0
            writer.writerow(['Average Cost', p_cost, b_cost, abs_improvement, pct_improvement, 'USD'])
        
        if all_p_deadlines and all_b_deadlines:
            p_deadline = sum(all_p_deadlines) / len(all_p_deadlines)
            b_deadline = sum(all_b_deadlines) / len(all_b_deadlines)
            abs_improvement = p_deadline - b_deadline
            pct_improvement = (abs_improvement / b_deadline * 100) if b_deadline != 0 else 0
            writer.writerow(['Average Deadline Met Rate', p_deadline, b_deadline, abs_improvement, pct_improvement, '%'])
        
        if all_p_queues and all_b_queues:
            p_queue = sum(all_p_queues) / len(all_p_queues)
            b_queue = sum(all_b_queues) / len(all_b_queues)
            abs_improvement = b_queue - p_queue
            pct_improvement = (abs_improvement / b_queue * 100) if b_queue != 0 else 0
            writer.writerow(['Average Queue Time', p_queue, b_queue, abs_improvement, pct_improvement, 'seconds'])
        
        if all_p_exec and all_b_exec:
            p_exec = sum(all_p_exec) / len(all_p_exec)
            b_exec = sum(all_b_exec) / len(all_b_exec)
            abs_improvement = b_exec - p_exec
            pct_improvement = (abs_improvement / b_exec * 100) if b_exec != 0 else 0
            writer.writerow(['Average Execution Time', p_exec, b_exec, abs_improvement, pct_improvement, 'seconds'])
    
    print(f"✅ Exported: {COMPARISON_SUMMARY}")


def export_detailed_statistics(proposed_data, baseline_data):
    """Export detailed statistical measures for all metrics."""
    
    with open(COMPARISON_DETAILED, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        header = ['scenario', 'metric_type', 'system', 'count', 'min', 'p25', 'p50', 'median', 'p75', 'p95', 'p99', 'max', 'mean', 'std_dev', 'range', 'sum']
        writer.writerow(header)
        
        scenarios = ['Small', 'Medium', 'Large', 'VeryLarge']
        
        for scenario in scenarios:
            for system, data in [('proposed', proposed_data), ('baseline', baseline_data)]:
                if scenario not in data:
                    continue
                
                s_data = data[scenario]
                
                for metric_name, values in [
                    ('cost', s_data.get('costs', [])),
                    ('deadline_met_rate', s_data.get('deadlines', [])),
                    ('queue_time', s_data.get('queues', [])),
                    ('exec_time', s_data.get('exec_times', [])),
                ]:
                    if not values:
                        continue
                    
                    stats = compute_stats(values)
                    
                    writer.writerow([
                        scenario,
                        metric_name,
                        system,
                        stats.get('count', 0),
                        f"{stats.get('min', 0):.8f}",
                        f"{stats.get('p25', 0):.8f}",
                        f"{stats.get('p50', 0):.8f}",
                        f"{stats.get('median', 0):.8f}",
                        f"{stats.get('p75', 0):.8f}",
                        f"{stats.get('p95', 0):.8f}",
                        f"{stats.get('p99', 0):.8f}",
                        f"{stats.get('max', 0):.8f}",
                        f"{stats.get('mean', 0):.8f}",
                        f"{stats.get('std_dev', 0):.8f}",
                        f"{stats.get('range', 0):.8f}",
                        f"{stats.get('sum', 0):.8f}",
                    ])
    
    print(f"✅ Exported: {COMPARISON_DETAILED}")


def export_improvement_metrics(proposed_data, baseline_data):
    """Export detailed improvement metrics for visualization."""
    
    with open(COMPARISON_IMPROVEMENT, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow([
            'scenario',
            'metric',
            'baseline_value',
            'proposed_value',
            'absolute_change',
            'percent_improvement',
            'baseline_std',
            'proposed_std',
            'is_improvement'
        ])
        
        scenarios = ['Small', 'Medium', 'Large', 'VeryLarge']
        
        for scenario in scenarios:
            if scenario not in proposed_data and scenario not in baseline_data:
                continue
            
            p_data = proposed_data.get(scenario, {})
            b_data = baseline_data.get(scenario, {})
            
            # Cost
            p_cost = compute_stats(p_data.get('costs', []))
            b_cost = compute_stats(b_data.get('costs', []))
            if p_cost and b_cost:
                abs_change = b_cost['mean'] - p_cost['mean']
                pct_improvement = (abs_change / b_cost['mean'] * 100) if b_cost['mean'] != 0 else 0
                writer.writerow([
                    scenario, 'cost',
                    b_cost['mean'], p_cost['mean'],
                    abs_change, pct_improvement,
                    b_cost['std_dev'], p_cost['std_dev'],
                    'Yes' if pct_improvement > 0 else 'No'
                ])
            
            # Deadline
            p_deadline = compute_stats(p_data.get('deadlines', []))
            b_deadline = compute_stats(b_data.get('deadlines', []))
            if p_deadline and b_deadline:
                abs_change = p_deadline['mean'] - b_deadline['mean']
                pct_improvement = (abs_change / b_deadline['mean'] * 100) if b_deadline['mean'] != 0 else 0
                writer.writerow([
                    scenario, 'deadline_met_rate',
                    b_deadline['mean'], p_deadline['mean'],
                    abs_change, pct_improvement,
                    b_deadline['std_dev'], p_deadline['std_dev'],
                    'Yes' if abs_change > 0 else 'No'
                ])
            
            # Queue time
            p_queue = compute_stats(p_data.get('queues', []))
            b_queue = compute_stats(b_data.get('queues', []))
            if p_queue and b_queue:
                abs_change = b_queue['mean'] - p_queue['mean']
                pct_improvement = (abs_change / b_queue['mean'] * 100) if b_queue['mean'] != 0 else 0
                writer.writerow([
                    scenario, 'queue_time',
                    b_queue['mean'], p_queue['mean'],
                    abs_change, pct_improvement,
                    b_queue['std_dev'], p_queue['std_dev'],
                    'Yes' if pct_improvement > 0 else 'No'
                ])
            
            # Execution time
            p_exec = compute_stats(p_data.get('exec_times', []))
            b_exec = compute_stats(b_data.get('exec_times', []))
            if p_exec and b_exec:
                abs_change = b_exec['mean'] - p_exec['mean']
                pct_improvement = (abs_change / b_exec['mean'] * 100) if b_exec['mean'] != 0 else 0
                writer.writerow([
                    scenario, 'exec_time',
                    b_exec['mean'], p_exec['mean'],
                    abs_change, pct_improvement,
                    b_exec['std_dev'], p_exec['std_dev'],
                    'Yes' if pct_improvement > 0 else 'No'
                ])
    
    print(f"✅ Exported: {COMPARISON_IMPROVEMENT}")


def print_console_summary(proposed_data, baseline_data):
    """Print human-readable summary to console."""
    
    print("\n" + "="*120)
    print("📊 BENCHMARK COMPARISON: Proposed System vs FCFS Baseline")
    print("="*120 + "\n")
    
    # Aggregate stats
    all_p_costs = []
    all_b_costs = []
    all_p_deadlines = []
    all_b_deadlines = []
    
    for scenario in proposed_data:
        all_p_costs.extend(proposed_data[scenario]['costs'])
        all_p_deadlines.extend(proposed_data[scenario]['deadlines'])
    
    for scenario in baseline_data:
        all_b_costs.extend(baseline_data[scenario]['costs'])
        all_b_deadlines.extend(baseline_data[scenario]['deadlines'])
    
    print("📈 OVERALL RESULTS:")
    print("-" * 120)
    
    if all_p_costs and all_b_costs:
        p_cost = sum(all_p_costs) / len(all_p_costs)
        b_cost = sum(all_b_costs) / len(all_b_costs)
        improvement = ((b_cost - p_cost) / b_cost * 100)
        print(f"💰 Average Cost: ${p_cost:.8f} vs ${b_cost:.8f} → {improvement:+.1f}% improvement ✅")
    
    if all_p_deadlines and all_b_deadlines:
        p_deadline = sum(all_p_deadlines) / len(all_p_deadlines)
        b_deadline = sum(all_b_deadlines) / len(all_b_deadlines)
        improvement = p_deadline - b_deadline
        print(f"📊 Deadline Met: {p_deadline:.1f}% vs {b_deadline:.1f}% → {improvement:+.1f}pp improvement ✅")
    
    print("\n✅ Detailed metrics exported to:")
    print(f"   1. {COMPARISON_BY_SCENARIO} - Per-scenario comparisons")
    print(f"   2. {COMPARISON_SUMMARY} - Overall summary statistics")
    print(f"   3. {COMPARISON_IMPROVEMENT} - Improvement percentages")
    print(f"   4. {COMPARISON_DETAILED} - Detailed statistical measures")
    print("\n💡 Use these CSVs with visualization tools (Excel, Tableau, Python, R, etc.)")
    print("="*120 + "\n")


def main():
    """Main analysis routine."""
    
    print("\n" + "="*120)
    print("🔍 BENCHMARK COMPARISON ANALYZER (Enhanced with CSV Export)")
    print("="*120)
    
    # Load data
    print("\n📂 Loading benchmark data...")
    
    if not os.path.exists(PROPOSED_CSV):
        print(f"❌ Proposed system results not found: {PROPOSED_CSV}")
        return 1
    
    if not os.path.exists(BASELINE_CSV):
        print(f"❌ Baseline results not found: {BASELINE_CSV}")
        return 1
    
    proposed_data = load_csv_data(PROPOSED_CSV)
    baseline_data = load_csv_data(BASELINE_CSV)
    
    if not proposed_data or not baseline_data:
        print("❌ Failed to load data from CSV files")
        return 1
    
    print(f"✓ Loaded proposed system results")
    print(f"✓ Loaded FCFS baseline results")
    
    # Export all metrics
    print("\n📊 Exporting comparison metrics...")
    export_metrics_by_scenario(proposed_data, baseline_data)
    export_summary_metrics(proposed_data, baseline_data)
    export_improvement_metrics(proposed_data, baseline_data)
    export_detailed_statistics(proposed_data, baseline_data)
    
    # Print console summary
    print_console_summary(proposed_data, baseline_data)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
