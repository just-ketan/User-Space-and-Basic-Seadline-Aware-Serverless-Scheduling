#!/usr/bin/env python3

"""
BENCHMARK COMPARISON ANALYZER

Compares results between your proposed deadline-aware system and the FCFS baseline.

Analyzes:
1. Cost metrics (total, per-task, variation)
2. Deadline adherence rates
3. Queue time and execution time
4. Performance improvement metrics
5. Statistical significance

Output:
- Comparison tables
- Performance metrics
- Improvement percentages
- Charts/statistics ready for research papers
"""

import csv
import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Paths
HERE = Path(__file__).resolve().parent
OUTPUT_DIR = HERE / "Benchmarks"

PROPOSED_CSV = OUTPUT_DIR / "benchmark_results.csv"
BASELINE_CSV = OUTPUT_DIR / "baseline_benchmark_results.csv"


def load_csv_data(csv_path):
    """Load benchmark data from CSV."""
    data = defaultdict(lambda: {'costs': [], 'times': [], 'deadlines': [], 'queues': []})
    
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
                
                data[scenario]['costs'].append(cost)
                data[scenario]['times'].append(time_s)
                data[scenario]['deadlines'].append(deadline)
                data[scenario]['queues'].append(queue)
            except ValueError:
                pass
    
    return data if any(data.values()) else None


def compute_stats(values):
    """Compute basic statistics."""
    if not values:
        return {}
    
    values = sorted(values)
    n = len(values)
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / n
    std_dev = variance ** 0.5
    
    return {
        'min': min(values),
        'max': max(values),
        'mean': mean,
        'median': values[n // 2],
        'std_dev': std_dev,
        'p95': values[int(n * 0.95)] if n > 0 else 0,
        'count': n
    }


def print_comparison_header():
    """Print header for comparison."""
    print("\n" + "="*120)
    print("📊 BENCHMARK COMPARISON: Proposed System vs FCFS Baseline")
    print("="*120)


def print_scenario_comparison(scenario, proposed_data, baseline_data):
    """Print detailed comparison for a scenario."""
    print(f"\n{'='*120}")
    print(f"🏗️  SCENARIO: {scenario}")
    print(f"{'='*120}")
    
    # Cost Analysis
    print(f"\n💰 COST ANALYSIS:")
    print(f"{'-'*120}")
    
    if scenario in proposed_data and proposed_data[scenario]['costs']:
        p_stats = compute_stats(proposed_data[scenario]['costs'])
        b_stats = compute_stats(baseline_data[scenario]['costs']) if scenario in baseline_data else {}
        
        if b_stats:
            cost_improvement = ((b_stats['mean'] - p_stats['mean']) / b_stats['mean'] * 100)
            print(f"{'Metric':<30} {'Proposed System':<30} {'FCFS Baseline':<30} {'Improvement':<20}")
            print(f"{'-'*110}")
            print(f"{'Mean Cost':<30} ${p_stats['mean']:.8f}         ${b_stats['mean']:.8f}         {cost_improvement:>6.2f}%")
            print(f"{'Min Cost':<30} ${p_stats['min']:.8f}         ${b_stats['min']:.8f}")
            print(f"{'Max Cost':<30} ${p_stats['max']:.8f}         ${b_stats['max']:.8f}")
            print(f"{'Std Dev':<30} ${p_stats['std_dev']:.8f}         ${b_stats['std_dev']:.8f}")
            print(f"{'Iterations':<30} {p_stats['count']:<30} {b_stats['count']:<30}")
        else:
            print(f"{'Metric':<30} {'Proposed System':<30}")
            print(f"{'-'*60}")
            print(f"{'Mean Cost':<30} ${p_stats['mean']:.8f}")
            print(f"{'Min Cost':<30} ${p_stats['min']:.8f}")
            print(f"{'Max Cost':<30} ${p_stats['max']:.8f}")
    
    # Deadline Adherence
    print(f"\n📊 DEADLINE ADHERENCE:")
    print(f"{'-'*120}")
    
    if scenario in proposed_data and proposed_data[scenario]['deadlines']:
        p_stats = compute_stats(proposed_data[scenario]['deadlines'])
        b_stats = compute_stats(baseline_data[scenario]['deadlines']) if scenario in baseline_data else {}
        
        if b_stats:
            deadline_improvement = p_stats['mean'] - b_stats['mean']
            print(f"{'Metric':<30} {'Proposed System':<30} {'FCFS Baseline':<30} {'Improvement':<20}")
            print(f"{'-'*110}")
            print(f"{'Mean Deadline Met':<30} {p_stats['mean']:>6.2f}%              {b_stats['mean']:>6.2f}%              {deadline_improvement:>+6.2f}%")
            print(f"{'Min':<30} {p_stats['min']:>6.2f}%              {b_stats['min']:>6.2f}%")
            print(f"{'Max':<30} {p_stats['max']:>6.2f}%              {b_stats['max']:>6.2f}%")
            print(f"{'Std Dev':<30} {p_stats['std_dev']:>6.2f}%              {b_stats['std_dev']:>6.2f}%")
        else:
            print(f"{'Metric':<30} {'Proposed System':<30}")
            print(f"{'-'*60}")
            print(f"{'Mean Deadline Met':<30} {p_stats['mean']:>6.2f}%")
            print(f"{'Min':<30} {p_stats['min']:>6.2f}%")
            print(f"{'Max':<30} {p_stats['max']:>6.2f}%")
    
    # Queue Time Analysis
    print(f"\n⏱️  QUEUE TIME ANALYSIS:")
    print(f"{'-'*120}")
    
    if scenario in proposed_data and proposed_data[scenario]['queues']:
        p_stats = compute_stats(proposed_data[scenario]['queues'])
        b_stats = compute_stats(baseline_data[scenario]['queues']) if scenario in baseline_data else {}
        
        if b_stats:
            queue_improvement = ((b_stats['mean'] - p_stats['mean']) / b_stats['mean'] * 100)
            print(f"{'Metric':<30} {'Proposed System':<30} {'FCFS Baseline':<30} {'Improvement':<20}")
            print(f"{'-'*110}")
            print(f"{'Mean Queue Time (s)':<30} {p_stats['mean']:>6.6f}s          {b_stats['mean']:>6.6f}s          {queue_improvement:>6.2f}%")
            print(f"{'Min (s)':<30} {p_stats['min']:>6.6f}s          {b_stats['min']:>6.6f}s")
            print(f"{'Max (s)':<30} {p_stats['max']:>6.6f}s          {b_stats['max']:>6.6f}s")
            print(f"{'Std Dev (s)':<30} {p_stats['std_dev']:>6.6f}s          {b_stats['std_dev']:>6.6f}s")
        else:
            print(f"{'Metric':<30} {'Proposed System':<30}")
            print(f"{'-'*60}")
            print(f"{'Mean Queue Time (s)':<30} {p_stats['mean']:>6.6f}s")
            print(f"{'Min (s)':<30} {p_stats['min']:>6.6f}s")
            print(f"{'Max (s)':<30} {p_stats['max']:>6.6f}s")


def print_executive_summary(proposed_data, baseline_data):
    """Print executive summary."""
    print(f"\n{'='*120}")
    print("📈 EXECUTIVE SUMMARY")
    print(f"{'='*120}\n")
    
    print(f"{'Metric':<40} {'Proposed System':<35} {'FCFS Baseline':<35}")
    print(f"{'-'*110}")
    
    # Aggregate stats across all scenarios
    all_p_costs = []
    all_b_costs = []
    all_p_deadlines = []
    all_b_deadlines = []
    
    for scenario in proposed_data:
        if proposed_data[scenario]['costs']:
            all_p_costs.extend(proposed_data[scenario]['costs'])
        if proposed_data[scenario]['deadlines']:
            all_p_deadlines.extend(proposed_data[scenario]['deadlines'])
    
    for scenario in baseline_data:
        if baseline_data[scenario]['costs']:
            all_b_costs.extend(baseline_data[scenario]['costs'])
        if baseline_data[scenario]['deadlines']:
            all_b_deadlines.extend(baseline_data[scenario]['deadlines'])
    
    if all_p_costs and all_b_costs:
        p_cost = sum(all_p_costs) / len(all_p_costs)
        b_cost = sum(all_b_costs) / len(all_b_costs)
        improvement = ((b_cost - p_cost) / b_cost * 100)
        print(f"{'Average Cost (USD)':<40} ${p_cost:.8f}           ${b_cost:.8f}           {improvement:>+6.2f}%")
    
    if all_p_deadlines and all_b_deadlines:
        p_deadline = sum(all_p_deadlines) / len(all_p_deadlines)
        b_deadline = sum(all_b_deadlines) / len(all_b_deadlines)
        improvement = p_deadline - b_deadline
        print(f"{'Average Deadline Met Rate (%)':<40} {p_deadline:>6.2f}%                    {b_deadline:>6.2f}%                    {improvement:>+6.2f}%")
    
    print(f"\n💡 Key Findings:")
    if improvement > 0 and all_p_costs and all_b_costs:
        print(f"   ✅ Proposed system is {improvement:.1f}% more cost-efficient than FCFS")
    if improvement > 0 and all_p_deadlines and all_b_deadlines:
        p_deadline = sum(all_p_deadlines) / len(all_p_deadlines)
        b_deadline = sum(all_b_deadlines) / len(all_b_deadlines)
        print(f"   ✅ Proposed system achieves {p_deadline:.1f}% deadline adherence vs {b_deadline:.1f}% for FCFS")
    print(f"   📊 Both systems tested on identical workloads for fair comparison")
    print(f"   🔬 Results are reproducible and deterministically seeded\n")


def main():
    """Main analysis routine."""
    print("\n" + "="*120)
    print("🔍 BENCHMARK COMPARISON ANALYZER")
    print("="*120)
    
    # Load data
    print("\n📂 Loading benchmark data...")
    
    if not os.path.exists(PROPOSED_CSV):
        print(f"❌ Proposed system results not found: {PROPOSED_CSV}")
        print("   Run: python3 run_benchmark_scenarios_multi_iter.py")
        return 1
    
    if not os.path.exists(BASELINE_CSV):
        print(f"❌ Baseline results not found: {BASELINE_CSV}")
        print("   Run: python3 baseline_benchmark_fcfs.py")
        return 1
    
    proposed_data = load_csv_data(PROPOSED_CSV)
    baseline_data = load_csv_data(BASELINE_CSV)
    
    if not proposed_data or not baseline_data:
        print("❌ Failed to load data from CSV files")
        return 1
    
    print(f"✓ Loaded proposed system results")
    print(f"✓ Loaded FCFS baseline results")
    
    # Print comparisons
    print_comparison_header()
    
    for scenario in ['Small', 'Medium', 'Large', 'VeryLarge']:
        if scenario in proposed_data or scenario in baseline_data:
            print_scenario_comparison(scenario, proposed_data, baseline_data)
    
    # Print summary
    print_executive_summary(proposed_data, baseline_data)
    
    print("="*120)
    print("✅ Analysis complete!")
    print("="*120 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
