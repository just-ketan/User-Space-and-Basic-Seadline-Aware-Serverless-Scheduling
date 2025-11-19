#!/usr/bin/env python3

"""
BENCHMARK VISUALIZATION SUITE

Generates comprehensive visualizations from all comparison CSV files.

Creates organized output:
    Visualizations/
    ├── comparison_metrics_by_scenario/
    │   ├── cost_comparison.png
    │   ├── deadline_comparison.png
    │   ├── queue_time_comparison.png
    │   ├── exec_time_comparison.png
    │   └── all_metrics_summary.png
    ├── comparison_metrics_summary/
    │   ├── overall_cost.png
    │   ├── overall_deadline.png
    │   ├── overall_improvements.png
    │   └── summary_dashboard.png
    ├── comparison_improvement_metrics/
    │   ├── improvement_heatmap.png
    │   ├── improvement_percentages.png
    │   ├── improvements_by_scenario.png
    │   └── improvement_confidence.png
    └── comparison_detailed_stats/
        ├── statistical_distributions.png
        ├── percentile_analysis.png
        ├── variance_comparison.png
        └── statistical_summary.png

Run from task_scheduler folder:
    python3 visualize_benchmarks.py
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import warnings

warnings.filterwarnings('ignore')

# Setup paths
HERE = Path(__file__).resolve().parent
BENCHMARKS_DIR = HERE / "Benchmarks"
VIZ_DIR = HERE / "Visualizations"

# Comparison CSV files
COMPARISON_BY_SCENARIO = BENCHMARKS_DIR / "comparison_metrics_by_scenario.csv"
COMPARISON_SUMMARY = BENCHMARKS_DIR / "comparison_metrics_summary.csv"
COMPARISON_IMPROVEMENT = BENCHMARKS_DIR / "comparison_improvement_metrics.csv"
COMPARISON_DETAILED = BENCHMARKS_DIR / "comparison_detailed_stats.csv"

# Output subdirectories
VIZ_BY_SCENARIO = VIZ_DIR / "comparison_metrics_by_scenario"
VIZ_SUMMARY = VIZ_DIR / "comparison_metrics_summary"
VIZ_IMPROVEMENT = VIZ_DIR / "comparison_improvement_metrics"
VIZ_DETAILED = VIZ_DIR / "comparison_detailed_stats"

# Create directories
for d in [VIZ_DIR, VIZ_BY_SCENARIO, VIZ_SUMMARY, VIZ_IMPROVEMENT, VIZ_DETAILED]:
    d.mkdir(parents=True, exist_ok=True)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10
colors_proposed = '#2E86AB'  # Blue
colors_baseline = '#A23B72'  # Purple


# ============================================================================
# 1. COMPARISON BY SCENARIO VISUALIZATIONS
# ============================================================================

def visualize_by_scenario():
    """Create visualizations from comparison_metrics_by_scenario.csv"""
    
    if not COMPARISON_BY_SCENARIO.exists():
        print(f"⚠️  Missing: {COMPARISON_BY_SCENARIO}")
        return
    
    df = pd.read_csv(COMPARISON_BY_SCENARIO)
    print(f"📊 Creating per-scenario visualizations...")
    
    # 1. Cost Comparison
    fig, ax = plt.subplots(figsize=(14, 7))
    cost_data = df[df['metric_type'] == 'cost']
    
    x = np.arange(len(cost_data))
    width = 0.35
    
    ax.bar(x - width/2, cost_data['proposed_mean'], width, label='Proposed System', color=colors_proposed, alpha=0.8)
    ax.bar(x + width/2, cost_data['baseline_mean'], width, label='FCFS Baseline', color=colors_baseline, alpha=0.8)
    
    ax.set_xlabel('Scenario', fontsize=12, fontweight='bold')
    ax.set_ylabel('Cost (USD)', fontsize=12, fontweight='bold')
    ax.set_title('Cost Comparison: Proposed System vs FCFS Baseline', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(cost_data['scenario'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add improvement % on top
    for i, (p, b, imp) in enumerate(zip(cost_data['proposed_mean'], cost_data['baseline_mean'], cost_data['improvement_pct'])):
        if imp > 0:
            ax.text(i, max(p, b) * 1.05, f'+{imp:.1f}%', ha='center', fontweight='bold', color='green')
    
    plt.tight_layout()
    plt.savefig(VIZ_BY_SCENARIO / "cost_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ cost_comparison.png")
    
    # 2. Deadline Comparison
    fig, ax = plt.subplots(figsize=(14, 7))
    deadline_data = df[df['metric_type'] == 'deadline_met_rate']
    
    x = np.arange(len(deadline_data))
    ax.bar(x - width/2, deadline_data['proposed_mean'], width, label='Proposed System', color=colors_proposed, alpha=0.8)
    ax.bar(x + width/2, deadline_data['baseline_mean'], width, label='FCFS Baseline', color=colors_baseline, alpha=0.8)
    
    ax.set_xlabel('Scenario', fontsize=12, fontweight='bold')
    ax.set_ylabel('Deadline Met Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title('Deadline Adherence: Proposed System vs FCFS Baseline', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(deadline_data['scenario'])
    ax.set_ylim([0, 110])
    ax.axhline(y=100, color='red', linestyle='--', alpha=0.3, label='100% Target')
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add improvement pp on top
    for i, (p, b, imp) in enumerate(zip(deadline_data['proposed_mean'], deadline_data['baseline_mean'], deadline_data['improvement_pct'])):
        if imp > 0:
            ax.text(i, max(p, b) * 1.02, f'+{imp:.1f}pp', ha='center', fontweight='bold', color='green')
    
    plt.tight_layout()
    plt.savefig(VIZ_BY_SCENARIO / "deadline_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ deadline_comparison.png")
    
    # 3. Queue Time Comparison
    fig, ax = plt.subplots(figsize=(14, 7))
    queue_data = df[df['metric_type'] == 'queue_time_avg']
    
    x = np.arange(len(queue_data))
    ax.bar(x - width/2, queue_data['proposed_mean'], width, label='Proposed System', color=colors_proposed, alpha=0.8)
    ax.bar(x + width/2, queue_data['baseline_mean'], width, label='FCFS Baseline', color=colors_baseline, alpha=0.8)
    
    ax.set_xlabel('Scenario', fontsize=12, fontweight='bold')
    ax.set_ylabel('Queue Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title('Average Queue Time: Proposed System vs FCFS Baseline', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(queue_data['scenario'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    # Add improvement % on top
    for i, (p, b, imp) in enumerate(zip(queue_data['proposed_mean'], queue_data['baseline_mean'], queue_data['improvement_pct'])):
        if imp > 0:
            ax.text(i, max(p, b) * 1.05, f'+{imp:.1f}%', ha='center', fontweight='bold', color='green')
    
    plt.tight_layout()
    plt.savefig(VIZ_BY_SCENARIO / "queue_time_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ queue_time_comparison.png")
    
    # 4. Execution Time Comparison
    fig, ax = plt.subplots(figsize=(14, 7))
    exec_data = df[df['metric_type'] == 'exec_time_avg']
    
    x = np.arange(len(exec_data))
    ax.bar(x - width/2, exec_data['proposed_mean'], width, label='Proposed System', color=colors_proposed, alpha=0.8)
    ax.bar(x + width/2, exec_data['baseline_mean'], width, label='FCFS Baseline', color=colors_baseline, alpha=0.8)
    
    ax.set_xlabel('Scenario', fontsize=12, fontweight='bold')
    ax.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title('Average Execution Time: Proposed System vs FCFS Baseline', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(exec_data['scenario'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(VIZ_BY_SCENARIO / "exec_time_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ exec_time_comparison.png")
    
    # 5. All Metrics Summary (Multi-panel)
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    metrics = ['cost', 'deadline_met_rate', 'queue_time_avg', 'exec_time_avg']
    ylabels = ['Cost (USD)', 'Deadline Met Rate (%)', 'Queue Time (s)', 'Execution Time (s)']
    
    for idx, (ax, metric, ylabel) in enumerate(zip(axes.flat, metrics, ylabels)):
        metric_df = df[df['metric_type'] == metric]
        x = np.arange(len(metric_df))
        
        ax.bar(x - width/2, metric_df['proposed_mean'], width, label='Proposed', color=colors_proposed, alpha=0.8)
        ax.bar(x + width/2, metric_df['baseline_mean'], width, label='Baseline', color=colors_baseline, alpha=0.8)
        
        ax.set_xlabel('Scenario', fontweight='bold')
        ax.set_ylabel(ylabel, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(metric_df['scenario'])
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
    
    fig.suptitle('All Metrics Comparison: Proposed System vs FCFS Baseline', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(VIZ_BY_SCENARIO / "all_metrics_summary.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ all_metrics_summary.png")


# ============================================================================
# 2. SUMMARY VISUALIZATIONS
# ============================================================================

def visualize_summary():
    """Create visualizations from comparison_metrics_summary.csv"""
    
    if not COMPARISON_SUMMARY.exists():
        print(f"⚠️  Missing: {COMPARISON_SUMMARY}")
        return
    
    df = pd.read_csv(COMPARISON_SUMMARY)
    print(f"📊 Creating summary visualizations...")
    
    # 1. Overall Cost
    fig, ax = plt.subplots(figsize=(10, 7))
    cost_row = df[df['metric'] == 'Average Cost'].iloc[0]
    
    systems = ['Proposed System', 'FCFS Baseline']
    costs = [cost_row['proposed_value'], cost_row['baseline_value']]
    colors = [colors_proposed, colors_baseline]
    
    bars = ax.bar(systems, costs, color=colors, alpha=0.8, width=0.5)
    ax.set_ylabel('Average Cost (USD)', fontsize=12, fontweight='bold')
    ax.set_title('Overall Average Cost Comparison', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels
    for bar, cost in zip(bars, costs):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'${cost:.8f}',
                ha='center', va='bottom', fontweight='bold')
    
    # Add improvement
    improvement = cost_row['percent_improvement']
    ax.text(0.5, max(costs) * 0.5, f'Improvement:\n+{improvement:.1f}%',
            ha='center', fontsize=14, fontweight='bold', 
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
    
    plt.tight_layout()
    plt.savefig(VIZ_SUMMARY / "overall_cost.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ overall_cost.png")
    
    # 2. Overall Deadline
    fig, ax = plt.subplots(figsize=(10, 7))
    deadline_row = df[df['metric'] == 'Average Deadline Met Rate'].iloc[0]
    
    deadlines = [deadline_row['proposed_value'], deadline_row['baseline_value']]
    
    bars = ax.bar(systems, deadlines, color=colors, alpha=0.8, width=0.5)
    ax.set_ylabel('Deadline Met Rate (%)', fontsize=12, fontweight='bold')
    ax.set_title('Overall Average Deadline Adherence', fontsize=14, fontweight='bold')
    ax.set_ylim([0, 110])
    ax.axhline(y=100, color='red', linestyle='--', alpha=0.3, label='100% Target')
    ax.grid(axis='y', alpha=0.3)
    ax.legend()
    
    # Add value labels
    for bar, deadline in zip(bars, deadlines):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{deadline:.1f}%',
                ha='center', va='bottom', fontweight='bold')
    
    # Add improvement
    improvement = deadline_row['percent_improvement']
    ax.text(0.5, 50, f'Improvement:\n+{improvement:.1f}pp',
            ha='center', fontsize=14, fontweight='bold',
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.7))
    
    plt.tight_layout()
    plt.savefig(VIZ_SUMMARY / "overall_deadline.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ overall_deadline.png")
    
    # 3. Overall Improvements
    fig, ax = plt.subplots(figsize=(12, 7))
    
    metrics = df['metric'].values
    improvements = df['percent_improvement'].values
    metric_labels = [m.replace('Average ', '') for m in metrics]
    
    colors_imp = ['green' if x > 0 else 'red' for x in improvements]
    
    bars = ax.barh(metric_labels, improvements, color=colors_imp, alpha=0.7)
    ax.set_xlabel('Improvement (%)', fontsize=12, fontweight='bold')
    ax.set_title('Overall System Improvements vs FCFS Baseline', fontsize=14, fontweight='bold')
    ax.axvline(x=0, color='black', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels
    for bar, imp in zip(bars, improvements):
        width = bar.get_width()
        ax.text(width, bar.get_y() + bar.get_height()/2.,
                f'{imp:+.1f}%',
                ha='left' if width > 0 else 'right', va='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(VIZ_SUMMARY / "overall_improvements.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ overall_improvements.png")
    
    # 4. Summary Dashboard
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Cost
    ax = axes[0, 0]
    ax.bar(systems, [cost_row['proposed_value'], cost_row['baseline_value']], color=colors, alpha=0.8)
    ax.set_ylabel('Cost (USD)', fontweight='bold')
    ax.set_title(f'Cost: +{cost_row["percent_improvement"]:.1f}% improvement', fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    # Deadline
    ax = axes[0, 1]
    ax.bar(systems, deadlines, color=colors, alpha=0.8)
    ax.set_ylabel('Deadline Met Rate (%)', fontweight='bold')
    ax.set_title(f'Deadline: +{deadline_row["percent_improvement"]:.1f}% improvement', fontweight='bold')
    ax.set_ylim([0, 110])
    ax.grid(axis='y', alpha=0.3)
    
    # Queue Time
    ax = axes[1, 0]
    queue_row = df[df['metric'] == 'Average Queue Time'].iloc[0]
    ax.bar(systems, [queue_row['proposed_value'], queue_row['baseline_value']], color=colors, alpha=0.8)
    ax.set_ylabel('Queue Time (s)', fontweight='bold')
    ax.set_title(f'Queue Time: +{queue_row["percent_improvement"]:.1f}% improvement', fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    # Execution Time
    ax = axes[1, 1]
    exec_row = df[df['metric'] == 'Average Execution Time'].iloc[0]
    ax.bar(systems, [exec_row['proposed_value'], exec_row['baseline_value']], color=colors, alpha=0.8)
    ax.set_ylabel('Exec Time (s)', fontweight='bold')
    ax.set_title(f'Execution Time: {exec_row["percent_improvement"]:+.1f}%', fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    fig.suptitle('Benchmark Summary Dashboard', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(VIZ_SUMMARY / "summary_dashboard.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ summary_dashboard.png")


# ============================================================================
# 3. IMPROVEMENT VISUALIZATIONS
# ============================================================================

def visualize_improvements():
    """Create visualizations from comparison_improvement_metrics.csv"""
    
    if not COMPARISON_IMPROVEMENT.exists():
        print(f"⚠️  Missing: {COMPARISON_IMPROVEMENT}")
        return
    
    df = pd.read_csv(COMPARISON_IMPROVEMENT)
    print(f"📊 Creating improvement visualizations...")
    
    # 1. Improvement Heatmap
    fig, ax = plt.subplots(figsize=(12, 8))
    
    pivot_data = df.pivot(index='scenario', columns='metric', values='percent_improvement')
    
    im = ax.imshow(pivot_data.values, cmap='RdYlGn', aspect='auto', vmin=0, vmax=100)
    
    ax.set_xticks(np.arange(len(pivot_data.columns)))
    ax.set_yticks(np.arange(len(pivot_data.index)))
    ax.set_xticklabels(pivot_data.columns)
    ax.set_yticklabels(pivot_data.index)
    
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
    
    # Add text annotations
    for i in range(len(pivot_data.index)):
        for j in range(len(pivot_data.columns)):
            value = pivot_data.values[i, j]
            if not np.isnan(value):
                ax.text(j, i, f'{value:.1f}%', ha="center", va="center", color="black", fontweight='bold')
    
    ax.set_title('Improvement Heatmap: % Improvement by Scenario & Metric', fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Metric', fontweight='bold')
    ax.set_ylabel('Scenario', fontweight='bold')
    
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Improvement (%)', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(VIZ_IMPROVEMENT / "improvement_heatmap.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ improvement_heatmap.png")
    
    # 2. Improvement Percentages (by metric)
    fig, ax = plt.subplots(figsize=(14, 8))
    
    metrics = df['metric'].unique()
    scenarios = df['scenario'].unique()
    
    x = np.arange(len(metrics))
    width = 0.2
    
    for i, scenario in enumerate(scenarios):
        scenario_data = df[df['scenario'] == scenario].sort_values('metric')
        improvements = scenario_data['percent_improvement'].values
        ax.bar(x + (i - len(scenarios)/2) * width, improvements, width, label=scenario, alpha=0.8)
    
    ax.set_xlabel('Metric', fontsize=12, fontweight='bold')
    ax.set_ylabel('Improvement (%)', fontsize=12, fontweight='bold')
    ax.set_title('Improvement Percentages by Metric & Scenario', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, rotation=45, ha='right')
    ax.legend(fontsize=10)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(VIZ_IMPROVEMENT / "improvement_percentages.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ improvement_percentages.png")
    
    # 3. Improvements by Scenario
    fig, ax = plt.subplots(figsize=(14, 8))
    
    for metric in metrics:
        metric_data = df[df['metric'] == metric].sort_values('scenario')
        ax.plot(metric_data['scenario'], metric_data['percent_improvement'], 
               marker='o', linewidth=2.5, markersize=8, label=metric)
    
    ax.set_xlabel('Scenario', fontsize=12, fontweight='bold')
    ax.set_ylabel('Improvement (%)', fontsize=12, fontweight='bold')
    ax.set_title('Improvement Trends Across Scenarios', fontsize=14, fontweight='bold')
    ax.legend(fontsize=11, loc='best')
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color='black', linestyle='--', linewidth=0.8)
    
    plt.tight_layout()
    plt.savefig(VIZ_IMPROVEMENT / "improvements_by_scenario.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ improvements_by_scenario.png")
    
    # 4. Improvement with Confidence (std deviation)
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = np.arange(len(df))
    improvements = df['percent_improvement'].values
    uncertainty = (df['baseline_std'] + df['proposed_std']).values
    
    scenarios_metrics = [f"{s}-{m}" for s, m in zip(df['scenario'], df['metric'])]
    
    colors_bar = ['green' if imp > 0 else 'red' for imp in improvements]
    
    ax.bar(x, improvements, yerr=uncertainty, capsize=5, color=colors_bar, alpha=0.7, error_kw={'linewidth': 2})
    
    ax.set_xlabel('Scenario-Metric', fontsize=12, fontweight='bold')
    ax.set_ylabel('Improvement (%) with Uncertainty', fontsize=12, fontweight='bold')
    ax.set_title('Improvement with Statistical Confidence Bounds', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios_metrics, rotation=45, ha='right', fontsize=9)
    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(VIZ_IMPROVEMENT / "improvement_confidence.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ improvement_confidence.png")


# ============================================================================
# 4. DETAILED STATISTICS VISUALIZATIONS
# ============================================================================

def visualize_detailed():
    """Create visualizations from comparison_detailed_stats.csv"""
    
    if not COMPARISON_DETAILED.exists():
        print(f"⚠️  Missing: {COMPARISON_DETAILED}")
        return
    
    df = pd.read_csv(COMPARISON_DETAILED)
    print(f"📊 Creating detailed statistics visualizations...")
    
    # 1. Statistical Distributions (Box Plots)
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    metrics_to_plot = ['cost', 'deadline_met_rate', 'queue_time', 'exec_time']
    titles = ['Cost Distribution', 'Deadline Met Rate Distribution', 
              'Queue Time Distribution', 'Execution Time Distribution']
    
    for ax, metric, title in zip(axes.flat, metrics_to_plot, titles):
        metric_df = df[df['metric_type'] == metric]
        
        proposed_data = metric_df[metric_df['system'] == 'proposed']
        baseline_data = metric_df[metric_df['system'] == 'baseline']
        
        box_data = [proposed_data['mean'].values, baseline_data['mean'].values]
        bp = ax.boxplot(box_data, labels=['Proposed', 'Baseline'], patch_artist=True)
        
        for patch, color in zip(bp['boxes'], [colors_proposed, colors_baseline]):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        
        ax.set_title(title, fontweight='bold', fontsize=12)
        ax.grid(axis='y', alpha=0.3)
    
    fig.suptitle('Statistical Distributions: Proposed vs Baseline', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(VIZ_DETAILED / "statistical_distributions.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ statistical_distributions.png")
    
    # 2. Percentile Analysis
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    for ax, metric, title in zip(axes.flat, metrics_to_plot, titles):
        metric_df = df[df['metric_type'] == metric]
        proposed = metric_df[metric_df['system'] == 'proposed'].iloc[0]
        baseline = metric_df[metric_df['system'] == 'baseline'].iloc[0]
        
        percentiles = ['p25', 'p50', 'p75', 'p95', 'p99']
        x = np.arange(len(percentiles))
        width = 0.35
        
        proposed_vals = [proposed[p] for p in percentiles]
        baseline_vals = [baseline[p] for p in percentiles]
        
        ax.plot(x, proposed_vals, marker='o', linewidth=2.5, markersize=8, 
               label='Proposed', color=colors_proposed)
        ax.plot(x, baseline_vals, marker='s', linewidth=2.5, markersize=8, 
               label='Baseline', color=colors_baseline)
        
        ax.set_xticks(x)
        ax.set_xticklabels(['P25', 'P50', 'P75', 'P95', 'P99'])
        ax.set_ylabel('Value', fontweight='bold')
        ax.set_title(title, fontweight='bold', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    fig.suptitle('Percentile Analysis: Proposed vs Baseline', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    plt.savefig(VIZ_DETAILED / "percentile_analysis.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ percentile_analysis.png")
    
    # 3. Variance Comparison
    fig, ax = plt.subplots(figsize=(14, 8))
    
    variance_data = df.pivot_table(values='std_dev', index='metric_type', columns='system')
    
    x = np.arange(len(variance_data.index))
    width = 0.35
    
    ax.bar(x - width/2, variance_data['proposed'], width, label='Proposed', color=colors_proposed, alpha=0.8)
    ax.bar(x + width/2, variance_data['baseline'], width, label='Baseline', color=colors_baseline, alpha=0.8)
    
    ax.set_ylabel('Standard Deviation', fontsize=12, fontweight='bold')
    ax.set_title('Variance Comparison: Proposed vs Baseline', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(['Cost', 'Deadline Rate', 'Queue Time', 'Exec Time'])
    ax.legend(fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(VIZ_DETAILED / "variance_comparison.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ variance_comparison.png")
    
    # 4. Statistical Summary Table (as visualization)
    fig, ax = plt.subplots(figsize=(16, 10))
    ax.axis('tight')
    ax.axis('off')
    
    # Create summary data for table
    summary_data = []
    for metric in metrics_to_plot:
        metric_df = df[df['metric_type'] == metric]
        for system in ['proposed', 'baseline']:
            sys_data = metric_df[metric_df['system'] == system].iloc[0]
            summary_data.append([
                metric.replace('_', ' ').title(),
                system.capitalize(),
                f"{sys_data['min']:.6f}",
                f"{sys_data['p50']:.6f}",
                f"{sys_data['max']:.6f}",
                f"{sys_data['mean']:.6f}",
                f"{sys_data['std_dev']:.6f}",
            ])
    
    columns = ['Metric', 'System', 'Min', 'Median', 'Max', 'Mean', 'Std Dev']
    table = ax.table(cellText=summary_data, colLabels=columns, cellLoc='center', loc='center',
                    colWidths=[0.15, 0.12, 0.13, 0.13, 0.13, 0.13, 0.13])
    
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Style header
    for i in range(len(columns)):
        table[(0, i)].set_facecolor('#40466e')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Alternate row colors
    for i in range(1, len(summary_data) + 1):
        color = '#f0f0f0' if i % 2 == 0 else 'white'
        for j in range(len(columns)):
            table[(i, j)].set_facecolor(color)
    
    plt.title('Statistical Summary Table', fontsize=14, fontweight='bold', pad=20)
    plt.savefig(VIZ_DETAILED / "statistical_summary.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ statistical_summary.png")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main visualization routine."""
    
    print("\n" + "="*90)
    print("🎨 BENCHMARK VISUALIZATION SUITE")
    print("="*90)
    
    print(f"\n📁 Creating visualization directories...")
    print(f"  ✓ {VIZ_BY_SCENARIO}")
    print(f"  ✓ {VIZ_SUMMARY}")
    print(f"  ✓ {VIZ_IMPROVEMENT}")
    print(f"  ✓ {VIZ_DETAILED}")
    
    print(f"\n📊 Generating visualizations...")
    
    visualize_by_scenario()
    visualize_summary()
    visualize_improvements()
    visualize_detailed()
    
    print("\n" + "="*90)
    print("✅ VISUALIZATION COMPLETE!")
    print("="*90)
    print(f"\n📁 Output Directory: {VIZ_DIR}")
    print(f"\n📊 Generated Visualizations:")
    print(f"\n1. By Scenario (comparison_metrics_by_scenario/)")
    print(f"   • cost_comparison.png")
    print(f"   • deadline_comparison.png")
    print(f"   • queue_time_comparison.png")
    print(f"   • exec_time_comparison.png")
    print(f"   • all_metrics_summary.png")
    
    print(f"\n2. Summary (comparison_metrics_summary/)")
    print(f"   • overall_cost.png")
    print(f"   • overall_deadline.png")
    print(f"   • overall_improvements.png")
    print(f"   • summary_dashboard.png")
    
    print(f"\n3. Improvements (comparison_improvement_metrics/)")
    print(f"   • improvement_heatmap.png")
    print(f"   • improvement_percentages.png")
    print(f"   • improvements_by_scenario.png")
    print(f"   • improvement_confidence.png")
    
    print(f"\n4. Detailed Stats (comparison_detailed_stats/)")
    print(f"   • statistical_distributions.png")
    print(f"   • percentile_analysis.png")
    print(f"   • variance_comparison.png")
    print(f"   • statistical_summary.png")
    
    print(f"\n💡 View with: open {VIZ_DIR} (or your file manager)")
    print("="*90 + "\n")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
