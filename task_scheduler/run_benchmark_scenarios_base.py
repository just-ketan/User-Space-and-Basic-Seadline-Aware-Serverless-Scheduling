#!/usr/bin/env python3
"""
RUN BENCHMARK SCENARIOS (Deterministic Version)
Simulates multiple workload sizes automatically:
 - Small (100 tasks)
 - Medium (5,000 tasks)
 - Large (25,000 tasks)
 - Very Large (100,000 tasks)

Each simulation:
  - Generates workload deterministically using unique seeds
  - Runs the optimized simulator
  - Records elapsed wall time and estimated cost
  - Writes combined results to benchmark_results.csv

All results are fully reproducible between runs.
"""

import subprocess
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Paths
HERE = Path(__file__).resolve().parent
SIM_PATH = HERE / "optimized_simulator.py"
GEN_PATH = HERE / "azure_workload_generator.py"
OUTPUT_DIR = HERE / "Benchmarks"
OUTPUT_DIR.mkdir(exist_ok=True)

# Benchmark Scenarios — deterministic seeds for reproducibility
SCENARIOS = [
    {"name": "Small",      "tasks": 1000,     "batch": 100,   "concurrency": 2,  "seed": 101},
    {"name": "Medium",     "tasks": 10000,    "batch": 500,   "concurrency": 4,  "seed": 202},
    {"name": "Large",      "tasks": 100000,   "batch": 1000,  "concurrency": 8,  "seed": 303},
    {"name": "VeryLarge",  "tasks": 500000,   "batch": 2000,  "concurrency": 12, "seed": 404},
    {"name": "Extreme",    "tasks": 1000000,  "batch": 5000,  "concurrency": 16, "seed": 505},
]



def run_cmd(cmd, stdin_data=None, timeout=None):
    """Run a subprocess command and capture stdout/stderr with timing."""
    try:
        start = time.time()
        result = subprocess.run(
            cmd, input=stdin_data, text=True,
            capture_output=True, timeout=timeout
        )
        elapsed = time.time() - start
        return result.returncode, result.stdout, result.stderr, elapsed
    except subprocess.TimeoutExpired:
        return 1, "", "Timeout", timeout

def generate_workload(num_tasks, output_file, seed):
    """Generate deterministic synthetic workload."""
    cmd = [
        sys.executable, str(GEN_PATH),
        "--tasks", str(num_tasks),
        "--output", output_file,
        "--seed", str(seed),
        "--verbose"
    ]
    print(f"\n🧩 Generating workload: {num_tasks} tasks (seed={seed})")
    rc, out, err, _ = run_cmd(cmd, timeout=180)
    if rc != 0:
        print("❌ Workload generation failed:", err)
        return False
    print(f"✓ Workload saved: {output_file}")
    return True

def run_simulation(config_file, batch_size, concurrency, cold_ms, ttl):
    """Run optimized simulator on a given workload."""
    cmd = [
        sys.executable, str(SIM_PATH),
        "--batch-size", str(batch_size),
        "--concurrency", str(concurrency),
        "--cold-start-ms", str(cold_ms),
        "--reuse-ttl", str(ttl)
    ]
    print(f"\n🚀 Running simulation: batch={batch_size}, concurrency={concurrency}")
    with open(config_file, "r") as f:
        rc, out, err, elapsed = run_cmd(cmd, stdin_data=f.read(), timeout=3600)
    return rc, out, err, elapsed

def extract_cost(stdout_text):
    """Parse printed cost summary from simulator output."""
    lines = stdout_text.splitlines()
    total_cost, avg_cost = 0.0, 0.0
    for line in lines:
        if "Estimated Total Cost" in line:
            try:
                total_cost = float(line.split("$")[-1])
            except ValueError:
                pass
        elif "Avg per task" in line:
            try:
                avg_cost = float(line.split("$")[-1])
            except ValueError:
                pass
    return total_cost, avg_cost

def main():
    results = []
    print("\n" + "="*80)
    print("⚙️  SERVERLESS SIMULATOR BENCHMARK (Deterministic Run)")
    print("="*80)

    for scenario in SCENARIOS:
        name = scenario["name"]
        num_tasks = scenario["tasks"]
        seed = scenario["seed"]
        workload_file = OUTPUT_DIR / f"run_{name.lower()}.json"

        print("\n" + "="*80)
        print(f"🏗️  SCENARIO: {name} ({num_tasks} tasks, seed={seed})")
        print("="*80)

        # Generate workload deterministically
        if not generate_workload(num_tasks, workload_file, seed):
            continue

        # Run simulation
        rc, out, err, elapsed = run_simulation(
            workload_file,
            batch_size=scenario.get("batch_size", scenario.get("batch", 1000)),
            concurrency=scenario["concurrency"],
            cold_ms=250,
            ttl=120
        )

        if rc != 0:
            print(f"❌ Simulation failed for {name}")
            print(err)
            continue

        total_cost, avg_cost = extract_cost(out)
        results.append({
            "Scenario": name,
            "Tasks": num_tasks,
            "Seed": seed,
            "Elapsed_s": round(elapsed, 2),
            "TotalCost": total_cost,
            "AvgCost": avg_cost
        })

        print(out)
        print(f"✅ Completed {name} in {elapsed:.2f}s\n")

    # Write combined summary
    summary_path = OUTPUT_DIR / "benchmark_results.csv"
    with open(summary_path, "w") as f:
        f.write("Scenario,Tasks,Seed,Elapsed_s,TotalCost,AvgCost\n")
        for r in results:
            f.write(f"{r['Scenario']},{r['Tasks']},{r['Seed']},{r['Elapsed_s']},"
                    f"{r['TotalCost']},{r['AvgCost']}\n")

    print("\n📊 BENCHMARK SUMMARY")
    print("-"*75)
    for r in results:
        print(f"{r['Scenario']:<12} | {r['Tasks']:>8} tasks | "
              f"Seed {r['Seed']:<4} | {r['Elapsed_s']:>6.2f}s | "
              f"Total ${r['TotalCost']:.6f} | Avg ${r['AvgCost']:.8f}")

    print(f"\n✓ All results saved to {summary_path}\n")

if __name__ == "__main__":
    main()
