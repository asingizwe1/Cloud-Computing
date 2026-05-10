"""
run_pipeline.py
Master script – runs all three pipeline steps in sequence.
Usage:  python run_pipeline.py
"""

import subprocess
import sys
import os
import time

SCRIPTS = [
    ("STEP 1 – Clean Data",        "scripts/01_clean_data.py"),
    ("STEP 2 – Load Database",     "scripts/02_load_database.py"),
    ("STEP 3 – Analyze & Report",  "scripts/03_analyze_and_report.py"),
]

BASE = os.path.dirname(os.path.abspath(__file__))


def run(label, script):
    path = os.path.join(BASE, script)
    print(f"\n{'='*55}")
    print(f"  {label}")
    print(f"{'='*55}")
    start = time.time()
    result = subprocess.run([sys.executable, path], cwd=BASE)
    elapsed = time.time() - start
    if result.returncode != 0:
        print(f"\n[ERROR] {label} failed (exit code {result.returncode}). Aborting.")
        sys.exit(result.returncode)
    print(f"\n  Completed in {elapsed:.1f}s")


if __name__ == "__main__":
    print("\n" + "="*55)
    print("   PATENT INTELLIGENCE PIPELINE")
    print("="*55)
    total_start = time.time()
    for label, script in SCRIPTS:
        run(label, script)
    total = time.time() - total_start
    print(f"\n{'='*55}")
    print(f"  PIPELINE COMPLETE  ({total:.1f}s total)")
    print(f"{'='*55}")
    print("\n  Output locations:")
    print("    data/clean/    → cleaned CSV files")
    print("    data/db/       → patents.db (SQLite)")
    print("    reports/       → CSVs, JSON, charts")
    print("\n  To launch the dashboard:")
    print("    streamlit run dashboard.py")
    print()
