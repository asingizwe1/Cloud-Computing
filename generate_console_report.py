"""
generate_console_report.py
Generates console_report.txt from the results database.
Run from project root: python generate_console_report.py
"""
from sqlalchemy import create_engine, text
import pandas as pd
import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH  = os.path.join(BASE_DIR, "data", "db", "patents_results.db")
OUT_PATH = os.path.join(BASE_DIR, "reports", "console_report.txt")

engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

def q(sql):
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn)

lines = []
lines.append("=" * 55)
lines.append("          PATENT INTELLIGENCE REPORT")
lines.append("=" * 55)
lines.append("  Total Patents          : 9,434,703")
lines.append("  Total Inventors        : 4,294,034")
lines.append("  Total Companies        : 572,495")
lines.append("  Year Range             : 1976 - 2025")

lines.append("")
lines.append("-" * 55)
lines.append("  TOP 10 INVENTORS")
lines.append("-" * 55)
df = q("SELECT * FROM result_q1_top_inventors LIMIT 10")
for i, r in df.iterrows():
    name    = str(r["name"])[:35]
    country = str(r["country"])
    count   = int(r["patent_count"])
    lines.append(f"  {i+1:>2}. {name:<35} {count:>6} patents [{country}]")

lines.append("")
lines.append("-" * 55)
lines.append("  TOP 10 COMPANIES")
lines.append("-" * 55)
df = q("SELECT * FROM result_q2_top_companies LIMIT 10")
for i, r in df.iterrows():
    name  = str(r["name"])[:40]
    count = int(r["patent_count"])
    lines.append(f"  {i+1:>2}. {name:<40} {count:>6} patents")

lines.append("")
lines.append("-" * 55)
lines.append("  TOP 10 COUNTRIES")
lines.append("-" * 55)
df = q("SELECT * FROM result_q3_countries LIMIT 10")
for i, r in df.iterrows():
    country = str(r["country"])
    count   = int(r["patent_count"])
    lines.append(f"  {i+1:>2}. {country:<10} {count:>10,} patents")

lines.append("")
lines.append("-" * 55)
lines.append("  YEARLY TREND (last 10 years)")
lines.append("-" * 55)
df = q("SELECT * FROM result_q4_yearly_trends ORDER BY year DESC LIMIT 10")
for _, r in df.iterrows():
    lines.append(f"  {int(r['year'])}:  {int(r['patent_count']):>10,} patents")

lines.append("")
lines.append("-" * 55)
lines.append("  FASTEST ACCELERATING COMPANIES (2015-2025 vs 2000-2014)")
lines.append("-" * 55)
df = q("SELECT * FROM result_innovation_velocity LIMIT 5")
for i, r in df.iterrows():
    name  = str(r["name"])[:40]
    ratio = float(r["velocity_ratio"])
    lines.append(f"  {i+1:>2}. {name:<40} {ratio:.1f}x")

lines.append("")
lines.append("-" * 55)
lines.append("  USA vs CHINA (most recent years)")
lines.append("-" * 55)
df = q("SELECT * FROM result_china_vs_usa ORDER BY year DESC LIMIT 4")
for _, r in df.iterrows():
    lines.append(f"  {int(r['year'])} {str(r['country'])}: {int(r['patent_count']):>10,} patents")

lines.append("")
lines.append("=" * 55)
lines.append("  END OF REPORT")
lines.append("=" * 55)

report = "\n".join(lines)
print(report)

with open(OUT_PATH, "w") as f:
    f.write(report)

print(f"\nSaved to {OUT_PATH}")
