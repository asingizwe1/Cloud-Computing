"""
04b_cpc_remaining.py
Completes CPC steps 2 and 3 using chunked processing to save memory.
Run this instead of re-running 04_cpc_analysis.py
"""

import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from collections import Counter

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
CLEAN_DIR   = os.path.join(BASE_DIR, "data", "clean")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
DB_PATH     = os.path.join(BASE_DIR, "data", "db", "patents.db")
ENGINE      = create_engine(f"sqlite:///{DB_PATH}", echo=False)

CPC_PATH = os.path.join(CLEAN_DIR, "clean_cpc.csv")
CHUNK    = 500_000

# ══════════════════════════════════════════════════════════
# STEP 2: Technology share — count in chunks, never load all at once
# ══════════════════════════════════════════════════════════
print("=" * 55)
print("  STEP 2: Technology Share (chunked)")
print("=" * 55)

tech_counter = Counter()
seen_patents  = set()
chunk_num = 0

for chunk in pd.read_csv(CPC_PATH,
                          usecols=["patent_id", "cpc_sequence", "technology_area"],
                          low_memory=False, chunksize=CHUNK):
    chunk_num += 1
    print(f"  Chunk {chunk_num}...", end="\r")
    # Only primary classification, one per patent
    primary = chunk[chunk["cpc_sequence"] == 0].copy()
    primary = primary[~primary["patent_id"].isin(seen_patents)]
    primary = primary.drop_duplicates(subset=["patent_id"])
    seen_patents.update(primary["patent_id"].tolist())
    tech_counter.update(primary["technology_area"].dropna().tolist())

print(f"\n  Processed {chunk_num} chunks, {len(seen_patents):,} unique patents")

share = pd.DataFrame(tech_counter.items(), columns=["technology_area", "patent_count"])
share = share.sort_values("patent_count", ascending=False).reset_index(drop=True)
share.to_sql("result_technology_share", ENGINE, if_exists="replace", index=False)
share.to_csv(os.path.join(REPORTS_DIR, "technology_share.csv"), index=False)
print(f"  Saved technology_share — {len(share)} areas")

# Chart
fig, ax = plt.subplots(figsize=(10, 7))
top    = share.head(8)
others = share.iloc[8:]["patent_count"].sum()
labels = list(top["technology_area"]) + (["Others"] if others > 0 else [])
sizes  = list(top["patent_count"])    + ([others]   if others > 0 else [])
ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140,
       colors=sns.color_palette("tab10", len(labels)))
ax.set_title("Patent Share by Technology Area (All Years)", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(os.path.join(REPORTS_DIR, "chart_technology_share.png"), dpi=120)
plt.close(fig)
print("  chart_technology_share.png saved")

# ══════════════════════════════════════════════════════════
# STEP 3: Country specialisation — chunked CPC + DB for inventors
# ══════════════════════════════════════════════════════════
print("\n" + "=" * 55)
print("  STEP 3: Country Specialisation (chunked)")
print("=" * 55)

# Load lead inventors from DB (small enough)
print("  Loading lead inventors from DB...")
pat_inv   = pd.read_sql("SELECT patent_id, inventor_id FROM patent_inventors WHERE inventor_sequence=0", ENGINE)
inventors = pd.read_sql("SELECT inventor_id, country FROM inventors", ENGINE)
lead = pat_inv.merge(inventors, on="inventor_id", how="left")
major = ["US", "CN", "JP", "KR", "DE", "GB", "FR", "TW"]
lead  = lead[lead["country"].isin(major)].set_index("patent_id")
print(f"  Loaded {len(lead):,} lead inventor records")
del pat_inv, inventors

# Count country-tech pairs in chunks
from collections import defaultdict
country_tech = defaultdict(Counter)
seen2 = set()
chunk_num = 0

for chunk in pd.read_csv(CPC_PATH,
                          usecols=["patent_id", "cpc_sequence", "technology_area"],
                          low_memory=False, chunksize=CHUNK):
    chunk_num += 1
    print(f"  Chunk {chunk_num}...", end="\r")
    primary = chunk[chunk["cpc_sequence"] == 0].copy()
    primary = primary[~primary["patent_id"].isin(seen2)]
    primary = primary.drop_duplicates(subset=["patent_id"])
    seen2.update(primary["patent_id"].tolist())

    primary["patent_id"] = primary["patent_id"].astype(str)
    primary = primary.set_index("patent_id")

    # Join with lead inventors
    joined = primary.join(lead[["country"]], how="inner")
    for _, row in joined.iterrows():
        if pd.notna(row["technology_area"]) and pd.notna(row["country"]):
            country_tech[row["country"]][row["technology_area"]] += 1

print(f"\n  Processed {chunk_num} chunks")

# Build dataframe from counters
rows = []
for country, tech_counts in country_tech.items():
    for tech, cnt in tech_counts.items():
        rows.append({"country": country, "technology_area": tech, "patents": cnt})
spec = pd.DataFrame(rows)
spec.to_sql("result_country_tech_specialisation", ENGINE, if_exists="replace", index=False)
spec.to_csv(os.path.join(REPORTS_DIR, "country_tech_specialisation.csv"), index=False)
print(f"  Saved country_tech_specialisation — {len(spec)} rows")

# Heatmap
pivot = spec.pivot_table(index="country", columns="technology_area",
                         values="patents", aggfunc="sum").fillna(0)
fig, ax = plt.subplots(figsize=(13, 6))
sns.heatmap(pivot, annot=True, fmt=".0f", cmap="YlOrRd", ax=ax,
            linewidths=0.5, cbar_kws={"label": "Patents"})
ax.set_title("Technology Specialisation by Country\n(Top 8 Patent-Producing Nations)",
             fontsize=13, fontweight="bold")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
fig.savefig(os.path.join(REPORTS_DIR, "chart_country_specialisation.png"), dpi=120)
plt.close(fig)
print("  chart_country_specialisation.png saved")

print("\n" + "=" * 55)
print("  ALL CPC STEPS COMPLETE")
print("=" * 55)
print("\n  Saved to reports/:")
print("    technology_share.csv")
print("    country_tech_specialisation.csv")
print("    chart_technology_share.png")
print("    chart_country_specialisation.png")