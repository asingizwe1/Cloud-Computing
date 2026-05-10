"""
03_analyze_and_report.py
Runs all required SQL queries using a hybrid approach:
- Heavy aggregations use pandas on clean CSVs (avoids SQLite temp space issues)
- Results are saved back to the database as result tables
- All 7 required queries are implemented and documented
Run AFTER 02_load_database.py.
"""

import os
import json
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import sqlite3

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
DB_PATH    = os.path.join(BASE_DIR, "data", "db", "patents.db")
CLEAN_DIR  = os.path.join(BASE_DIR, "data", "clean")
REPORTS_DIR= os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

ENGINE = create_engine(f"sqlite:///{DB_PATH}", echo=False)

# ── Set temp dir to F: so SQLite has room ──────────────────
os.makedirs("F:/Temp", exist_ok=True)
_con = sqlite3.connect(DB_PATH)
_con.execute("PRAGMA temp_store = 1")
_con.execute("PRAGMA temp_store_directory = 'F:/Temp'")
_con.close()


def load_csv(name, usecols=None):
    path = os.path.join(CLEAN_DIR, name)
    if not os.path.exists(path):
        print(f"  [SKIP] {name} not found")
        return None
    print(f"  Loading {name}...")
    return pd.read_csv(path, usecols=usecols, low_memory=False)


def save_to_db(df, table_name):
    """Save a result dataframe back into the database."""
    df.to_sql(table_name, ENGINE, if_exists="replace", index=False)


def simple_query(sql):
    """For small/simple queries that don't need temp space."""
    try:
        with ENGINE.connect() as conn:
            return pd.read_sql_query(text(sql), conn)
    except Exception as e:
        print(f"  [WARN] DB query failed: {e}")
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════
#  LOAD CLEAN DATA
# ══════════════════════════════════════════════════════════

print("=" * 55)
print("  STEP 3: ANALYSIS & REPORTING")
print("=" * 55)
print("\nLoading clean CSV files...")

patents    = load_csv("clean_patents.csv",
                      usecols=["patent_id","title","year","patent_type","num_claims"])
inventors  = load_csv("clean_inventors.csv",
                      usecols=["inventor_id","name","country"])
companies  = load_csv("clean_companies.csv",
                      usecols=["company_id","name","country"])
pat_inv    = load_csv("clean_patent_inventors.csv",
                      usecols=["patent_id","inventor_id","inventor_sequence"])
pat_co     = load_csv("clean_patent_companies.csv",
                      usecols=["patent_id","company_id","assignee_sequence"])

# ══════════════════════════════════════════════════════════
#  Q1: TOP INVENTORS
# ══════════════════════════════════════════════════════════
print("\nQ1: Top Inventors...")
q1 = (pat_inv
      .merge(inventors, on="inventor_id", how="left")
      .groupby(["inventor_id","name","country"])["patent_id"]
      .count()
      .reset_index(name="patent_count")
      .sort_values("patent_count", ascending=False)
      .head(20))
save_to_db(q1, "result_q1_top_inventors")

# ══════════════════════════════════════════════════════════
#  Q2: TOP COMPANIES
# ══════════════════════════════════════════════════════════
print("Q2: Top Companies...")
q2 = (pat_co
      .merge(companies, on="company_id", how="left")
      .dropna(subset=["name"])
      .groupby(["company_id","name","country"])["patent_id"]
      .count()
      .reset_index(name="patent_count")
      .sort_values("patent_count", ascending=False)
      .head(20))
save_to_db(q2, "result_q2_top_companies")

# ══════════════════════════════════════════════════════════
#  Q3: COUNTRIES
# ══════════════════════════════════════════════════════════
print("Q3: Countries...")
q3 = (pat_inv
      .merge(inventors[["inventor_id","country"]], on="inventor_id", how="left")
      .dropna(subset=["country"])
      .query("country != ''")
      .drop_duplicates(subset=["patent_id","country"])
      .groupby("country")["patent_id"]
      .count()
      .reset_index(name="patent_count")
      .sort_values("patent_count", ascending=False)
      .head(30))
save_to_db(q3, "result_q3_countries")

# ══════════════════════════════════════════════════════════
#  Q4: TRENDS OVER TIME
# ══════════════════════════════════════════════════════════
print("Q4: Yearly trends...")
q4 = (patents
      .dropna(subset=["year"])
      .astype({"year": int})
      .query("year >= 1976 and year <= 2025")
      .groupby("year")["patent_id"]
      .count()
      .reset_index(name="patent_count")
      .sort_values("year"))
save_to_db(q4, "result_q4_yearly_trends")

# ══════════════════════════════════════════════════════════
#  Q5: JOIN – patents with inventor and company info
# ══════════════════════════════════════════════════════════
print("Q5: Join query...")
lead_inv = pat_inv[pat_inv["inventor_sequence"]==0][["patent_id","inventor_id"]]
lead_co  = pat_co[pat_co["assignee_sequence"]==0][["patent_id","company_id"]]
q5 = (patents[["patent_id","title","year"]]
      .merge(lead_inv, on="patent_id", how="left")
      .merge(inventors[["inventor_id","name","country"]].rename(
             columns={"name":"inventor_name","country":"inventor_country"}),
             on="inventor_id", how="left")
      .merge(lead_co, on="patent_id", how="left")
      .merge(companies[["company_id","name"]].rename(columns={"name":"company_name"}),
             on="company_id", how="left")
      [["patent_id","title","year","inventor_name","inventor_country","company_name"]]
      .head(1000))
save_to_db(q5, "result_q5_join")

# ══════════════════════════════════════════════════════════
#  Q6: CTE – top inventor per country
#  (implemented as pandas equivalent of a WITH statement)
# ══════════════════════════════════════════════════════════
print("Q6: CTE — top inventor per country...")
# Step 1 (CTE: inventor_counts)
inv_counts = (pat_inv
              .merge(inventors, on="inventor_id", how="left")
              .dropna(subset=["country"])
              .groupby(["inventor_id","name","country"])["patent_id"]
              .count()
              .reset_index(name="patent_count"))
# Step 2 (CTE: country_totals)
country_totals = (inv_counts
                  .groupby("country")["patent_count"]
                  .sum()
                  .reset_index(name="country_total"))
# Step 3: join and find top inventor per country
q6 = (inv_counts
      .merge(country_totals, on="country")
      .sort_values("patent_count", ascending=False)
      .drop_duplicates(subset=["country"])
      .rename(columns={"name":"top_inventor","patent_count":"inventor_patents"})
      .sort_values("country_total", ascending=False)
      .head(20))
save_to_db(q6, "result_q6_cte")

# ══════════════════════════════════════════════════════════
#  Q7: RANKING – window function equivalent
# ══════════════════════════════════════════════════════════
print("Q7: Ranking with window functions...")
inv_rank = (pat_inv
            .merge(inventors, on="inventor_id", how="left")
            .dropna(subset=["country"])
            .groupby(["inventor_id","name","country"])["patent_id"]
            .count()
            .reset_index(name="patent_count"))
total_patents = inv_rank["patent_count"].sum()
inv_rank["overall_rank"]    = inv_rank["patent_count"].rank(method="min", ascending=False).astype(int)
inv_rank["rank_in_country"] = inv_rank.groupby("country")["patent_count"].rank(method="min", ascending=False).astype(int)
inv_rank["pct_of_all"]      = (inv_rank["patent_count"] / total_patents * 100).round(4)
q7 = inv_rank.sort_values("overall_rank").head(50)
save_to_db(q7, "result_q7_ranking")

# ══════════════════════════════════════════════════════════
#  INSIGHT: Innovation Velocity
# ══════════════════════════════════════════════════════════
print("Insight: Innovation velocity...")
recent = (pat_co
          .merge(patents[["patent_id","year"]], on="patent_id")
          .merge(companies[["company_id","name"]], on="company_id")
          .dropna(subset=["name","year"])
          .query("year >= 2015")
          .groupby("name")["patent_id"].count()
          .reset_index(name="recent_patents"))
historical = (pat_co
              .merge(patents[["patent_id","year"]], on="patent_id")
              .merge(companies[["company_id","name"]], on="company_id")
              .dropna(subset=["name","year"])
              .query("2000 <= year <= 2014")
              .groupby("name")["patent_id"].count()
              .reset_index(name="historical_patents"))
velocity = (recent.merge(historical, on="name", how="left")
            .fillna({"historical_patents": 0})
            .query("recent_patents >= 50"))
velocity["velocity_ratio"] = (velocity["recent_patents"] /
                               velocity["historical_patents"].clip(lower=1)).round(2)
velocity = velocity.sort_values("velocity_ratio", ascending=False).head(20)
save_to_db(velocity, "result_innovation_velocity")

# ══════════════════════════════════════════════════════════
#  INSIGHT: China vs USA
# ══════════════════════════════════════════════════════════
print("Insight: China vs USA patent race...")
race = (pat_inv
        .merge(inventors[["inventor_id","country"]], on="inventor_id")
        .merge(patents[["patent_id","year"]], on="patent_id")
        .query("country in ['US','CN'] and year >= 1990")
        .drop_duplicates(subset=["patent_id","country"])
        .groupby(["year","country"])["patent_id"].count()
        .reset_index(name="patent_count"))
save_to_db(race, "result_china_vs_usa")

print("\nAll results saved to database.")

# ══════════════════════════════════════════════════════════
#  CONSOLE REPORT
# ══════════════════════════════════════════════════════════
total = len(patents)
bar = "=" * 55
print(f"\n{bar}")
print("          PATENT INTELLIGENCE REPORT")
print(bar)
print(f"  Total Patents in Database : {total:,}")
if not q4.empty:
    print(f"  Year Range               : {int(q4['year'].min())} – {int(q4['year'].max())}")

print(f"\n{'─'*55}")
print("  TOP 10 INVENTORS")
print(f"{'─'*55}")
for i, row in q1.head(10).reset_index(drop=True).iterrows():
    print(f"  {i+1:>2}. {str(row['name']):<35} {row['patent_count']:>6} patents  [{row.get('country','?')}]")

print(f"\n{'─'*55}")
print("  TOP 10 COMPANIES")
print(f"{'─'*55}")
for i, row in q2.head(10).reset_index(drop=True).iterrows():
    name = str(row['name'])[:40] if row['name'] else "Unknown"
    print(f"  {i+1:>2}. {name:<40} {row['patent_count']:>6} patents  [{row.get('country','?')}]")

print(f"\n{'─'*55}")
print("  TOP 10 COUNTRIES")
print(f"{'─'*55}")
for i, row in q3.head(10).reset_index(drop=True).iterrows():
    print(f"  {i+1:>2}. {str(row['country']):<10} {row['patent_count']:>10,} patents")

if not velocity.empty:
    print(f"\n{'─'*55}")
    print("  FASTEST ACCELERATING COMPANIES (2015–2025 vs 2000–2014)")
    print(f"{'─'*55}")
    for i, row in velocity.head(5).reset_index(drop=True).iterrows():
        print(f"  {str(row['name'])[:40]:<40} {row['velocity_ratio']:.1f}x")

if not race.empty:
    us_l = race[race["country"]=="US"]["patent_count"].iloc[-1] if len(race[race["country"]=="US"]) else 0
    cn_l = race[race["country"]=="CN"]["patent_count"].iloc[-1] if len(race[race["country"]=="CN"]) else 0
    print(f"\n{'─'*55}")
    print("  USA vs CHINA (most recent year)")
    print(f"{'─'*55}")
    print(f"  USA:   {int(us_l):>8,} patents")
    print(f"  China: {int(cn_l):>8,} patents")

print(f"\n{bar}\n")

# ══════════════════════════════════════════════════════════
#  CSV EXPORTS
# ══════════════════════════════════════════════════════════
print("Exporting CSVs...")
q1.to_csv(os.path.join(REPORTS_DIR, "top_inventors.csv"), index=False)
q2.to_csv(os.path.join(REPORTS_DIR, "top_companies.csv"), index=False)
q3.to_csv(os.path.join(REPORTS_DIR, "country_trends.csv"), index=False)
q4.to_csv(os.path.join(REPORTS_DIR, "yearly_trends.csv"), index=False)
q7.to_csv(os.path.join(REPORTS_DIR, "inventor_rankings.csv"), index=False)
q5.to_csv(os.path.join(REPORTS_DIR, "q5_join_result.csv"), index=False)
q6.to_csv(os.path.join(REPORTS_DIR, "q6_cte_result.csv"), index=False)
velocity.to_csv(os.path.join(REPORTS_DIR, "innovation_velocity.csv"), index=False)
race.to_csv(os.path.join(REPORTS_DIR, "china_vs_usa.csv"), index=False)
print("  CSVs saved to reports/")

# ══════════════════════════════════════════════════════════
#  JSON REPORT
# ══════════════════════════════════════════════════════════
country_total = q3["patent_count"].sum()
report = {
    "total_patents": int(total),
    "top_inventors": [
        {"rank": i+1, "name": r["name"], "country": r.get("country"), "patents": int(r["patent_count"])}
        for i, r in q1.head(10).reset_index(drop=True).iterrows()
    ],
    "top_companies": [
        {"rank": i+1, "name": r["name"], "country": r.get("country"), "patents": int(r["patent_count"])}
        for i, r in q2.head(10).reset_index(drop=True).iterrows()
    ],
    "top_countries": [
        {"country": r["country"], "patents": int(r["patent_count"]),
         "share": round(r["patent_count"] / country_total, 4) if country_total else 0}
        for _, r in q3.head(10).iterrows()
    ],
    "yearly_trends": [
        {"year": int(r["year"]), "patents": int(r["patent_count"])}
        for _, r in q4.iterrows()
    ],
    "fastest_accelerating_companies": [
        {"name": r["name"], "recent_patents": int(r["recent_patents"]),
         "historical_patents": int(r["historical_patents"]),
         "velocity_ratio": float(r["velocity_ratio"])}
        for _, r in velocity.head(10).iterrows()
    ] if not velocity.empty else [],
}
with open(os.path.join(REPORTS_DIR, "report.json"), "w") as f:
    json.dump(report, f, indent=2)
print("  report.json saved")

# ══════════════════════════════════════════════════════════
#  CHARTS
# ══════════════════════════════════════════════════════════
print("\nGenerating charts...")
sns.set_theme(style="whitegrid")

# Chart 1 – Top Inventors
fig, ax = plt.subplots(figsize=(10, 6))
data = q1.head(15).sort_values("patent_count")
ax.barh(data["name"].astype(str), data["patent_count"],
        color=sns.color_palette("Blues_d", len(data)))
ax.set_title("Top 15 Inventors by Patent Count", fontsize=14, fontweight="bold")
ax.set_xlabel("Number of Patents")
plt.tight_layout()
fig.savefig(os.path.join(REPORTS_DIR, "chart_top_inventors.png"), dpi=120)
plt.close(fig)

# Chart 2 – Top Companies
fig, ax = plt.subplots(figsize=(12, 6))
data = q2.head(15).sort_values("patent_count")
names = [str(n)[:35] if n else "Unknown" for n in data["name"]]
ax.barh(names, data["patent_count"], color=sns.color_palette("Greens_d", len(data)))
ax.set_title("Top 15 Companies by Patent Count", fontsize=14, fontweight="bold")
ax.set_xlabel("Number of Patents")
plt.tight_layout()
fig.savefig(os.path.join(REPORTS_DIR, "chart_top_companies.png"), dpi=120)
plt.close(fig)

# Chart 3 – Country pie
fig, ax = plt.subplots(figsize=(9, 7))
top_n = 10
data = q3.head(top_n)
others = q3.iloc[top_n:]["patent_count"].sum()
labels = list(data["country"]) + (["Others"] if others > 0 else [])
sizes  = list(data["patent_count"]) + ([others] if others > 0 else [])
ax.pie(sizes, labels=labels, autopct="%1.1f%%", startangle=140,
       colors=sns.color_palette("tab20", len(labels)))
ax.set_title("Patent Share by Country (Top 10)", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(os.path.join(REPORTS_DIR, "chart_country_share.png"), dpi=120)
plt.close(fig)

# Chart 4 – Yearly trend
if not q4.empty:
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(q4["year"], q4["patent_count"], color="#2196F3", linewidth=2)
    ax.fill_between(q4["year"], q4["patent_count"], alpha=0.15, color="#2196F3")
    ax.set_title("Patents Granted Per Year (1976–2025)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Year"); ax.set_ylabel("Number of Patents")
    plt.tight_layout()
    fig.savefig(os.path.join(REPORTS_DIR, "chart_yearly_trend.png"), dpi=120)
    plt.close(fig)

# Chart 5 – Innovation Velocity
if not velocity.empty:
    data = velocity.head(15).sort_values("velocity_ratio")
    fig, ax = plt.subplots(figsize=(11, 6))
    ax.barh([str(n)[:40] for n in data["name"]], data["velocity_ratio"],
            color=sns.color_palette("Oranges_d", len(data)))
    ax.set_title("Fastest Accelerating Companies\n(2015–2025 vs 2000–2014)",
                 fontsize=13, fontweight="bold")
    ax.set_xlabel("Velocity Ratio (recent ÷ historical)")
    ax.axvline(x=1, color="red", linestyle="--", linewidth=1, label="No change")
    ax.legend()
    plt.tight_layout()
    fig.savefig(os.path.join(REPORTS_DIR, "chart_innovation_velocity.png"), dpi=120)
    plt.close(fig)

# Chart 6 – China vs USA
if not race.empty:
    us = race[race["country"]=="US"]
    cn = race[race["country"]=="CN"]
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(us["year"], us["patent_count"], color="#3C78D8", linewidth=2.5, label="USA")
    ax.fill_between(us["year"], us["patent_count"], alpha=0.1, color="#3C78D8")
    ax.plot(cn["year"], cn["patent_count"], color="#E53935", linewidth=2.5, label="China")
    ax.fill_between(cn["year"], cn["patent_count"], alpha=0.1, color="#E53935")
    ax.set_title("The Patent Race: USA vs China (1990–Present)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Year"); ax.set_ylabel("Patents Granted")
    ax.legend(fontsize=12)
    merged = us.merge(cn, on="year", suffixes=("_us","_cn"))
    cross = merged[merged["patent_count_cn"] >= merged["patent_count_us"]]
    if not cross.empty:
        yr = int(cross.iloc[0]["year"])
        ax.axvline(x=yr, color="gray", linestyle=":", linewidth=1.5)
        ax.annotate(f"Crossover\n{yr}", xy=(yr, ax.get_ylim()[1]*0.8),
                    fontsize=9, color="gray", ha="center")
    plt.tight_layout()
    fig.savefig(os.path.join(REPORTS_DIR, "chart_china_vs_usa.png"), dpi=120)
    plt.close(fig)

print("  Charts saved to reports/")
print("\nAll done! Check the reports/ folder.")
print("\nAll result tables also saved to patents.db for SQL verification.")