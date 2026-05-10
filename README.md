# 🔬 Global Patent Intelligence Data Pipeline

A complete data engineering project that cleans, stores, and analyses
USPTO PatentsView granted patent data.

---

## Project Structure

```
patent_pipeline/
├── data/
│   ├── raw/           ← PUT YOUR TSV FILES HERE
│   ├── clean/         ← generated clean CSVs
│   └── db/            ← generated SQLite database
├── scripts/
│   ├── 01_clean_data.py
│   ├── 02_load_database.py
│   └── 03_analyze_and_report.py
├── sql/
│   ├── schema.sql
│   └── queries.sql
├── reports/           ← generated reports and charts
├── dashboard.py       ← optional Streamlit dashboard
├── run_pipeline.py    ← run everything in one command
└── requirements.txt
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Place your data files

Copy the following TSV files (downloaded from PatentsView) into `data/raw/`:

| File | Required? |
|------|-----------|
| `g_patent.tsv` | ✅ Required |
| `g_patent_abstract.tsv` | Recommended |
| `g_application.tsv` | Recommended |
| `g_inventor_disambiguated.tsv` | ✅ Required |
| `g_assignee_disambiguated.tsv` | ✅ Required |
| `g_location_disambiguated.tsv` | Recommended |

> Download from: https://data.uspto.gov/bulkdata/datasets/pvgpatdis

---

## Running the Pipeline

### Option A – Run everything at once (recommended)

```bash
python run_pipeline.py
```

### Option B – Run step by step

```bash
python scripts/01_clean_data.py      # Clean raw TSVs → data/clean/
python scripts/02_load_database.py   # Load into SQLite → data/db/patents.db
python scripts/03_analyze_and_report.py  # Run queries, export reports
```

---

## Outputs

### Console Report
Printed automatically during Step 3 – shows totals, top inventors,
companies, and countries directly in the terminal.

### CSV Reports (in `reports/`)
| File | Contents |
|------|----------|
| `top_inventors.csv` | Top 20 inventors by patent count |
| `top_companies.csv` | Top 20 companies by patent count |
| `country_trends.csv` | Patent counts by country |
| `yearly_trends.csv` | Patent counts by year |
| `inventor_rankings.csv` | Window function ranking of inventors |
| `q5_join_result.csv` | Sample JOIN query result |
| `q6_cte_result.csv` | CTE query – top inventor per country |

### JSON Report
`reports/report.json` – machine-readable summary of totals,
top inventors, companies, countries and yearly trends.

### Charts (in `reports/`)
- `chart_top_inventors.png`
- `chart_top_companies.png`
- `chart_country_share.png`
- `chart_yearly_trend.png`

---

## Optional Dashboard

```bash
streamlit run dashboard.py
```

Opens an interactive browser dashboard with:
- KPI cards (total patents, inventors, companies)
- Year-range filter and Top-N slider
- Trend chart, inventor chart, company chart, country pie chart

---

## SQL Queries Implemented

| # | Query | Technique |
|---|-------|-----------|
| Q1 | Top inventors | GROUP BY + ORDER BY |
| Q2 | Top companies | GROUP BY + ORDER BY |
| Q3 | Countries | GROUP BY + DISTINCT |
| Q4 | Yearly trends | GROUP BY year |
| Q5 | Patent detail | 4-table JOIN |
| Q6 | Top inventor per country | CTE (WITH statement) |
| Q7 | Inventor ranking | Window functions (RANK, PARTITION BY) |

See `sql/queries.sql` to run them manually in DB Browser for SQLite.

---

## Reproducibility

Anyone can clone this repo and reproduce the results by:

1. Downloading the same TSV files from PatentsView
2. Running `pip install -r requirements.txt`
3. Running `python run_pipeline.py`
