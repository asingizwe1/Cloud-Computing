# 🔬 Global Patent Intelligence Pipeline

> **Live Dashboard:** [https://patent-dashboard-5azq.onrender.com](https://patent-dashboard-5azq.onrender.com)
> **Repository:** [https://github.com/asingizwe1/Cloud-Computing.git](https://github.com/asingizwe1/Cloud-Computing.git)

---

## 📌 Project Overview

This project builds a full end-to-end data pipeline that collects, cleans, stores, and analyses real-world patent data from the USPTO PatentsView dataset. It covers **9,434,703 granted patents** spanning **1976 to 2025**, with data on inventors, companies, technology classifications, and filing trends.

The pipeline is built like a real-world data engineering system:

```
Raw TSV Files (USPTO PatentsView)
        ↓
Python Scripts (Data Cleaning & Processing)
        ↓
pandas (Cleaning, Fixing, Organising)
        ↓
SQLite Database (Storage & Querying)
        ↓
SQL Queries + pandas Analysis
        ↓
Reports (CSV, JSON, Console, Charts)
        ↓
Streamlit Dashboard (Interactive Visualisation)
```

---

## 📊 Dataset Statistics

| Metric | Value |
|--------|-------|
| Total Patents | 9,434,703 |
| Unique Inventors | 4,294,034 |
| Unique Companies | 572,495 |
| CPC Classification Records | 59,805,669 |
| Year Range | 1976 – 2025 |

**Data Source:** [USPTO PatentsView Granted Patent Disambiguated Data](https://data.uspto.gov/bulkdata/datasets/pvgpatdis)

---

## 🗂️ Project Structure

```
patent_pipeline/
├── data/
│   ├── raw/                        ← Place TSV files here
│   ├── clean/                      ← Generated clean CSVs
│   └── db/
│       ├── patents.db              ← Full database (local only)
│       └── patents_results.db      ← Lightweight results DB (deployed)
├── scripts/
│   ├── 01_clean_data.py            ← Clean raw TSV files
│   ├── 02_load_database.py         ← Load into SQLite
│   ├── 03_analyze_and_report.py    ← Run all SQL queries + export
│   ├── 04_cpc_analysis.py          ← Technology (CPC) analysis
│   └── 04b_cpc_remaining.py        ← CPC steps 2 & 3 (chunked)
├── sql/
│   ├── schema.sql                  ← Database schema
│   └── queries.sql                 ← All 7 SQL queries documented
├── reports/
│   ├── console_report.txt          ← Terminal output report
│   ├── report.json                 ← JSON structured report
│   ├── top_inventors.csv           ← Top inventors
│   ├── top_companies.csv           ← Top companies
│   ├── country_trends.csv          ← Patents by country
│   ├── yearly_trends.csv           ← Patents per year
│   ├── inventor_rankings.csv       ← Window function rankings
│   ├── china_vs_usa.csv            ← USA vs China race data
│   ├── innovation_velocity.csv     ← Company acceleration data
│   ├── technology_trends.csv       ← Tech area trends
│   ├── chart_top_inventors.png
│   ├── chart_top_companies.png
│   ├── chart_country_share.png
│   ├── chart_yearly_trend.png
│   ├── chart_china_vs_usa.png
│   ├── chart_innovation_velocity.png
│   ├── chart_technology_trends.png
│   ├── chart_technology_share.png
│   └── chart_country_specialisation.png
├── dashboard.py                    ← Streamlit dashboard
├── generate_console_report.py      ← Console report generator
├── run_pipeline.py                 ← Run all scripts at once
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/asingizwe1/Cloud-Computing.git
cd Cloud-Computing/patent_pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Download the data

Download these TSV files from [PatentsView](https://data.uspto.gov/bulkdata/datasets/pvgpatdis) and place them in `data/raw/`:

| File | Description |
|------|-------------|
| `g_patent.tsv` | Core patent records |
| `g_patent_abstract.tsv` | Patent abstracts |
| `g_application.tsv` | Filing dates |
| `g_inventor_disambiguated.tsv` | Inventor records |
| `g_assignee_disambiguated.tsv` | Company/assignee records |
| `g_location_disambiguated.tsv` | Location data |
| `g_cpc_current.tsv` | Technology classifications |

---

## 🚀 Running the Pipeline

Run each script in order:

```bash
python scripts/01_clean_data.py
python scripts/02_load_database.py
python scripts/03_analyze_and_report.py
python scripts/04_cpc_analysis.py
```

Or all at once:

```bash
python run_pipeline.py
```

---

## 📋 Console Report

To generate the console report:

```bash
python generate_console_report.py
```

Output is printed to terminal and saved to `reports/console_report.txt`.

Example output:
```
=======================================================
          PATENT INTELLIGENCE REPORT
=======================================================
  Total Patents          : 9,434,703
  Total Inventors        : 4,294,034
  Total Companies        : 572,495
  Year Range             : 1976 - 2025

-------------------------------------------------------
  TOP 10 INVENTORS
-------------------------------------------------------
   1. Shunpei Yamazaki               11,583 patents [JP]
   2. ...
```

---

## 🗄️ Database Schema

```sql
patents           -- patent_id, title, abstract, filing_date, year
inventors         -- inventor_id, name, country, city, state
companies         -- company_id, name, country
patent_inventors  -- patent_id, inventor_id, inventor_sequence
patent_companies  -- patent_id, company_id, assignee_sequence
cpc_classifications -- patent_id, cpc_section, technology_area
```

---

## 📈 SQL Queries Implemented

| Query | Description | Technique |
|-------|-------------|-----------|
| Q1 | Top inventors by patent count | GROUP BY + ORDER BY |
| Q2 | Top companies by patent count | GROUP BY + JOIN |
| Q3 | Countries producing most patents | COUNT DISTINCT |
| Q4 | Patents granted per year | GROUP BY year |
| Q5 | Patents with inventor and company | 4-table JOIN |
| Q6 | Top inventor per country | CTE (WITH statement) |
| Q7 | Inventor ranking | Window Functions (RANK, PARTITION BY) |

All queries are documented in `sql/queries.sql`.

---

## 🔍 Unique Insights

Beyond the required queries, this project includes 4 original analytical findings:

### 1. 🚀 Innovation Velocity
Identifies companies *accelerating* their patent filings, not just those with the most patents. Compares 2015–2025 output against 2000–2014 baseline to find emerging innovators traditional rankings miss.

### 2. 🌏 USA vs China Patent Race
Tracks the geopolitical story of innovation — China's patent output was negligible before 2000 but grew dramatically, approaching and in some categories surpassing US levels. The crossover point is annotated on the chart.

### 3. 📡 Technology Area Trends
Uses 59 million CPC classification records to track which technology sectors (Physics, Electricity, Chemistry, Emerging Technologies) are rising or falling across decades — connecting patent data to real-world tech waves like the AI boom.

### 4. 🗺️ Country Technology Specialisation
A heatmap showing what each major patent-producing nation actually specialises in. Germany leads in Mechanical Engineering, South Korea and Taiwan concentrate in Electricity (semiconductors), the US leads in Physics and Electricity.

---

## 📊 Dashboard

The interactive Streamlit dashboard has 6 tabs:

| Tab | Content |
|-----|---------|
| 📈 Trends | Yearly patent trends + USA vs China race |
| 👤 Inventors | Top inventors + window function rankings |
| 🏢 Companies | Top companies by patent count |
| 🌍 Countries | Country share pie + CTE query results |
| ⚡ Innovation Velocity | Fastest accelerating companies |
| 🔬 Technology | Tech trends, share pie, country specialisation heatmap |

**Run locally:**
```bash
streamlit run dashboard.py
```

**Live deployment:** [https://patent-dashboard-5azq.onrender.com](https://patent-dashboard-5azq.onrender.com)

---

## 📤 Reports Generated

| File | Description |
|------|-------------|
| `reports/console_report.txt` | Terminal-style summary report |
| `reports/report.json` | Machine-readable JSON report |
| `reports/top_inventors.csv` | Top 20 inventors |
| `reports/top_companies.csv` | Top 20 companies |
| `reports/country_trends.csv` | Patents by country |
| `reports/yearly_trends.csv` | Patents per year 1976–2025 |
| `reports/inventor_rankings.csv` | Window function rankings |
| `reports/china_vs_usa.csv` | USA vs China annual comparison |
| `reports/innovation_velocity.csv` | Company acceleration ratios |
| `reports/technology_trends.csv` | Tech area trends by year |
| `reports/*.png` | All chart visualisations |

---

## 🛠️ Technologies Used

| Tool | Purpose |
|------|---------|
| Python | Pipeline scripting |
| pandas | Data cleaning and aggregation |
| SQLite + SQLAlchemy | Database storage and querying |
| matplotlib + seaborn | Data visualisation |
| Streamlit | Interactive dashboard |
| GitHub | Version control |
| Render | Cloud deployment |

---

## 🔁 Reproducibility

Anyone can clone this repo and reproduce all results:

```bash
git clone https://github.com/asingizwe1/Cloud-Computing.git
pip install -r requirements.txt
# Download TSV files from PatentsView into data/raw/
python scripts/01_clean_data.py
python scripts/02_load_database.py
python scripts/03_analyze_and_report.py
python scripts/04_cpc_analysis.py
python generate_console_report.py
streamlit run dashboard.py
```

---

*Built with USPTO PatentsView data | Global Patent Intelligence Pipeline*
