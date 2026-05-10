"""
02_load_database.py
Loads clean CSVs from data/clean/ into a SQLite database at data/db/patents.db.
Run AFTER 01_clean_data.py.
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
DB_DIR = os.path.join(BASE_DIR, "data", "db")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "patents.db")

ENGINE = create_engine(f"sqlite:///{DB_PATH}", echo=False)

CHUNK = 50_000


def load_csv(filename, table_name, if_exists="replace", dtype=None):
    path = os.path.join(CLEAN_DIR, filename)
    if not os.path.exists(path):
        print(f"  [SKIP] {filename} not found.")
        return 0

    print(f"  Loading {filename} → table '{table_name}'...")
    first = True
    total = 0
    for chunk in tqdm(
        pd.read_csv(path, dtype=dtype, low_memory=False, chunksize=CHUNK),
        desc="    Chunks"
    ):
        chunk.to_sql(
            table_name,
            ENGINE,
            if_exists=if_exists if first else "append",
            index=False,
        )
        first = False
        total += len(chunk)
    print(f"    {total:,} rows inserted.")
    return total


def create_indexes():
    print("\n  Creating indexes for fast queries...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_patents_year ON patents(year);",
        "CREATE INDEX IF NOT EXISTS idx_patents_id ON patents(patent_id);",
        "CREATE INDEX IF NOT EXISTS idx_inventors_id ON inventors(inventor_id);",
        "CREATE INDEX IF NOT EXISTS idx_companies_id ON companies(company_id);",
        "CREATE INDEX IF NOT EXISTS idx_pi_patent ON patent_inventors(patent_id);",
        "CREATE INDEX IF NOT EXISTS idx_pi_inventor ON patent_inventors(inventor_id);",
        "CREATE INDEX IF NOT EXISTS idx_pc_patent ON patent_companies(patent_id);",
        "CREATE INDEX IF NOT EXISTS idx_pc_company ON patent_companies(company_id);",
        "CREATE INDEX IF NOT EXISTS idx_cpc_patent ON cpc_classifications(patent_id);",
        "CREATE INDEX IF NOT EXISTS idx_cpc_section ON cpc_classifications(cpc_section);",
    ]
    with ENGINE.connect() as conn:
        for sql in indexes:
            conn.execute(text(sql))
        conn.commit()
    print("    Indexes created.")


if __name__ == "__main__":
    print("=" * 55)
    print("  STEP 2: LOAD INTO SQLite DATABASE")
    print("=" * 55)
    print(f"  Database: {DB_PATH}\n")

    load_csv("clean_patents.csv", "patents")
    load_csv("clean_inventors.csv", "inventors")
    load_csv("clean_companies.csv", "companies")
    load_csv("clean_patent_inventors.csv", "patent_inventors")
    load_csv("clean_patent_companies.csv", "patent_companies")

    create_indexes()

    print("\nDone! Database ready at data/db/patents.db")
