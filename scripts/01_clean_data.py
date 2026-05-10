"""
01_clean_data.py
Reads raw TSV files from data/raw/, cleans them, and saves clean CSVs to data/clean/.
Place your downloaded TSV files in data/raw/ before running.
"""

import os
import pandas as pd
from tqdm import tqdm

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
CLEAN_DIR = os.path.join(BASE_DIR, "data", "clean")
os.makedirs(CLEAN_DIR, exist_ok=True)

CHUNK_SIZE = 100_000  # process large files in chunks


def read_tsv(filename, usecols=None, dtype=None):
    """Read a TSV file in chunks to handle large files."""
    path = os.path.join(RAW_DIR, filename)
    if not os.path.exists(path):
        print(f"  [SKIP] {filename} not found in data/raw/")
        return None
    chunks = []
    print(f"  Reading {filename}...")
    for chunk in tqdm(
        pd.read_csv(path, sep="\t", usecols=usecols, dtype=dtype,
                    low_memory=False, chunksize=CHUNK_SIZE),
        desc=f"    Chunks"
    ):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    print(f"    Loaded {len(df):,} rows")
    return df


# ─────────────────────────────────────────────
# 1. PATENTS  (g_patent + g_patent_abstract + g_application)
# ─────────────────────────────────────────────
def clean_patents():
    print("\n[1/3] Cleaning patents...")

    patent = read_tsv(
        "g_patent.tsv",
        usecols=["patent_id", "patent_type", "patent_date", "patent_title", "num_claims", "withdrawn"],
    )
    abstract = read_tsv("g_patent_abstract.tsv", usecols=["patent_id", "patent_abstract"])
    application = read_tsv("g_application.tsv", usecols=["patent_id", "filing_date"])

    if patent is None:
        print("  ERROR: g_patent.tsv is required. Aborting patents clean.")
        return

    df = patent.copy()

    # Merge abstract
    if abstract is not None:
        df = df.merge(abstract, on="patent_id", how="left")
    else:
        df["patent_abstract"] = None

    # Merge filing date
    if application is not None:
        df = df.merge(application[["patent_id", "filing_date"]], on="patent_id", how="left")
    else:
        df["filing_date"] = None

    # Rename columns
    df.rename(columns={
        "patent_title": "title",
        "patent_abstract": "abstract",
        "patent_date": "grant_date",
    }, inplace=True)

    # Parse dates & extract year
    df["grant_date"] = pd.to_datetime(df["grant_date"], errors="coerce")
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["year"] = df["grant_date"].dt.year

    # Drop withdrawn patents
    df = df[df["withdrawn"] != 1].copy()

    # Drop rows with no patent_id
    df.dropna(subset=["patent_id"], inplace=True)
    df.drop_duplicates(subset=["patent_id"], inplace=True)

    # Select final columns
    df = df[["patent_id", "title", "abstract", "filing_date", "grant_date", "year",
             "patent_type", "num_claims"]]

    out = os.path.join(CLEAN_DIR, "clean_patents.csv")
    df.to_csv(out, index=False)
    print(f"  Saved {len(df):,} patents → {out}")
    return df


# ─────────────────────────────────────────────
# 2. INVENTORS  (g_inventor_disambiguated + g_location_disambiguated)
# ─────────────────────────────────────────────
def clean_inventors():
    print("\n[2/3] Cleaning inventors...")

    inv = read_tsv(
        "g_inventor_disambiguated.tsv",
        usecols=["patent_id", "inventor_id", "inventor_sequence",
                 "disambig_inventor_name_first", "disambig_inventor_name_last",
                 "gender_code", "location_id"],
    )
    loc = read_tsv(
        "g_location_disambiguated.tsv",
        usecols=["location_id", "disambig_city", "disambig_state", "disambig_country"],
    )

    if inv is None:
        print("  ERROR: g_inventor_disambiguated.tsv not found.")
        return None, None

    # Build full name
    inv["name"] = (
        inv["disambig_inventor_name_first"].fillna("") + " " +
        inv["disambig_inventor_name_last"].fillna("")
    ).str.strip()

    # Merge location
    if loc is not None:
        inv = inv.merge(loc, on="location_id", how="left")
        inv.rename(columns={"disambig_country": "country",
                             "disambig_city": "city",
                             "disambig_state": "state"}, inplace=True)
    else:
        inv["country"] = None
        inv["city"] = None
        inv["state"] = None

    inv.dropna(subset=["inventor_id"], inplace=True)

    # Patent–inventor relationship table
    rel_inv = inv[["patent_id", "inventor_id", "inventor_sequence"]].drop_duplicates()

    # Unique inventors table
    inventors = (
        inv[["inventor_id", "name", "country", "city", "state", "gender_code"]]
        .drop_duplicates(subset=["inventor_id"])
    )

    rel_path = os.path.join(CLEAN_DIR, "clean_patent_inventors.csv")
    inv_path = os.path.join(CLEAN_DIR, "clean_inventors.csv")
    rel_inv.to_csv(rel_path, index=False)
    inventors.to_csv(inv_path, index=False)
    print(f"  Saved {len(inventors):,} unique inventors → {inv_path}")
    print(f"  Saved {len(rel_inv):,} patent–inventor links → {rel_path}")
    return inventors, rel_inv


# ─────────────────────────────────────────────
# 3. COMPANIES / ASSIGNEES  (g_assignee_disambiguated + g_location_disambiguated)
# ─────────────────────────────────────────────
def clean_companies():
    print("\n[3/3] Cleaning companies (assignees)...")

    asgn = read_tsv(
        "g_assignee_disambiguated.tsv",
        usecols=["patent_id", "assignee_id", "assignee_sequence",
                 "disambig_assignee_organization",
                 "disambig_assignee_individual_name_first",
                 "disambig_assignee_individual_name_last",
                 "assignee_type", "location_id"],
    )
    loc = read_tsv(
        "g_location_disambiguated.tsv",
        usecols=["location_id", "disambig_country"],
    )

    if asgn is None:
        print("  ERROR: g_assignee_disambiguated.tsv not found.")
        return None, None

    # Build name: prefer organization, fall back to individual
    asgn["name"] = asgn["disambig_assignee_organization"].fillna(
        (asgn["disambig_assignee_individual_name_first"].fillna("") + " " +
         asgn["disambig_assignee_individual_name_last"].fillna("")).str.strip()
    )
    asgn["name"] = asgn["name"].replace("", pd.NA)

    # Merge location
    if loc is not None:
        asgn = asgn.merge(loc, on="location_id", how="left")
        asgn.rename(columns={"disambig_country": "country"}, inplace=True)
    else:
        asgn["country"] = None

    asgn.dropna(subset=["assignee_id"], inplace=True)

    # Patent–company relationship table
    rel_co = asgn[["patent_id", "assignee_id", "assignee_sequence"]].drop_duplicates()
    rel_co.rename(columns={"assignee_id": "company_id"}, inplace=True)

    # Unique companies table
    companies = (
        asgn[["assignee_id", "name", "assignee_type", "country"]]
        .rename(columns={"assignee_id": "company_id"})
        .drop_duplicates(subset=["company_id"])
    )

    rel_path = os.path.join(CLEAN_DIR, "clean_patent_companies.csv")
    co_path = os.path.join(CLEAN_DIR, "clean_companies.csv")
    rel_co.to_csv(rel_path, index=False)
    companies.to_csv(co_path, index=False)
    print(f"  Saved {len(companies):,} unique companies → {co_path}")
    print(f"  Saved {len(rel_co):,} patent–company links → {rel_path}")
    return companies, rel_co


# ─────────────────────────────────────────────
# 4. CPC CLASSIFICATIONS  (g_cpc_current.tsv)
# ─────────────────────────────────────────────
def clean_cpc():
    print("\n[4/4] Cleaning CPC classifications...")

    cpc = read_tsv(
        "g_cpc_current.tsv",
        usecols=["patent_id", "cpc_sequence", "cpc_section",
                 "cpc_class", "cpc_subclass", "cpc_group", "cpc_type"],
    )

    if cpc is None:
        print("  ERROR: g_cpc_current.tsv not found.")
        return None

    cpc.dropna(subset=["patent_id", "cpc_section"], inplace=True)

    # Map section codes to human-readable technology areas
    section_map = {
        "A": "Human Necessities",
        "B": "Performing Operations / Transporting",
        "C": "Chemistry / Metallurgy",
        "D": "Textiles / Paper",
        "E": "Fixed Constructions",
        "F": "Mechanical Engineering",
        "G": "Physics",
        "H": "Electricity",
        "Y": "Emerging Technologies",
    }
    cpc["technology_area"] = cpc["cpc_section"].map(section_map).fillna("Other")

    out = os.path.join(CLEAN_DIR, "clean_cpc.csv")
    cpc.to_csv(out, index=False)
    print(f"  Saved {len(cpc):,} CPC records → {out}")
    return cpc

if __name__ == "__main__":
    print("=" * 55)
    print("  STEP 1: DATA CLEANING")
    print("=" * 55)
    clean_patents()
    clean_inventors()
    clean_companies()
    clean_cpc()
    print("\nDone! Check data/clean/ for output files.")


