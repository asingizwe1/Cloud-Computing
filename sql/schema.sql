-- schema.sql
-- SQLite schema for the Patent Intelligence Pipeline
-- This is auto-created by 02_load_database.py, but documented here for reference.

CREATE TABLE IF NOT EXISTS patents (
    patent_id    TEXT PRIMARY KEY,
    title        TEXT,
    abstract     TEXT,
    filing_date  TEXT,
    grant_date   TEXT,
    year         INTEGER,
    patent_type  TEXT,
    num_claims   INTEGER
);

CREATE TABLE IF NOT EXISTS inventors (
    inventor_id  TEXT PRIMARY KEY,
    name         TEXT,
    country      TEXT,
    city         TEXT,
    state        TEXT,
    gender_code  TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    company_id   TEXT PRIMARY KEY,
    name         TEXT,
    assignee_type INTEGER,
    country      TEXT
);

CREATE TABLE IF NOT EXISTS patent_inventors (
    patent_id          TEXT,
    inventor_id        TEXT,
    inventor_sequence  INTEGER,
    PRIMARY KEY (patent_id, inventor_id),
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id)
);

CREATE TABLE IF NOT EXISTS patent_companies (
    patent_id         TEXT,
    company_id        TEXT,
    assignee_sequence INTEGER,
    PRIMARY KEY (patent_id, company_id),
    FOREIGN KEY (patent_id)  REFERENCES patents(patent_id),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_patents_year     ON patents(year);
CREATE INDEX IF NOT EXISTS idx_pi_patent        ON patent_inventors(patent_id);
CREATE INDEX IF NOT EXISTS idx_pi_inventor      ON patent_inventors(inventor_id);
CREATE INDEX IF NOT EXISTS idx_pc_patent        ON patent_companies(patent_id);
CREATE INDEX IF NOT EXISTS idx_pc_company       ON patent_companies(company_id);
