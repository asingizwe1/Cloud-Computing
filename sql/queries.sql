-- queries.sql
-- All analytical SQL queries for the Patent Intelligence Pipeline
-- These are run automatically by 03_analyze_and_report.py
-- but you can also run them manually in any SQLite client (e.g., DB Browser for SQLite)

-- ═══════════════════════════════════════════════════════
-- Q1: TOP INVENTORS – who has the most patents?
-- ═══════════════════════════════════════════════════════
SELECT
    i.inventor_id,
    i.name,
    i.country,
    COUNT(pi.patent_id) AS patent_count
FROM inventors i
JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
GROUP BY i.inventor_id, i.name, i.country
ORDER BY patent_count DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════
-- Q2: TOP COMPANIES – which companies own the most patents?
-- ═══════════════════════════════════════════════════════
SELECT
    c.company_id,
    c.name,
    c.country,
    COUNT(pc.patent_id) AS patent_count
FROM companies c
JOIN patent_companies pc ON c.company_id = pc.company_id
WHERE c.name IS NOT NULL
GROUP BY c.company_id, c.name, c.country
ORDER BY patent_count DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════
-- Q3: COUNTRIES – which produce the most patents?
-- ═══════════════════════════════════════════════════════
SELECT
    i.country,
    COUNT(DISTINCT pi.patent_id) AS patent_count
FROM inventors i
JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
WHERE i.country IS NOT NULL AND i.country != ''
GROUP BY i.country
ORDER BY patent_count DESC
LIMIT 30;


-- ═══════════════════════════════════════════════════════
-- Q4: TRENDS OVER TIME – patents granted per year
-- ═══════════════════════════════════════════════════════
SELECT
    year,
    COUNT(*) AS patent_count
FROM patents
WHERE year IS NOT NULL AND year >= 1976 AND year <= 2025
GROUP BY year
ORDER BY year;


-- ═══════════════════════════════════════════════════════
-- Q5: JOIN – patents combined with inventor and company info
-- ═══════════════════════════════════════════════════════
SELECT
    p.patent_id,
    p.title,
    p.year,
    i.name        AS inventor_name,
    i.country     AS inventor_country,
    c.name        AS company_name
FROM patents p
LEFT JOIN patent_inventors pi ON p.patent_id = pi.patent_id
                              AND pi.inventor_sequence = 0
LEFT JOIN inventors i         ON pi.inventor_id = i.inventor_id
LEFT JOIN patent_companies pc ON p.patent_id = pc.patent_id
                              AND pc.assignee_sequence = 0
LEFT JOIN companies c         ON pc.company_id = c.company_id
LIMIT 1000;


-- ═══════════════════════════════════════════════════════
-- Q6: CTE – top inventor per country (WITH statement)
-- ═══════════════════════════════════════════════════════
WITH inventor_counts AS (
    -- Step 1: count patents per inventor
    SELECT
        i.inventor_id,
        i.name,
        i.country,
        COUNT(pi.patent_id) AS patent_count
    FROM inventors i
    JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
    WHERE i.country IS NOT NULL
    GROUP BY i.inventor_id, i.name, i.country
),
country_totals AS (
    -- Step 2: sum up totals per country
    SELECT
        country,
        SUM(patent_count) AS country_total
    FROM inventor_counts
    GROUP BY country
)
-- Step 3: find the top inventor per country
SELECT
    ic.country,
    ct.country_total,
    ic.name          AS top_inventor,
    ic.patent_count  AS inventor_patents
FROM inventor_counts ic
JOIN country_totals ct ON ic.country = ct.country
WHERE ic.patent_count = (
    SELECT MAX(patent_count)
    FROM inventor_counts ic2
    WHERE ic2.country = ic.country
)
ORDER BY ct.country_total DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════
-- Q7: RANKING – rank inventors using window functions
-- ═══════════════════════════════════════════════════════
SELECT
    inventor_id,
    name,
    country,
    patent_count,
    RANK() OVER (ORDER BY patent_count DESC)                            AS overall_rank,
    RANK() OVER (PARTITION BY country ORDER BY patent_count DESC)       AS rank_in_country,
    ROUND(100.0 * patent_count / SUM(patent_count) OVER (), 4)         AS pct_of_total
FROM (
    SELECT
        i.inventor_id,
        i.name,
        i.country,
        COUNT(pi.patent_id) AS patent_count
    FROM inventors i
    JOIN patent_inventors pi ON i.inventor_id = pi.inventor_id
    WHERE i.country IS NOT NULL
    GROUP BY i.inventor_id, i.name, i.country
) sub
ORDER BY overall_rank
LIMIT 50;
