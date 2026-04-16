-- BUSINESS QUESTION: What proportion of Stryker's payments are royalties
-- vs consulting vs speaking?
-- WHY IT MATTERS: Royalties = surgeon designed the implant (highest loyalty).
-- Consulting = advisory board (strong relationship).
-- Speaking = field education (volume signal).

USE OrthoAnalytics;
GO
WITH mix_summary AS (
 SELECT
 c.company_name,
 c.is_stryker,
 f.payment_type,
 f.payment_year,
 SUM(f.payment_usd) AS total_usd,
 COUNT(*) AS record_count,
 COUNT(DISTINCT f.surgeon_key) AS unique_surgeons
 FROM dw.fact_payments f
 JOIN dw.dim_company c ON f.company_key = c.company_key
 WHERE f.payment_year = 2023
 GROUP BY c.company_name, c.is_stryker, f.payment_type, f.payment_year
),

with_share AS (
 SELECT
 *,
 SUM(total_usd) OVER (PARTITION BY company_name) AS company_total,
 ROUND(
 total_usd * 100.0
 / SUM(total_usd) OVER (PARTITION BY company_name),
 1
 ) AS pct_of_company_total,
 RANK() OVER (
 PARTITION BY company_name
 ORDER BY total_usd DESC
 ) AS type_rank_within_company
 FROM mix_summary
)
SELECT
 company_name,
 payment_type,
 total_usd,
 record_count,
 unique_surgeons,
 pct_of_company_total,
 type_rank_within_company
FROM with_share
ORDER BY company_name, total_usd DESC;
-- If Stryker's royalty % exceeds Zimmer Biomet's,
-- that signals deeper surgeon IP lock-in — a key competitive advantage
-- and a selling point in the national sales meeting deck.
