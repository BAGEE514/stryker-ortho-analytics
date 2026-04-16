-- BUSINESS QUESTION: Which states and districts are growing vs declining
-- for Stryker vs each competitor?
-- OUTPUT: District-level performance with YoY growth — for exec quarterly review.
-- WINDOW FUNCTIONS USED: LAG() for prior year, RANK() for district ranking.
USE OrthoAnalytics;
GO
WITH district_annual AS (
-- Step 1: Aggregate payments by district + company + year
 SELECT
 g.sales_district,
 g.region,
 g.state,
 c.company_name,
 c.is_stryker,
 f.payment_year,
 SUM(f.payment_usd) AS total_payments,
 COUNT(DISTINCT f.surgeon_key) AS unique_surgeons,
 COUNT(*) AS payment_records,
 AVG(f.payment_usd) AS avg_payment_size
 FROM dw.fact_payments f
 JOIN dw.dim_geography g ON f.geo_key = g.geo_key
 JOIN dw.dim_company c ON f.company_key = c.company_key
 WHERE f.payment_year IN (2022, 2023)
 GROUP BY g.sales_district, g.region, g.state,
 c.company_name, c.is_stryker, f.payment_year
),
with_prior_year AS (
-- Step 2: Use LAG to pull the prior year value for YoY math
 SELECT
 *,
 LAG(total_payments) OVER (
 PARTITION BY sales_district, company_name
 ORDER BY payment_year
 ) AS prior_year_payments,
 LAG(unique_surgeons) OVER (
 PARTITION BY sales_district, company_name
 ORDER BY payment_year
 ) AS prior_year_surgeons
 FROM district_annual
),
final AS (
-- Step 3: Calculate growth rates and rank
 SELECT
 sales_district,
 region,
 state,
 company_name,
 is_stryker,
 payment_year,
 total_payments,
 unique_surgeons,
payment_records,
 prior_year_payments,
 -- YoY growth %
 CASE WHEN prior_year_payments > 0
 THEN ROUND((total_payments - prior_year_payments)
 / prior_year_payments * 100, 1)
 ELSE NULL
 END AS yoy_growth_pct,
 -- Absolute dollar change
 total_payments - prior_year_payments AS yoy_growth_dollars,
 -- Rank within company (highest payments = rank 1)
 RANK() OVER (
 PARTITION BY payment_year, company_name
 ORDER BY total_payments DESC
 ) AS district_rank
 FROM with_prior_year
 WHERE payment_year = 2023 -- show current year only
)
SELECT * 
FROM final
ORDER BY is_stryker DESC, total_payments DESC;
-- HOW TO PRESENT THIS: Flag districts where Stryker YoY growth < 0
-- while competitors are positive — those are market share loss signals.
-- Highlight top 3 Stryker districts for the leaderboard slide.