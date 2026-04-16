-- BUSINESS QUESTION: How are monthly payments trending for Stryker vs
-- competitors? Is the gap widening or narrowing?
-- WINDOW FUNCTIONS: Rolling AVG, LAG for MoM and YoY calculations.

USE OrthoAnalytics;
GO
WITH monthly_base AS (
 SELECT
 d.year,
 d.month,
 d.month_name,
 d.month_short,
 d.quarter_name,
 DATEFROMPARTS(d.year, d.month, 1) AS month_start,
 c.company_name,
 c.is_stryker,
 SUM(f.payment_usd) AS monthly_total,
 COUNT(DISTINCT f.surgeon_key) AS active_surgeons,
 COUNT(*) AS payment_records
 FROM dw.fact_payments f
 JOIN dw.dim_date d ON f.date_key = d.date_key
 JOIN dw.dim_company c ON f.company_key = c.company_key
 WHERE d.year BETWEEN 2021 AND 2023
 GROUP BY d.year, d.month, d.month_name, d.month_short,
 d.quarter_name,
 DATEFROMPARTS(d.year, d.month, 1),
 c.company_name, c.is_stryker
),
with_calculations AS (
 SELECT
 *,
 -- Rolling 12-month average (smooths seasonal noise)
 AVG(monthly_total) OVER (
 PARTITION BY company_name
 ORDER BY month_start
 ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
 ) AS rolling_12mo_avg,
 -- Month-over-month change (previous month, same company)
 LAG(monthly_total) OVER (
 PARTITION BY company_name
 ORDER BY month_start
 ) AS prior_month_total,
 -- Year-over-year (same calendar month, prior year)
 LAG(monthly_total, 12) OVER (
 PARTITION BY company_name
 ORDER BY month_start
 ) AS same_month_prior_year
 FROM monthly_base
)

SELECT
 year, month, month_name, month_short, quarter_name,
 month_start, company_name, is_stryker,
 monthly_total,
 rolling_12mo_avg,
 active_surgeons,
 -- MoM growth %
 CASE WHEN prior_month_total > 0
 THEN ROUND((monthly_total - prior_month_total)
 / prior_month_total * 100, 1)
 END AS mom_growth_pct,
 -- YoY growth %
 CASE WHEN same_month_prior_year > 0
 THEN ROUND((monthly_total - same_month_prior_year)
 / same_month_prior_year * 100, 1)
 END AS yoy_growth_pct
FROM with_calculations
ORDER BY company_name, month_start;