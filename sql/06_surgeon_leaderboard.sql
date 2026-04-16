-- BUSINESS QUESTION: Who are Stryker's most valuable surgeon relationships?
-- core 'account performance' view used in every med-device company.
-- top surgeon slide fixture of every national sales meeting deck.
-- WINDOW FUNCTIONS: RANK(), PERCENT_RANK() for percentile scoring.
USE OrthoAnalytics;
GO
WITH surgeon_summary AS (
 SELECT
 s.surgeon_name,
 s.specialty,
 s.state,
 s.city,
 s.hospital_name,
 g.region,
 g.sales_district,
 c.company_name,
 f.payment_year,
 -- Aggregated metrics
 SUM(f.payment_usd) AS total_paid,
 COUNT(*) AS payment_count,
 COUNT(DISTINCT f.payment_type) AS payment_type_count,
 MAX(f.payment_usd) AS largest_single_payment,
 MIN(d.full_date) AS first_payment_date,
 MAX(d.full_date) AS most_recent_payment,
DATEDIFF(MONTH,
    MIN(d.full_date),
    MAX(d.full_date)) AS relationship_months
 FROM dw.fact_payments f
 JOIN dw.dim_surgeon s ON f.surgeon_key = s.surgeon_key
 JOIN dw.dim_geography g ON f.geo_key = g.geo_key
 JOIN dw.dim_company c ON f.company_key = c.company_key
 JOIN dw.dim_date d ON f.date_key = d.date_key
 WHERE f.payment_year = 2023
 GROUP BY s.surgeon_name, s.specialty, s.state, s.city,
 s.hospital_name, g.region, g.sales_district,
 c.company_name, f.payment_year
),
ranked AS (
 SELECT
 *,
 RANK() OVER (
 PARTITION BY company_name
 ORDER BY total_paid DESC
 ) AS national_rank,

  -- Percentile: 1.0 = top, 0.0 = bottom of all surgeons for that company
 PERCENT_RANK() OVER (
 PARTITION BY company_name
 ORDER BY total_paid
 ) AS percentile_score
 FROM surgeon_summary
)
SELECT
 national_rank,
 surgeon_name,
 specialty,
 state,
 city,
 hospital_name,
 sales_district,
 company_name,
 FORMAT(total_paid, 'C0') AS total_paid_fmt,
 payment_count,
 payment_type_count,
 FORMAT(largest_single_payment, 'C0') AS largest_payment_fmt,
 relationship_months,
 ROUND(percentile_score * 100, 1) AS pct_rank
FROM ranked
WHERE company_name = 'Stryker'
 AND national_rank <= 25
ORDER BY national_rank;
-- INSIGHT TO PRESENT: Top 25 surgeons typically account for 15-25% of
-- total Stryker payments — classic Pareto concentration.
-- Flag highest royalty-earning surgeons as key IP relationship holders.
