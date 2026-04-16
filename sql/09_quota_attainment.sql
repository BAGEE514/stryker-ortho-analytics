-- BUSINESS QUESTION: Which districts are above/below quota?
-- Who qualifies for President's Club recognition?


USE OrthoAnalytics;
GO
WITH annual_totals AS (
 SELECT
 g.sales_district,
 g.region,
 c.company_name,
 f.payment_year,
 SUM(f.payment_usd) AS annual_total,
 COUNT(DISTINCT f.surgeon_key) AS surgeons,
 COUNT(DISTINCT f.geo_key) AS states_active
 FROM dw.fact_payments f
 JOIN dw.dim_geography g ON f.geo_key = g.geo_key
 JOIN dw.dim_company c ON f.company_key = c.company_key
 WHERE f.payment_year IN (2022, 2023)
 GROUP BY g.sales_district, g.region, c.company_name, f.payment_year
),

quota_model AS (
-- Apply 8% growth target over prior year as the quota
-- This mirrors how most med-device companies set annual quotas
 SELECT
 curr.sales_district,
 curr.region,
 curr.company_name,
 curr.annual_total AS actuals_2023,
 prior.annual_total AS baseline_2022,
 ROUND(prior.annual_total * 1.08, 0) AS quota_2023, -- 8% growth target
 curr.surgeons AS surgeons_2023,
 prior.surgeons AS surgeons_2022
 FROM annual_totals curr
 JOIN annual_totals prior
 ON curr.sales_district = prior.sales_district
 AND curr.company_name = prior.company_name
 AND curr.payment_year = 2023
 AND prior.payment_year = 2022
)
SELECT
 sales_district,
 region,
 company_name,
 actuals_2023,
 quota_2023,
 -- Attainment %: actual / quota
 ROUND(actuals_2023 * 100.0 / NULLIF(quota_2023, 0), 1) AS attainment_pct,
 -- Dollar gap: positive = above quota, negative = below
 actuals_2023 - quota_2023 AS gap_to_quota,
 surgeons_2023,
 surgeons_2022,
 surgeons_2023 - surgeons_2022 AS surgeon_net_add,

-- Performance tier for recognition
 CASE
 WHEN actuals_2023 >= quota_2023 * 1.10 THEN 'President''s Club (110%+)'
 WHEN actuals_2023 >= quota_2023 THEN 'Quota Achieved (100–109%)'
 WHEN actuals_2023 >= quota_2023 * 0.90 THEN 'Near Miss (90–99%)'
 ELSE 'Below Target (<90%)'
 END AS performance_tier
FROM quota_model
WHERE company_name = 'Stryker'
ORDER BY attainment_pct DESC;

