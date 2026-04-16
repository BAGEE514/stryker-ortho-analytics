CREATE DATABASE OrthoAnalytics;

USE OrthoAnalytics;
-- Check row counts by company
SELECT company_name, COUNT(*) AS records,
 SUM(payment_usd) AS total_usd,
 MIN(payment_year) AS earliest_year,
 MAX(payment_year) AS latest_year
FROM raw.raw_payments
GROUP BY company_name
ORDER BY total_usd DESC;
-- Check for nulls in key columns
SELECT
 SUM(CASE WHEN company_name IS NULL THEN 1 ELSE 0 END) AS null_company,
 SUM(CASE WHEN payment_usd IS NULL THEN 1 ELSE 0 END) AS null_usd,
 SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END) AS null_state,
 SUM(CASE WHEN payment_date IS NULL THEN 1 ELSE 0 END) AS null_date
FROM raw.raw_payments;
-- All null counts should be 0 after our cleaning step
