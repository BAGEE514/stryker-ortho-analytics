-- Populate dimension and fact tables from raw ingested data.
-- Run AFTER 02_create_dimensions.sql and 03_create_fact.sql.
USE OrthoAnalytics;
GO

-- ─── Step 1: Populate dim_surgeon ─────────────────────────────────────
-- Extract distinct surgeon records from raw data
PRINT 'Step 1: Populating dim_surgeon...';
DELETE FROM dw.dim_surgeon;
INSERT INTO dw.dim_surgeon (surgeon_name, npi, specialty, city, state,
hospital_name)
SELECT DISTINCT
 surgeon_name,
 npi,
 specialty,
 city,
 state,
 hospital_name
FROM raw.raw_payments
WHERE surgeon_name IS NOT NULL
 AND LEN(LTRIM(RTRIM(surgeon_name))) > 0
 AND state IN (SELECT state FROM dw.dim_geography)
ORDER BY surgeon_name;
DECLARE @surgeon_count INT = (SELECT COUNT(*) FROM dw.dim_surgeon);

PRINT CONCAT(' Loaded ', @surgeon_count, ' unique surgeons.');
-- ─── Step 2: Populate fact_payments ───────────────────────────────────
PRINT 'Step 2: Populating fact_payments...';
DELETE FROM dw.fact_payments;
INSERT INTO dw.fact_payments (
 company_key, surgeon_key, geo_key, date_key,
 payment_usd, payment_type, payment_year, payment_month,
 payment_quarter, device_name, record_id
)
SELECT
 c.company_key,
 s.surgeon_key,
 g.geo_key,
 d.date_key,
 r.payment_usd,
 r.payment_type,
 r.payment_year,
 r.payment_month,
 r.payment_quarter,
 r.device_name,
 r.Record_ID
FROM raw.raw_payments r
-- JOIN to each dimension — rows that can't be matched get excluded
JOIN dw.dim_company c ON r.company_name = c.company_name
JOIN dw.dim_surgeon s ON r.surgeon_name = s.surgeon_name
 AND r.state = s.state
JOIN dw.dim_geography g ON r.state = g.state
JOIN dw.dim_date d ON CAST(FORMAT(CAST(r.payment_date AS DATE),'yyyyMMdd') AS
INT) = d.date_key
WHERE r.payment_usd > 0;
DECLARE @fact_count INT = (SELECT COUNT(*) FROM dw.fact_payments);
PRINT CONCAT(' Loaded ', @fact_count, ' payment records.');
GO

-- ─── Step 3: Validation — ALWAYS run this after any ETL load ──────────
PRINT 'Step 3: Validation...';
SELECT 'raw.raw_payments' AS source, COUNT(*) AS rows FROM raw.raw_payments
UNION ALL
SELECT 'dw.fact_payments' AS source, COUNT(*) AS rows FROM dw.fact_payments
UNION ALL
SELECT 'dw.dim_surgeon' AS source, COUNT(*) AS rows FROM dw.dim_surgeon;
-- Check: raw rows that DID NOT make it into the fact table (unmatched joins)
SELECT
 COUNT(*) AS unmatched_raw_rows,
 ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM raw.raw_payments), 1) AS
unmatched_pct
FROM raw.raw_payments r
WHERE NOT EXISTS (
 SELECT 1 FROM dw.fact_payments f
 WHERE f.record_id = r.Record_ID
);

-- If unmatched_pct > 10%, investigate which join is dropping rows
-- (most common cause: company names that don't match dim_company exactly)
-- Quick sanity check: totals by company
SELECT
 c.company_name,
 COUNT(*) AS records,
 SUM(f.payment_usd) AS total_usd,
 COUNT(DISTINCT f.surgeon_key) AS unique_surgeons
FROM dw.fact_payments f
JOIN dw.dim_company c ON f.company_key = c.company_key
GROUP BY c.company_name
ORDER BY total_usd DESC;