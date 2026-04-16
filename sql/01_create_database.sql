-- Step 2: Switch to the new database
USE OrthoAnalytics;
GO
-- Step 3: Create schemas (namespaces for organizing tables)
-- raw = data as loaded by Python (messy, source of truth)
-- dw = data warehouse (clean star schema — what Power BI connects to)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'raw')
 EXEC('CREATE SCHEMA raw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
 EXEC('CREATE SCHEMA dw');
GO
-- Verify schemas were created
SELECT name AS schema_name FROM sys.schemas
WHERE name IN ('raw','dw')
ORDER BY name;
-- Expected output: dw, raw