-- sql/02_create_dimensions.sql
-- Four dimension tables: Company, Surgeon, Geography, Date
USE OrthoAnalytics;
GO
-- ═══════════════════════════════════════════════════════
-- DIM_COMPANY
-- WHY: Separates company attributes from the fact table.
-- Stryker analysts filter dashboards by company constantly.
-- ═══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dw.dim_company;
CREATE TABLE dw.dim_company (
 company_key INT IDENTITY(1,1) PRIMARY KEY,
 company_name NVARCHAR(100) NOT NULL,
 company_tier NVARCHAR(30), -- 'Large Cap' | 'Mid Size' | 'Niche'
 is_stryker BIT DEFAULT 0, -- 1 for Stryker rows — fast filter in DAX
 is_competitor BIT DEFAULT 0 -- 1 for all competitors
);
GO
INSERT INTO dw.dim_company (company_name, company_tier, is_stryker, is_competitor)
VALUES
 ('Stryker', 'Large Cap', 1, 0),
 ('Zimmer Biomet', 'Large Cap', 0, 1),
 ('J&J MedTech', 'Large Cap', 0, 1),
 ('Smith+Nephew', 'Large Cap', 0, 1),
 ('Arthrex', 'Mid Size', 0, 1),
 ('Wright Medical', 'Mid Size', 0, 1),
 ('Other Ortho', 'Niche', 0, 1);
GO
-- ═══════════════════════════════════════════════════════
-- DIM_SURGEON
-- WHY: Surgeon is the key account entity in ortho sales.
-- Each row = one unique surgeon/physician.
-- ═══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dw.dim_surgeon;
CREATE TABLE dw.dim_surgeon (
 surgeon_key INT IDENTITY(1,1) PRIMARY KEY,
 surgeon_name NVARCHAR(200),
 npi NVARCHAR(20), -- National Provider Identifier (unique per physician)
 specialty NVARCHAR(100),
 city NVARCHAR(100),
 state CHAR(2),
 hospital_name NVARCHAR(300)
);
GO
-- ═══════════════════════════════════════════════════════
-- DIM_GEOGRAPHY
-- WHY: Territory analysis is the #1 use case in this role.
-- Includes simulated Stryker sales districts for realism.
-- ═══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dw.dim_geography;
CREATE TABLE dw.dim_geography (
 geo_key INT IDENTITY(1,1) PRIMARY KEY,
 state CHAR(2) NOT NULL,
 state_name NVARCHAR(50),
 region NVARCHAR(30), -- Northeast | South | Midwest | West
 sales_district NVARCHAR(50) -- Simulated Stryker district name
);
GO
INSERT INTO dw.dim_geography (state, state_name, region, sales_district)
VALUES
('AL','Alabama','South','Southeast'),('AK','Alaska','West','Pacific'),
('AZ','Arizona','West','Mountain West'),('AR','Arkansas','South','South Central'),
('CA','California','West','Pacific'),('CO','Colorado','West','Mountain West'),
('CT','Connecticut','Northeast','New England'),('DE','Delaware','Northeast','MidAtlantic'),
('FL','Florida','South','Southeast'),('GA','Georgia','South','Southeast'),
('HI','Hawaii','West','Pacific'),('ID','Idaho','West','Mountain West'),
('IL','Illinois','Midwest','Great Lakes'),('IN','Indiana','Midwest','Great Lakes'),
('IA','Iowa','Midwest','Plains'),('KS','Kansas','Midwest','Plains'),
('KY','Kentucky','South','Southeast'),('LA','Louisiana','South','South Central'),
('ME','Maine','Northeast','New England'),('MD','Maryland','Northeast','MidAtlantic'),
('MA','Massachusetts','Northeast','New England'),('MI','Michigan','Midwest','Great
Lakes'),
('MN','Minnesota','Midwest','Plains'),('MS','Mississippi','South','South Central'),
('MO','Missouri','Midwest','Plains'),('MT','Montana','West','Mountain West'),
('NE','Nebraska','Midwest','Plains'),('NV','Nevada','West','Mountain West'),
('NH','New Hampshire','Northeast','New England'),('NJ','New
Jersey','Northeast','Mid-Atlantic'),
('NM','New Mexico','West','Mountain West'),('NY','New York','Northeast','MidAtlantic'),
('NC','North Carolina','South','Southeast'),('ND','North
Dakota','Midwest','Plains'),
('OH','Ohio','Midwest','Great Lakes'),('OK','Oklahoma','South','South Central'),
('OR','Oregon','West','Pacific'),('PA','Pennsylvania','Northeast','Mid-Atlantic'),
('RI','Rhode Island','Northeast','New England'),('SC','South
Carolina','South','Southeast'),
('SD','South Dakota','Midwest','Plains'),('TN','Tennessee','South','Southeast'),
('TX','Texas','South','South Central'),('UT','Utah','West','Mountain West'),
('VT','Vermont','Northeast','New England'),('VA','Virginia','South','Mid-Atlantic'),
('WA','Washington','West','Pacific'),('WV','West Virginia','South','Mid-Atlantic'),
('WI','Wisconsin','Midwest','Great Lakes'),('WY','Wyoming','West','Mountain West');
GO
-- ═══════════════════════════════════════════════════════
-- DIM_DATE
-- WHY: Power BI DAX time intelligence functions
-- (TOTALYTD, SAMEPERIODLASTYEAR) REQUIRE a proper date
-- table with continuous dates and no gaps.
-- ═══════════════════════════════════════════════════════
DROP TABLE IF EXISTS dw.dim_date;
CREATE TABLE dw.dim_date (
 date_key INT PRIMARY KEY, -- YYYYMMDD integer — fast join key
 full_date DATE NOT NULL,
 year INT,
 quarter INT,
 quarter_name NVARCHAR(8), -- 'Q1 2023'
 month INT,
 month_name NVARCHAR(12), -- 'January'
 month_short NVARCHAR(4), -- 'Jan'
 week_of_year INT,
 day_of_week INT, -- 1=Sunday, 7=Saturday
 is_weekday BIT
);
GO
-- Populate dim_date from 2018 through 2025
-- MAXRECURSION 3000 is needed to handle 7+ years of daily dates
WITH date_spine AS (
 SELECT CAST('2018-01-01' AS DATE) AS d
 UNION ALL
 SELECT DATEADD(DAY, 1, d)
 FROM date_spine
 WHERE d < '2025-12-31'
)
INSERT INTO dw.dim_date
SELECT
 CAST(FORMAT(d, 'yyyyMMdd') AS INT) AS date_key,
 d AS full_date,
 YEAR(d) AS year,
 DATEPART(QUARTER, d) AS quarter,
 CONCAT('Q', DATEPART(QUARTER,d), ' ', YEAR(d)) AS quarter_name,
 MONTH(d) AS month,
 DATENAME(MONTH, d) AS month_name,
 LEFT(DATENAME(MONTH, d), 3) AS month_short,
 DATEPART(WEEK, d) AS week_of_year,
 DATEPART(WEEKDAY, d) AS day_of_week,
 CASE WHEN DATEPART(WEEKDAY,d) IN (1,7)
 THEN 0 ELSE 1 END AS is_weekday
FROM date_spine
OPTION (MAXRECURSION 3000);
GO
-- Verify all dimension tables
SELECT 'dim_company' AS tbl, COUNT(*) AS rows FROM dw.dim_company
UNION ALL
SELECT 'dim_geography' AS tbl, COUNT(*) AS rows FROM dw.dim_geography
UNION ALL
SELECT 'dim_date' AS tbl, COUNT(*) AS rows FROM dw.dim_date;
-- Expected: dim_company=7, dim_geography=50, dim_date=~2922
