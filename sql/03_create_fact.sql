USE OrthoAnalytics;
GO
-- ═══════════════════════════════════════════════════════
-- FACT_PAYMENTS
-- One row per payment record.
-- All foreign keys reference the dimension tables.
-- Numeric measures (payment_usd) live here for aggregation.
-- ═══════════════════════════════════════════════════════
CREATE TABLE dw.fact_payments (
 payment_key BIGINT IDENTITY(1,1) PRIMARY KEY,
 company_key INT NOT NULL,
 surgeon_key INT NOT NULL,
 geo_key INT NOT NULL,
 date_key INT NOT NULL,
 payment_usd DECIMAL(14,2) NOT NULL,
 payment_type NVARCHAR(50),
 payment_year INT,
 payment_month INT,
 payment_quarter INT,
 device_name NVARCHAR(300),
 record_id NVARCHAR(50),

-- Foreign key constraints enforce referential integrity
 CONSTRAINT FK_pay_company FOREIGN KEY (company_key) REFERENCES
dw.dim_company(company_key),
 CONSTRAINT FK_pay_surgeon FOREIGN KEY (surgeon_key) REFERENCES
dw.dim_surgeon(surgeon_key),
 CONSTRAINT FK_pay_geo FOREIGN KEY (geo_key) REFERENCES
dw.dim_geography(geo_key),
 CONSTRAINT FK_pay_date FOREIGN KEY (date_key) REFERENCES
dw.dim_date(date_key),
 -- Performance indexes for common filter patterns
 INDEX IX_fact_company (company_key),
 INDEX IX_fact_surgeon (surgeon_key),
 INDEX IX_fact_geo (geo_key),
 INDEX IX_fact_date (date_key),
 INDEX IX_fact_year (payment_year)
);
GO
PRINT 'fact_payments table created successfully.';