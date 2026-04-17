# stryker-ortho-analytics
# Ortho Joint Replacement — Field Sales Analytics
**Built to mirror Stryker's Ortho Analytics function | CMS Open Payments Data**

---

## Business Problem
Medical device sales analytics teams need to answer:
- Which territories and sales districts are growing vs declining YoY?
- Which surgeons represent the highest-value Stryker relationships?
- Is Stryker gaining or losing market share vs Zimmer Biomet, J&J, and Smith+Nephew?
- Which districts are below quota heading into the national sales meeting?

This project builds a complete analytics pipeline answering these questions
using CMS Open Payments data — the U.S. government's public disclosure of
all payments medical device companies made to surgeons and hospitals.

---

## Architecture
```
CMS Open Payments CSV (22GB raw — 3 years)
  → Python ingest.py (filter + clean → SQL Server raw schema)
  → T-SQL ETL (star schema: 1 fact + 4 dims)
  → Power BI (4-page executive dashboard, 13 DAX measures)
  → Microsoft Fabric (Lakehouse + Dataflow Gen2 + Direct Lake)
```

---

## Key Findings
1. **Total Market**: $1.61B in orthopedic payments across 503,337 records (2021–2023)
2. **Top Company**: Zimmer Biomet led 2023 payments at $171M, followed by Arthrex at $129M and Stryker at $115M
3. **Surgeon Concentration**: Top surgeon (James Bono) received $976K from Stryker — classic Pareto concentration among top 25 accounts
4. **Data Quality**: 0.6% unmatched rows after ETL — well within acceptable threshold
5. **Geographic Coverage**: 56 states/territories represented across 32,067 unique surgeons

---

## Data
| Metric | Value |
|---|---|
| Raw records processed | 503,337 |
| Fact table rows loaded | 664,829 |
| Unique surgeons | 32,067 |
| Companies tracked | 6 |
| Years covered | 2021–2023 |
| Total payment value | $1.61B |

---

## Star Schema
```
                    dim_company
                         │
dim_date ── fact_payments ── dim_surgeon
                         │
                    dim_geography
```

- **fact_payments** — 664,829 rows, one per payment record
- **dim_company** — 7 companies (Stryker, Zimmer Biomet, J&J MedTech, Smith+Nephew, Arthrex, Wright Medical, Other Ortho)
- **dim_surgeon** — 32,067 unique surgeons with NPI, specialty, city, state
- **dim_geography** — 50 states with region and simulated Stryker sales district
- **dim_date** — 2,922 daily dates (2018–2025) for DAX time intelligence

---

## SQL Analysis Queries
| File | Business Question |
|---|---|
| `05_territory_scorecard.sql` | YoY growth by state and sales district using LAG() window function |
| `06_surgeon_leaderboard.sql` | Top 25 surgeons by Stryker relationship value using RANK() and PERCENT_RANK() |
| `07_monthly_trend.sql` | Monthly trend with rolling 12-month average using AVG() OVER() |
| `08_payment_mix.sql` | Royalty vs Consulting vs Speaking payment type breakdown |
| `09_quota_attainment.sql` | Simulated 8% growth quota model with President's Club tiers |

---

## Power BI Dashboard
**4 pages | 13 DAX measures | Star schema model**

- **Page 1 — Executive Scorecard**: KPI cards (Total Payments, YoY Growth %, Unique Surgeons, Market Share %), company bar chart, year and company slicers
- **Page 2 — Territory Map**: Bubble map sized by Total Payments, district performance table with YoY Growth % and Attainment %, region slicer
- **Page 3 — Surgeon Leaderboard**: Top surgeon table sorted by Total Payments, payment type donut chart, company and year slicers
- **Page 4 — Trend Analysis**: Monthly line chart by company, district attainment column chart with green/red conditional formatting and 100% quota reference line

**DAX Measures:**
`Total Payments` | `Payment Records` | `Unique Surgeons` | `YTD Payments` | `Prior Year Payments` | `YoY Growth $` | `YoY Growth %` | `Rolling 12 Month Avg` | `Quota (8% Growth)` | `Attainment %` | `Gap to Quota` | `Stryker Market Share %` | `Surgeon Rank` | `District Rank`

---

## Microsoft Fabric (Bonus)
- **Lakehouse**: `ortho_lakehouse` — Delta Lake storage in OneLake
- **Dataflow Gen2**: `ortho_transformation` — Power Query pipeline filtering to Stryker, adding year_quarter column, loading to gold table
- **Gold table**: `stryker_payments_gold` — production-ready Stryker-only dataset
- **Page 5**: Direct Lake connection in Power BI showing Stryker payments by state

---

## Data Quality Notes
- CMS 2026 export renamed columns from `Physician_*` to `Covered_Recipient_*` — handled in ingest.py
- Stryker operates under 3+ entity names (STRYKER CORPORATION, HOWMEDICA OSTEONICS, STRYKER SALES) — consolidated to single label
- specialty column widened to NVARCHAR(500) to handle full CMS specialty strings
- 2,769 unmatched rows (0.6%) excluded from fact table — all joins validated

---

## Tech Stack
| Component | Tool |
|---|---|
| Data Source | CMS Open Payments (openpaymentsdata.cms.gov) |
| Ingestion | Python 3.14 + pandas |
| Database | SQL Server Developer Edition (BLONDCI\SQLEXPRESS01) |
| SQL IDE | SSMS 16.0 |
| Transformations | T-SQL star schema ETL |
| Dashboard | Power BI Desktop |
| DAX | 13 production measures |
| Fabric | Lakehouse + Dataflow Gen2 + Direct Lake |
| Version Control | Git + GitHub |

---

## How to Reproduce
```
1. git clone https://github.com/BAGEE514/stryker-ortho-analytics
2. Install SQL Server Developer + SSMS + Power BI Desktop + Python
3. Download CMS Open Payments data from openpaymentsdata.cms.gov (2021-2023)
4. pip install pandas sqlalchemy pyodbc numpy
5. python ingest/ingest.py
6. In SSMS: run sql/01 through sql/04 in order
7. In SSMS: run sql/05 through sql/09 to verify analysis queries
8. Open powerbi/Stryker_Ortho_Analytics_Dashboard.pbix → refresh
```

---

## Dataset
CMS Open Payments Program — federally mandated public disclosure
https://openpaymentsdata.cms.gov/
Program Years 2021–2023 | General Payments | 503,337 ortho records after filtering