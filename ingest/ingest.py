# ingest/ingest.py
# PURPOSE: Filter the 4GB+ CMS Open Payments CSV down to orthopedic companies,
# clean messy columns, and load into SQL Server for analysis.
# EXPECTED OUTPUT:
# - Prints row counts at each step
# - Saves data/processed/ortho_payments_clean.csv
# - Saves data/processed/null_audit.csv (document for README)
# - Loads table [OrthoAnalytics].[raw].[raw_payments] in SQL Server

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
import re
import os

# ── CONFIGURATION ────────────────────────────────────────────────────────
# Update these paths to match where you saved the CSV files
DATA_FOLDER = r"data\raw"

# Update SERVER to match your SQL Server instance name
# Common values: ".", "localhost", "DESKTOP-XXXX\SQLEXPRESS", "(local)"
SQL_SERVER = "BLONDCI\\SQLEXPRESS01"
SQL_DB = "OrthoAnalytics"

# Years to process — match the CSV files you downloaded
YEARS = ["2021", "2022", "2023"]

# Filename pattern — adjust if your downloaded files have different names
def get_csv_path(year):
    # Try both common filename patterns
    patterns = [
        f"OP_DTL_GNRL_PGYR{year}_P01232026_01102026.csv",  # ← ADD THIS LINE
        f"OP_DTL_GNRL_PGYR{year}_P06282024.csv",
        f"OP_DTL_GNRL_PGYR{year}.csv",
    ]
    for p in patterns:
        full = os.path.join(DATA_FOLDER, p)
        if os.path.exists(full):
            return full
    raise FileNotFoundError(f"Cannot find CSV for year {year} in {DATA_FOLDER}")

# ── COMPANY KEYWORD FILTER ───────────────────────────────────────────────
# These are partial matches — catches all Stryker subsidiaries
ORTHO_KEYWORDS = [
    "STRYKER", "HOWMEDICA", "OSTEONICS",   # Stryker entities
    "ZIMMER", "BIOMET",                     # Zimmer Biomet entities
    "DEPUY", "SYNTHES",                     # J&J MedTech entities
    "SMITH.*NEPHEW", "SMITH AND NEPHEW",    # Smith+Nephew
    "ARTHREX",                              # Arthrex
    "WRIGHT MEDICAL",                       # Wright Medical
    "EXACTECH",                             # Exactech
    "CONFORMIS",                            # Conformis
    "MEDACTA",                              # Medacta
]

# Join into one regex pattern for fast filtering
COMPANY_PATTERN = "|".join(ORTHO_KEYWORDS)

# ── STEP 1: LOAD AND FILTER EACH YEAR ───────────────────────────────────
print("=" * 60)
print("STRYKER ORTHO ANALYTICS — INGEST SCRIPT")
print("=" * 60)

all_frames = []

for year in YEARS:
    csv_path = get_csv_path(year)
    print(f"\n[{year}] Reading: {csv_path}")
    print(f"  File size: {os.path.getsize(csv_path)/1e9:.2f} GB")

    # WHY chunksize: The full file is 4GB+ — loading all at once would
    # likely crash on machines with less than 16GB RAM.
    # We read 200k rows at a time, filter to ortho, then discard the rest
    chunk_iter = pd.read_csv(
        csv_path,
        chunksize=200_000,
        low_memory=False,
        on_bad_lines="skip",   # some CMS rows have malformed quotes
        encoding="latin-1",    # CMS exports use latin-1 encoding, not UTF-8
    )

    year_frames = []
    total_read = 0

    for i, chunk in enumerate(chunk_iter):
        total_read += len(chunk)

        # Create upper-case version for matching
        name_col = "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"
        if name_col not in chunk.columns:
            # Try short column name used in older years
            name_col = "Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name"

        chunk["_company_upper"] = chunk[name_col].fillna("").str.upper().str.strip()

        # Filter to ortho companies
        mask_company = chunk["_company_upper"].str.contains(
            COMPANY_PATTERN, na=False, regex=True
        )

        # Filter to ortho-relevant physician specialties
        spec_col = "Physician_Specialty" if "Physician_Specialty" in chunk.columns else "Covered_Recipient_Specialty_1"
        if spec_col in chunk.columns:
            mask_specialty = chunk[spec_col].str.contains(
                "Orthopaedic|Orthopedic|Hand|Spine|Physical Medicine",
                na=False, regex=True
            )
        else:
            mask_specialty = pd.Series(True, index=chunk.index)

        filtered = chunk[mask_company & mask_specialty].copy()

        if len(filtered) > 0:
            year_frames.append(filtered)

        # Progress indicator every 5 chunks
        if (i + 1) % 5 == 0:
            kept = sum(len(f) for f in year_frames)
            print(f"  Chunk {i+1}: read {total_read:,} total → kept {kept:,} ortho rows so far...")

    year_df = pd.concat(year_frames, ignore_index=True) if year_frames else pd.DataFrame()
    print(f"  [DONE] {year}: {len(year_df):,} ortho rows from {total_read:,} total rows ({len(year_df)/total_read*100:.1f}% of file)")
    all_frames.append(year_df)

df = pd.concat(all_frames, ignore_index=True)
print(f"\n{'='*60}")
print(f"TOTAL ORTHO RECORDS (all years): {len(df):,}")
print(f"COLUMNS: {len(df.columns)}")
print(f"{'='*60}")

# ── STEP 2: NULL AUDIT ───────────────────────────────────────────────────
# WHY: Document data gaps BEFORE cleaning — this demonstrates Data

print("\n[STEP 2] Running null audit...")
null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
print("\nTop 15 columns by null %:")
print(null_pct.head(15).to_string())
null_pct.reset_index().rename(columns={"index": "column", 0: "null_pct"}).to_csv(
    "data/processed/null_audit.csv", index=False
)
print("\nSaved: data/processed/null_audit.csv")

# ── STEP 3: STANDARDIZE COMPANY NAMES ───────────────────────────────────
# WHY: Stryker has 5+ entity names in the raw data (STRYKER CORPORATION,
# HOWMEDICA OSTEONICS CORP, STRYKER SALES CORPORATION, etc.).
# We consolidate to 6 clean company names for meaningful group-by analysis.
print("\n[STEP 3] Standardizing company names...")

def map_company(raw_name):
    name = str(raw_name).upper()
    if any(x in name for x in ["STRYKER", "HOWMEDICA", "OSTEONICS"]):
        return "Stryker"
    elif any(x in name for x in ["ZIMMER", "BIOMET"]):
        return "Zimmer Biomet"
    elif any(x in name for x in ["DEPUY", "SYNTHES", "JOHNSON"]):
        return "J&J MedTech"
    elif "SMITH" in name and ("NEPHEW" in name):
        return "Smith+Nephew"
    elif "ARTHREX" in name:
        return "Arthrex"
    elif "WRIGHT" in name:
        return "Wright Medical"
    else:
        return "Other Ortho"

df["company_name"] = df["_company_upper"].apply(map_company)
print("  Company distribution:")
print(df["company_name"].value_counts().to_string())

# ── STEP 4: CLEAN AND PARSE KEY COLUMNS ─────────────────────────────────
print("\n[STEP 4] Cleaning and parsing columns...")

# Payment date
df["payment_date"] = pd.to_datetime(
    df["Date_of_Payment"], errors="coerce", format="%m/%d/%Y"
)
df["payment_year"] = df["payment_date"].dt.year.astype("Int64")
df["payment_month"] = df["payment_date"].dt.month.astype("Int64")
df["payment_quarter"] = df["payment_date"].dt.quarter.astype("Int64")

# Payment amount — arrives as string e.g. "1,234.56"
df["payment_usd"] = (
    df["Total_Amount_of_Payment_USDollars"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace("$", "", regex=False)
    .str.strip()
)
df["payment_usd"] = pd.to_numeric(df["payment_usd"], errors="coerce")

# State — standardize to 2-letter uppercase
df["state"] = df["Recipient_State"].fillna("").str.upper().str.strip()

# Surgeon full name
first = df.get("Covered_Recipient_First_Name",
       df.get("Physician_First_Name", pd.Series([""] * len(df)))).fillna("")
last = df.get("Covered_Recipient_Last_Name",
      df.get("Physician_Last_Name", pd.Series([""] * len(df)))).fillna("")
df["surgeon_name"] = (first + " " + last).str.strip()

# City
df["city"] = df["Recipient_City"].fillna("").str.title().str.strip()

# Specialty — clean up
df["specialty"] = df.get("Physician_Specialty", df.get("Covered_Recipient_Specialty_1", pd.Series(["Unknown"] * len(df)))).fillna("Unknown").str.strip()

# NPI — unique physician identifier
df["npi"] = df.get("Physician_NPI", pd.Series([""] * len(df))).fillna("").astype(str)

# Hospital
df["hospital_name"] = df.get(
    "Teaching_Hospital_Name", pd.Series([""] * len(df))
).fillna("").str.strip()

# Device/product name
df["device_name"] = df.get(
    "Name_of_Associated_Device_or_Medical_Supply",
    pd.Series([""] * len(df))
).fillna("").str.strip()

# ── STEP 5: BUCKET PAYMENT TYPES ────────────────────────────────────────
# WHY: 25+ raw payment categories → 7 clean buckets for dashboard slicers.
# Royalties = highest surgeon loyalty signal. Consulting = advisory relationships.
def bucket_payment(raw_type):
    t = str(raw_type).lower()
    if "royalt" in t:
        return "Royalty"
    elif "consult" in t:
        return "Consulting"
    elif "speaker" in t or "educ" in t:
        return "Speaking / Education"
    elif "research" in t:
        return "Research"
    elif "food" in t or "beverage" in t:
        return "Meals & Events"
    elif "travel" in t:
        return "Travel"
    else:
        return "Other"

df["payment_type"] = df["Nature_of_Payment_or_Transfer_of_Value"].apply(bucket_payment)
print("  Payment type distribution:")
print(df["payment_type"].value_counts().to_string())

# ── STEP 6: REMOVE BAD ROWS ──────────────────────────────────────────────
print("\n[STEP 6] Removing invalid rows...")
before = len(df)
df = df[df["payment_usd"] > 0]                  # must have positive amount
df = df[df["payment_date"].notna()]              # must have valid date
df = df[df["state"].str.len() == 2]             # must have 2-char state code
df = df[df["payment_year"].between(2018, 2024)] # sanity-check year range
after = len(df)
print(f"  Removed {before - after:,} bad rows ({(before-after)/before*100:.1f}% of total)")
print(f"  Clean records: {after:,}")

# ── STEP 7: SELECT FINAL COLUMNS ────────────────────────────────────────
final_cols = [
    "company_name", "surgeon_name", "npi", "specialty", "city", "state",
    "hospital_name", "payment_date", "payment_year", "payment_month",
    "payment_quarter", "payment_usd", "payment_type", "device_name",
    "Record_ID"
]

# Keep only columns that exist
final_cols = [c for c in final_cols if c in df.columns]
df_final = df[final_cols].copy()

# ── STEP 8: SAVE PROCESSED CSV ───────────────────────────────────────────
os.makedirs("data/processed", exist_ok=True)
out_path = "data/processed/ortho_payments_clean.csv"
df_final.to_csv(out_path, index=False)
print(f"\nSaved: {out_path} ({len(df_final):,} rows)")

# ── STEP 9: LOAD TO SQL SERVER ───────────────────────────────────────────
print("\n[STEP 9] Loading to SQL Server...")
print(f"  Connecting to: {SQL_SERVER} / {SQL_DB}")

conn_str = (
    f"mssql+pyodbc://{SQL_SERVER}/{SQL_DB}"
    f"?driver=ODBC+Driver+17+for+SQL+Server"
    f"&trusted_connection=yes"
)

try:
    engine = create_engine(conn_str, fast_executemany=True)

    # Create raw schema if it doesn't exist yet
    with engine.connect() as conn:
        conn.execute(text(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='raw') "
            "EXEC('CREATE SCHEMA raw')"
        ))
        conn.commit()

    # Load data — chunksize 5000 keeps memory usage low
    df_final.to_sql(
    "raw_payments",
    engine,
    schema="raw",
    if_exists="replace",
    index=False,
    chunksize=100,
)

    # Verify the load
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw.raw_payments")).fetchone()
        print(f"  Verified: {result[0]:,} rows in [OrthoAnalytics].[raw].[raw_payments]")

    print("\n[SUCCESS] Ingestion complete!")

except Exception as e:
    print(f"\n[ERROR] SQL Server connection failed: {e}")
    print("  Check that SQL Server is running and the server name is correct.")
    print("  Your server name options: . | localhost | (local) | COMPUTERNAME\\SQLEXPRESS")
    print("  Edit SQL_SERVER at the top of this script and retry.")

# ── STEP 10: PRINT SUMMARY STATS ────────────────────────────────────────
print("\n=== FINAL SUMMARY ===")
print(f"Total clean rows: {len(df_final):,}")
print(f"Companies: {df_final['company_name'].nunique()}")
print(f"Unique surgeons: {df_final['surgeon_name'].nunique():,}")
print(f"States: {df_final['state'].nunique()}")
print(f"Date range: {df_final['payment_date'].min()} to {df_final['payment_date'].max()}")
print(f"Total USD value: " + str(round(df_final['payment_usd'].sum(), 0)))
print("\nTop 5 companies by total payments:")
print(df_final.groupby('company_name')['payment_usd'].sum().sort_values(ascending=False).head().to_string())