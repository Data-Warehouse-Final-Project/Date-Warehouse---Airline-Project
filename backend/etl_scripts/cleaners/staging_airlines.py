# ============================================================
# INSTALL (run once)
# ============================================================
# python -m pip install supabase pandas requests thefuzz python-Levenshtein

# ============================================================
# Library Imports
# ============================================================
import pandas as pd
import sys
from supabase import create_client, Client
import uuid 
import json
from datetime import datetime
import os

SUPABASE_URL = "https://eoyopmstogazaflwisbg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVveW9wbXN0b2dhemFmbHdpc2JnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI3NjM5OTUsImV4cCI6MjA3ODMzOTk5NX0.JJC9myvRVLAk_7OSW57JtnbcMyB1xb2F0cDrbOpOZi8"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================
# LOGGING MODULE (Integrated as provided)
# ============================================================
RUN_ID = str(uuid.uuid4())
LOG_FILE_PATH = f"pipeline_run_log_{RUN_ID}.txt"
CURRENT_PHASE = None

# SQL to create log table (if not exists)
CREATE_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id TEXT,
    event_time TIMESTAMP WITH TIME ZONE,
    layer CHAR(1),
    component TEXT,
    table_name TEXT,
    level TEXT,
    message TEXT,
    details_json JSONB
);
"""

# -------------------------
# Helper: check if pipeline_run_log exists
# -------------------------
def log_table_exists():
    try:
        supabase.table("pipeline_run_log").select("id", count="exact").limit(1).execute()
        return True
    except Exception:
        return False

# -------------------------
# Supabase structured logger
# -------------------------
def log_event_supabase(layer, component, table_name, level, message, details=None):
    event_time = datetime.utcnow().isoformat()
    details_json = json.dumps(details, ensure_ascii=False) if details else None
    record = {
        "run_id": RUN_ID,
        "event_time": event_time,
        "layer": layer,
        "component": component,
        "table_name": table_name,
        "level": level,
        "message": message,
        "details_json": details_json
    }
    try:
        supabase.table("pipeline_run_log").insert(record).execute()
    except Exception as e:
        print("Warning: Failed to write log to Supabase:", str(e))
        if not log_table_exists():
            print("Run this SQL in Supabase SQL editor to create the log table:")
            print(CREATE_LOG_TABLE_SQL)

# -------------------------
# Timestamp helper
# -------------------------
def _timestamp():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# -------------------------
# Phase logger
# -------------------------
def set_phase(phase_name):
    global CURRENT_PHASE
    CURRENT_PHASE = phase_name.upper()
    header = f"\n==================== {CURRENT_PHASE} ====================\n"

    # TXT + terminal
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(header)
    print(header.strip())

    # Supabase structured logging
    layer_map = {"EXTRACT": "E", "TRANSFORMATION": "T", "VALIDATION": "T",
                 "TRANSFORM-CLEANED": "T", "LOAD": "L", "SUMMARY": "S"}
    layer = layer_map.get(CURRENT_PHASE, "X")
    component = CURRENT_PHASE.lower()
    table_name = "pipeline"  # default
    message = f"--- Entering phase: {CURRENT_PHASE} ---"
    log_event_supabase(layer, component, table_name, "INFO", message)

# -------------------------
# Event logger
# -------------------------
def log_event(message, layer=None, component=None, table_name=None, level="INFO", details=None):
    ts = _timestamp()
    line = f"[{ts}] {message}\n"

    # TXT + terminal
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(line)
    print(line.strip())

    # Supabase structured logging
    layer_map = {"EXTRACT": "E", "TRANSFORMATION": "T", "VALIDATION": "T",
                 "TRANSFORM-CLEANED": "T", "LOAD": "L", "SUMMARY": "S"}
    sup_layer = layer or layer_map.get(CURRENT_PHASE, "X")
    sup_component = component or (CURRENT_PHASE.lower() if CURRENT_PHASE else "pipeline")
    sup_table_name = table_name or "pipeline"
    log_event_supabase(sup_layer, sup_component, sup_table_name, level, message, details)

# -------------------------
# Get TXT log file path
# -------------------------
def get_log_file_path():
    return LOG_FILE_PATH

# ============================================================
# CLEANING CODE (Original with logging integrated)
# ============================================================

# Start pipeline
set_phase("EXTRACT")
log_event("Starting airlines pipeline", table_name="airline.csv", details={"run_id": RUN_ID})

file_path = r"C:\Users\Randy\airline-project\raw-data\airline.csv"
# Allow input path via argv (frontend will provide)
input_path = sys.argv[1] if len(sys.argv) > 1 else file_path
try:
    df = pd.read_csv(input_path)
    log_event(f"STEP 1 DONE: Extracted {len(df)} rows from {file_path}", table_name="airline.csv")
    log_event("Data preview:", table_name="airline.csv")
    print(df.head())  # Keep original print for preview
    log_event("File loaded successfully", table_name="airline.csv", 
              details={"rows": len(df), "columns": df.columns.tolist()})
except Exception as e:
    error_msg = f"Failed to read file: {str(e)}"
    log_event(f"ERROR: {error_msg}", table_name="airline.csv", level="ERROR", details={"file_path": file_path})
    print(f"ERROR: {error_msg}")
    exit()

# ============================================================
# STEP 2 – TRANSFORM (clean & prepare)
# ============================================================
set_phase("TRANSFORMATION")

# Table checker (kept from original)
def table_exists(table_name):
    try:
        supabase.table(table_name).select("*", count="exact").limit(1).execute()
        return True
    except Exception:
        return False

# Fetch existing airline keys (kept from original)
def fetch_existing_airlinekeys():
    if not table_exists("staging_airlines"):
        log_event("staging_airlines table not found", component="references", table_name="staging_airlines", level="INFO")
        return []
    
    try:
        res = supabase.table("staging_airlines").select("airlinekey").execute()
        return [row["airlinekey"].upper() for row in res.data] if res.data else []
    except Exception as e:
        log_event("Failed to fetch existing keys", component="references", table_name="staging_airlines", 
                  level="WARN", details={"error": str(e)})
        return []

# Standardize column names
log_event("Cleaning column names...", table_name="airline.csv")
df.columns = df.columns.str.strip().str.lower()
log_event("Standardized column names", table_name="airline.csv", 
          details={"columns": df.columns.tolist()})

# Ensure we have required columns
required_columns = ['airlinekey', 'airlinename', 'alliance']
for col in required_columns:
    if col not in df.columns:
        if col == 'airlinename' and 'airlinekey' in df.columns:
            df['airlinename'] = df['airlinekey']
            log_event(f"Warning: Created '{col}' column from airlinekey", table_name="staging_airlines", level="WARN")
        elif col == 'alliance':
            df['alliance'] = 'None'  # Changed from '' to 'None'
            log_event(f"Warning: Created '{col}' column with default 'None'", table_name="staging_airlines", level="WARN")
        else:
            log_event(f"ERROR: Missing required column: {col}", table_name="staging_airlines", level="ERROR")
            print(f"ERROR: Missing required column: {col}")
            exit()

# Basic cleaning with proper NaN handling
df["airlinekey"] = df["airlinekey"].astype(str).str.strip().str.upper()
df["airlinename"] = df["airlinename"].astype(str).str.strip().str.title()
df["airlinename"] = df["airlinename"].str.replace(r"\s+", " ", regex=True)

# FIX: Handle NaN properly - use 'None' for empty/NaN values (to match your ENUM)
df["alliance"] = df["alliance"].fillna('None')  # Fill NaN with 'None' not ''
df["alliance"] = df["alliance"].astype(str).str.strip()

# Standardize alliance values - ensure all map to valid ENUM values
alliance_mapping = {
    '': 'None',           # Map empty string to 'None'
    'nan': 'None',        # Map 'nan' string to 'None'
    'none': 'None',       # Already 'None'
    'oneworld': 'Oneworld', 
    'sky team': 'SkyTeam', 
    'skyteam': 'SkyTeam',
    'star alliance': 'Star Alliance',
    'staralliance': 'Star Alliance'
}
df["alliance"] = df["alliance"].replace(alliance_mapping)
df["alliance"] = df["alliance"].fillna('None')  # Final safety fill

# Additional fix: Ensure values match ENUM exactly
valid_alliances = ['Oneworld', 'SkyTeam', 'Star Alliance', 'None']
df["alliance"] = df["alliance"].apply(
    lambda x: x if x in valid_alliances else 'None'
)

# Add specific alliance fixes
alliance_fixes = {
    'VS': 'SkyTeam',  # Virgin Atlantic should be SkyTeam
    'AZ': 'None'      # Alitalia should be None
}

# Apply fixes to the dataframe
for airline_key, correct_alliance in alliance_fixes.items():
    mask = df["airlinekey"] == airline_key
    if mask.any():
        df.loc[mask, "alliance"] = correct_alliance
        log_event(f"Fixed alliance for {airline_key}: {correct_alliance}", 
                  table_name="staging_airlines", details={"airlinekey": airline_key, "new_alliance": correct_alliance})

# Validation
airlinekey_valid = df["airlinekey"].str.match(r"^[A-Z0-9]{2,3}$")  # Changed to allow 3 chars
airlinename_valid = df["airlinename"].str.match(r"^[A-Za-z0-9\s\.\-\&]+$")
duplicate_mask = df["airlinekey"].duplicated(keep="first")

all_valid = airlinekey_valid & airlinename_valid & (~duplicate_mask)
df_clean = df[all_valid].copy()
df_quarantine = df[~all_valid].copy()

log_event(f"STEP 2 DONE: Cleaned {len(df_clean)} valid rows", table_name="staging_airlines")
log_event(f"Quarantined: {len(df_quarantine)} rows", table_name="staging_airlines", level="WARN")

# Debug: Show alliance values
log_event("Unique alliance values after cleaning:", table_name="staging_airlines")
print(df_clean["alliance"].value_counts())

if not df_quarantine.empty:
    log_event("Quarantined rows:", table_name="staging_airlines", level="WARN")
    print(df_quarantine[["airlinekey", "airlinename", "alliance"]].to_string())
    log_event("Found invalid rows", component="quarantine", table_name="staging_airlines", 
              level="WARN", details={"count": len(df_quarantine)})

log_event("Data cleaning completed", table_name="staging_airlines",
          details={"valid": len(df_clean), "quarantine": len(df_quarantine)})

# ============================================================
# STEP 3 – LOAD (to Supabase)
# ============================================================
set_phase("LOAD")

# Check if staging_airlines table exists
if not table_exists("staging_airlines"):
    log_event("staging_airlines table does not exist in Supabase!", table_name="staging_airlines", level="ERROR")
    print("ERROR: staging_airlines table does not exist in Supabase!")
    print("Please create the table first with this SQL:")
    print("""
    CREATE TABLE staging_airlines (
        airlinekey VARCHAR(3) PRIMARY KEY,
        airlinename VARCHAR(100) NOT NULL,
        alliance alliance NOT NULL  -- ENUM type: 'Oneworld', 'SkyTeam', 'Star Alliance', 'None'
    );
    """)
    exit()

# First, clear existing data to avoid conflicts
log_event("Clearing existing data from staging_airlines...", table_name="staging_airlines")
try:
    supabase.table("staging_airlines").delete().neq("airlinekey", "___dummy___").execute()
    log_event("Existing data cleared", table_name="staging_airlines")
except Exception as e:
    log_event(f"Warning: Could not clear table (might be empty): {str(e)}", table_name="staging_airlines", level="WARN")

log_event("Uploading to staging_airlines...", table_name="staging_airlines")

# Debug: Show first few records being uploaded
log_event("First 5 records being uploaded:", table_name="staging_airlines")
for i, row in df_clean.head(5).iterrows():
    log_event(f"  {row['airlinekey']}: '{row['airlinename']}', alliance: '{row['alliance']}'", 
              table_name="staging_airlines")

successful = 0
failed = []

for _, row in df_clean.iterrows():
    try:
        # Ensure alliance is a valid ENUM value
        alliance_value = row["alliance"]
        if alliance_value not in ['Oneworld', 'SkyTeam', 'Star Alliance', 'None']:
            alliance_value = 'None'  # Default to 'None' if invalid
        
        record = {
            "airlinekey": row["airlinekey"],
            "airlinename": row["airlinename"],
            "alliance": alliance_value
        }
        supabase.table("staging_airlines").upsert(record).execute()
        successful += 1
        if successful % 10 == 0:  # Progress indicator
            log_event(f"  Uploaded {successful} records...", table_name="staging_airlines")
    except Exception as e:
        failed.append({"key": row["airlinekey"], "error": str(e)})
        log_event(f"Failed to upload {row['airlinekey']}", table_name="staging_airlines", 
                  level="ERROR", details={"error": str(e), "alliance_value": str(row["alliance"])})

log_event(f"STEP 3 DONE: Uploaded {successful} rows to Supabase", table_name="staging_airlines")
if failed:
    log_event(f"Failed: {len(failed)} rows", table_name="staging_airlines", level="ERROR")
    log_event("Failed records:", table_name="staging_airlines", level="ERROR")
    for f in failed:
        log_event(f"  - {f['key']}: {f['error']}", table_name="staging_airlines", level="ERROR")

log_event("Upload completed", table_name="staging_airlines",
          details={"successful": successful, "failed": len(failed)})

# ============================================================
# VERIFICATION
# ============================================================
set_phase("VALIDATION")
log_event("Verifying upload...", table_name="staging_airlines")
try:
    result = supabase.table("staging_airlines").select("count", count="exact").execute()
    db_count = result.count if hasattr(result, 'count') else len(result.data)
    log_event(f"Total records in database: {db_count}", table_name="staging_airlines")
    
    # Show a few records from DB
    sample = supabase.table("staging_airlines").select("*").limit(5).execute()
    if sample.data:
        log_event("Sample from database:", table_name="staging_airlines")
        for row in sample.data:
            log_event(f"  {row['airlinekey']}: {row['airlinename']} - Alliance: {row['alliance']}", 
                      table_name="staging_airlines")
except Exception as e:
    log_event(f"Warning: Could not verify: {str(e)}", table_name="staging_airlines", level="WARN")

# ============================================================
# FINAL SUMMARY
# ============================================================
set_phase("SUMMARY")
log_event("="*60, table_name="staging_airlines")
log_event("AIRLINES CLEANING PIPELINE COMPLETED", table_name="staging_airlines")
log_event("="*60, table_name="staging_airlines")
log_event("Summary:", table_name="staging_airlines")
log_event(f"  Total processed: {len(df)} rows", table_name="staging_airlines")
log_event(f"  Successfully uploaded: {successful} rows", table_name="staging_airlines")
log_event(f"  Quarantined: {len(df_quarantine)} rows", table_name="staging_airlines")
log_event(f"  Failed uploads: {len(failed)} rows", table_name="staging_airlines")
log_event(f"Log file: {LOG_FILE_PATH}", table_name="staging_airlines")
log_event("="*60, table_name="staging_airlines")

# Save cleaned data locally
output_file = "cleaned_airlines.csv"
df_clean.to_csv(output_file, index=False)
log_event(f"Clean data also saved to: {output_file}", table_name="staging_airlines")

log_event("Pipeline completed", table_name="staging_airlines",
          details={"total": len(df), "uploaded": successful, "quarantine": len(df_quarantine), "failed": len(failed)})

# Print final summary to console as well
print("\n" + "="*60)
print("AIRLINES CLEANING PIPELINE COMPLETED")
print("="*60)
print(f"Summary:")
print(f"  Total processed: {len(df)} rows")
print(f"  Successfully uploaded: {successful} rows")
print(f"  Quarantined: {len(df_quarantine)} rows")
print(f"  Failed uploads: {len(failed)} rows")
print(f"Log file: {LOG_FILE_PATH}")
print("="*60)
print(f"Clean data also saved to: {output_file}")