# ============================================================
# Library Imports
# ============================================================
import uuid
import json
from datetime import datetime

import pandas as pd
from supabase import create_client, Client

# ============================================================
# 🔌 SUPABASE CONFIGURATION
# ============================================================
SUPABASE_URL = "https://zitrseennwgvdzmmavdt.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InppdHJzZWVubndndmR6bW1hdmR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUxOTQ1MTIsImV4cCI6MjA4MDc3MDUxMn0.SBVVYD3g4enewAKmnyAgEzz2Lxdj5muhTILFqCnjCv4"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================
# RUN / LOGGING SETUP
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

def log_table_exists():
    try:
        supabase.table("pipeline_run_log").select("id", count="exact").limit(1).execute()
        return True
    except Exception:
        return False

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
        resp = supabase.table("pipeline_run_log").insert(record).execute()

        if isinstance(resp, dict):
            resp_data = resp.get("data")
            resp_error = resp.get("error")
        else:
            resp_data = getattr(resp, "data", None)
            resp_error = getattr(resp, "error", None)

        if resp_error:
            print("⚠️ Supabase log insert error:", resp_error)
        elif not resp_data:
            print("⚠️ Supabase log insert returned no data (check RLS / policies):", resp)
    except Exception as e:
        print("⚠️ Failed to write log to Supabase (exception):", repr(e))
        if not log_table_exists():
            print("Run this SQL in Supabase SQL editor to create the log table:")
            print(CREATE_LOG_TABLE_SQL)

def _timestamp():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def set_phase(phase_name):
    global CURRENT_PHASE
    CURRENT_PHASE = phase_name.upper()
    header = f"\n==================== {CURRENT_PHASE} ====================\n"
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(header)
    print(header.strip())

    layer_map = {"EXTRACT": "E", "TRANSFORMATION": "T", "VALIDATION": "T",
                 "TRANSFORM-CLEANED": "T", "LOAD": "L", "SUMMARY": "S"}
    layer = layer_map.get(CURRENT_PHASE, "X")
    component = CURRENT_PHASE.lower()
    table_name = "pipeline"
    message = f"--- Entering phase: {CURRENT_PHASE} ---"
    log_event_supabase(layer, component, table_name, "INFO", message)

def log_event(message, layer=None, component=None, table_name=None, level="INFO", details=None):
    ts = _timestamp()
    line = f"[{ts}] {message}\n"
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(line)
    print(line.strip())

    layer_map = {"EXTRACT": "E", "TRANSFORMATION": "T", "VALIDATION": "T",
                 "TRANSFORM-CLEANED": "T", "LOAD": "L", "SUMMARY": "S"}
    sup_layer = layer or layer_map.get(CURRENT_PHASE, "X")
    sup_component = component or (CURRENT_PHASE.lower() if CURRENT_PHASE else "pipeline")
    sup_table_name = table_name or "pipeline"
    log_event_supabase(sup_layer, sup_component, sup_table_name, level, message, details)

def get_log_file_path():
    return LOG_FILE_PATH

# -------------------------
# Upload helper (for CLEAN rows only)
# -------------------------
def upload_dataframe_to_supabase(df, table_name, component="airports", chunk_size=500):
    if df.empty:
        log_event(
            f"No rows to upload to {table_name}",
            component=component,
            table_name=table_name
        )
        return

    df_for_upload = df.where(df.notnull(), None)
    records = df_for_upload.to_dict(orient="records")

    log_event(
        f"Starting upload of {len(records)} rows to Supabase table '{table_name}'",
        component=component,
        table_name=table_name
    )

    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        try:
            # Plain INSERT (no ON CONFLICT), so it works even without PK/UNIQUE in Supabase
            resp = supabase.table(table_name).insert(chunk).execute()

            if isinstance(resp, dict):
                resp_data = resp.get("data")
                resp_error = resp.get("error")
            else:
                resp_data = getattr(resp, "data", None)
                resp_error = getattr(resp, "error", None)

            if resp_error:
                print(f"⚠️ Supabase insert error for table {table_name}:", resp_error)
                log_event(
                    f"Supabase insert error for table {table_name}: {resp_error}",
                    component=component,
                    table_name=table_name,
                    level="ERROR",
                    details={"error": str(resp_error)}
                )
            elif resp_data is None:
                print(f"⚠️ Supabase insert returned no data for table {table_name}: {resp}")
                log_event(
                    f"Supabase insert returned no data for table {table_name}",
                    component=component,
                    table_name=table_name,
                    level="WARNING",
                    details={"response": str(resp)}
                )
            else:
                log_event(
                    f"Uploaded batch {i}–{i+len(chunk)-1} to {table_name}",
                    component=component,
                    table_name=table_name,
                    details={"batch_size": len(chunk)}
                )
        except Exception as e:
            print(f"⚠️ Exception while inserting into {table_name}:", repr(e))
            log_event(
                f"Exception while inserting into {table_name}: {e}",
                component=component,
                table_name=table_name,
                level="ERROR",
                details={"exception": repr(e)}
            )

# ============================================================
# AIRPORT CLEANING PIPELINE
# ============================================================

# ---------- EXTRACT ----------
set_phase("EXTRACT")
log_event(
    "Started loading airports.csv",
    component="airports",
    table_name="airports_raw"
)

try:
    df = pd.read_csv("airports.csv", encoding="utf-8-sig", on_bad_lines="skip")
    log_event(
        f"Data loaded successfully. Rows: {len(df)}",
        component="airports",
        table_name="airports_raw",
        details={"row_count": int(len(df))}
    )
except Exception as e:
    print("⚠️ Error reading file:", e)
    log_event(
        f"Error reading file: {e}",
        component="airports",
        table_name="airports_raw",
        level="ERROR",
        details={"exception": str(e)}
    )
    raise SystemExit(1)

# ---------- TRANSFORMATION ----------
set_phase("TRANSFORMATION")
log_event(
    "Cleaning whitespace, internal spaces, and standardizing text for airports.",
    component="airports",
    table_name="staging_airports"
)

# Trim leading/trailing spaces
cols_to_strip = ["AirportKey", "AirportName", "City", "Country"]
for col in cols_to_strip:
    df[col] = df[col].astype(str).str.strip()

# Collapse multiple spaces in name/city/country
for col in ["AirportName", "City", "Country"]:
    df[col] = df[col].str.replace(r"\s+", " ", regex=True)

# Standardize casing (initial Title Case)
df["AirportKey"] = df["AirportKey"].str.upper()
df["AirportName"] = df["AirportName"].str.title()
df["City"] = df["City"].str.title()
df["Country"] = df["Country"].str.title()

# ------- Fix incorrect title-case issues & small words -------

# Fix "'S" capitalization (St. John'S → St. John's)
df["City"] = df["City"].str.replace(r"'S\b", "'s", regex=True)
df["AirportName"] = df["AirportName"].str.replace(r"'S\b", "'s", regex=True)

# Normalize connecting words: And/Of/The/In/At → lowercase
def fix_small_words(text):
    if not isinstance(text, str):
        return text
    small_words = ["And", "Of", "The", "At", "In"]
    for w in small_words:
        text = text.replace(f" {w} ", f" {w.lower()} ")
    return text

df["AirportName"] = df["AirportName"].apply(fix_small_words)
df["City"] = df["City"].apply(fix_small_words)
df["Country"] = df["Country"].apply(fix_small_words)

# Governance rule: fix country variants (including "Us")
country_replacements = {
    "Usa": "United States",
    "Us": "United States",
    "U.S.A.": "United States",
    "U.S.": "United States",
    "United States Of America": "United States",
    "Uk": "United Kingdom",
    "U.K.": "United Kingdom",
}
df["Country"] = df["Country"].replace(country_replacements)

log_event(
    "Transformation complete: whitespace cleaned, case normalized, apostrophes & small words fixed, country standardized.",
    component="airports",
    table_name="staging_airports"
)

# ---------- VALIDATION ----------
set_phase("VALIDATION")
log_event(
    "Validating fields and identifying only truly bad rows (missing/null/NaN/etc.).",
    component="airports",
    table_name="airports_validated"
)

# Helper: detect missing-like values (empty, 'nan', 'none', 'null', etc.)
MISSING_STRINGS = {"", "nan", "none", "null", "NaN", "None", "Null"}

def is_missing(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return s.isin(MISSING_STRINGS)

# AirportKey validity: must be non-missing and 3 uppercase letters
airportkey_missing = is_missing(df["AirportKey"])
airportkey_format_valid = df["AirportKey"].str.match(r"^[A-Z]{3}$", na=False)
airportkey_valid = (~airportkey_missing) & airportkey_format_valid

# Name/city/country must be present (non-missing after cleaning)
name_missing = is_missing(df["AirportName"])
city_missing = is_missing(df["City"])
country_missing = is_missing(df["Country"])

name_valid = ~name_missing
city_valid = ~city_missing
country_valid = ~country_missing

# Quarantine ONLY rows with missing/NaN/null/invalid key or critical fields
quarantine_mask = (
    (~airportkey_valid) |
    (~name_valid) |
    (~city_valid) |
    (~country_valid)
)

clean_mask = ~quarantine_mask

clean_df = df[clean_mask].copy()
quarantine_df = df[quarantine_mask].copy()

log_event(
    "Validation complete.",
    component="airports",
    table_name="airports_validated",
    details={
        "total_rows": int(len(df)),
        "clean_rows": int(len(clean_df)),
        "quarantine_rows": int(len(quarantine_df)),
        "missing_key_rows": int((~airportkey_valid).sum()),
        "missing_name_rows": int(name_missing.sum()),
        "missing_city_rows": int(city_missing.sum()),
        "missing_country_rows": int(country_missing.sum())
    }
)

# ---------- TRANSFORM-CLEANED / EXPORT ----------
set_phase("TRANSFORM-CLEANED")

# Export CSVs
clean_df.to_csv("cleaned_airports.csv", index=False, encoding="utf-8")

try:
    quarantine_df.to_csv("quarantine_airports.csv", index=False, encoding="utf-8")
except PermissionError as e:
    log_event(
        f"Could not write quarantine_airports.csv (file may be open or locked): {e}",
        component="airports",
        table_name="airports_quarantine",
        level="ERROR",
        details={"exception": repr(e)}
    )

log_event(
    f"Exported cleaned_airports.csv ({len(clean_df)} rows)",
    component="airports",
    table_name="airports_cleaned",
    details={"clean_rows": int(len(clean_df))}
)
log_event(
    f"Exported quarantine_airports.csv ({len(quarantine_df)} rows - only missing/null/invalid rows)",
    component="airports",
    table_name="airports_quarantine",
    details={"quarantine_rows": int(len(quarantine_df))}
)

# ---------- LOAD CLEAN ROWS TO SUPABASE ----------
set_phase("LOAD")

# Map pandas columns -> Supabase lowercase columns
column_map = {
    "AirportKey": "airportkey",
    "AirportName": "airportname",
    "City": "city",
    "Country": "country",
}
clean_for_supabase = clean_df.rename(columns=column_map)

# Dedup inside this load batch (based on airportkey)
before_dedup = len(clean_for_supabase)
clean_for_supabase = clean_for_supabase.drop_duplicates(
    subset=["airportkey"],
    keep="first"
)
after_dedup = len(clean_for_supabase)

if after_dedup != before_dedup:
    log_event(
        f"Detected and removed {before_dedup - after_dedup} duplicate airportkey rows before upload.",
        component="airports",
        table_name="staging_airports",
        level="INFO",
        details={
            "before_dedup": int(before_dedup),
            "after_dedup": int(after_dedup),
            "removed": int(before_dedup - after_dedup),
        }
    )

upload_dataframe_to_supabase(clean_for_supabase, "staging_airports", component="airports")

log_event(
    "Skipped Supabase upload for quarantine_airports (CSV only, for missing/null/invalid rows).",
    component="airports",
    table_name="airports_quarantine"
)

# 🔍 VERIFY ROW COUNT IN STAGING_AIRPORTS DIRECTLY FROM SUPABASE
try:
    resp = supabase.table("staging_airports").select("airportkey", count="exact").limit(1).execute()
    if isinstance(resp, dict):
        total = resp.get("count")
        error = resp.get("error")
    else:
        total = getattr(resp, "count", None)
        error = getattr(resp, "error", None)

    if error:
        log_event(
            f"Error when counting rows in staging_airports: {error}",
            component="airports",
            table_name="staging_airports",
            level="ERROR",
            details={"error": str(error)}
        )
    else:
        log_event(
            f"staging_airports row count (from Supabase): {total}",
            component="airports",
            table_name="staging_airports",
            details={"supabase_row_count": total}
        )
        print(f"🔎 Supabase staging_airports row count: {total}")
except Exception as e:
    log_event(
        f"Exception when counting rows in staging_airports: {e}",
        component="airports",
        table_name="staging_airports",
        level="ERROR",
        details={"exception": repr(e)}
    )

# ---------- SUMMARY ----------
set_phase("SUMMARY")
log_event(
    "Airport cleaning pipeline completed successfully!",
    component="airports",
    table_name="airports_pipeline"
)

print("✔ Cleaning Pipeline Completed")
print(f"   Clean rows      : {len(clean_df)}  (intended for staging_airports + cleaned_airports.csv)")
print(f"   Quarantine rows : {len(quarantine_df)}  (only missing/null/invalid, in quarantine_airports.csv)")
