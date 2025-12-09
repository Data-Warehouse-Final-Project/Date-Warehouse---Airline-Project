# ============================================================
# Library Imports
# ============================================================
import uuid
import json
from datetime import datetime
import pandas as pd
from supabase import create_client, Client
from rapidfuzz import process, fuzz
import sys

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
        print("⚠️ Failed to write log to Supabase:", str(e))
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
    layer_map = {
        "EXTRACT": "E",
        "TRANSFORMATION": "T",
        "VALIDATION": "T",
        "TRANSFORM-CLEANED": "T",
        "LOAD": "L",
        "SUMMARY": "S"
    }
    layer = layer_map.get(CURRENT_PHASE, "X")
    component = CURRENT_PHASE.lower()
    table_name = "pipeline"  # default
    message = f"--- Entering phase: {CURRENT_PHASE} ---"
    log_event_supabase(layer, component, table_name, "INFO", message)

# -------------------------
# Event logger (supports old & new styles)
# -------------------------
def log_event(*args, **kwargs):
    """
    Supports:
      1) Old style:
         log_event("E", "extract", "flights.csv", "INFO", "some message", {...})

      2) New style:
         log_event("some message",
                   layer="E", component="extract", table_name="flights.csv",
                   level="INFO", details={...})
    """
    layer_map = {
        "EXTRACT": "E",
        "TRANSFORMATION": "T",
        "VALIDATION": "T",
        "TRANSFORM-CLEANED": "T",
        "LOAD": "L",
        "SUMMARY": "S"
    }

    # Detect old-style call: (layer, component, table_name, level, message, [details])
    if len(args) >= 5 and isinstance(args[0], str) and isinstance(args[4], str):
        layer, component, table_name, level, message = args[:5]
        details = args[5] if len(args) > 5 else kwargs.get("details")
    else:
        # New-style: message first, then keyword args
        if len(args) >= 1:
            message = args[0]
        else:
            message = kwargs.get("message", "")
        layer = kwargs.get("layer")
        component = kwargs.get("component")
        table_name = kwargs.get("table_name")
        level = kwargs.get("level", "INFO")
        details = kwargs.get("details")

    ts = _timestamp()
    line = f"[{ts}] {message}\n"

    # TXT + terminal
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write(line)
    print(line.strip())

    # Supabase structured logging
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
# INSTALL (run once)
# ============================================================
# python -m pip install supabase pandas requests thefuzz python-Levenshtein

# ============================================================
# PIPELINE
# ============================================================

# ============================================================
# STEP 1 – EXTRACT (flights)
# ============================================================
set_phase("EXTRACT")

# Initial run log (kept from your original code)
log_event("E", "extract", "flights.csv", "INFO", "Started pipeline run", {"rows_in_source": None})

# More detailed EXTRACT logs
log_event(
    "Started loading flights.csv",
    layer="E",
    component="extract",
    table_name="flights.csv",
    level="INFO",
)

# Allow passing input path from command line (runner/frontend will provide)
input_path = sys.argv[1] if len(sys.argv) > 1 else "flights.csv"
df = pd.read_csv(input_path)
print(f"STEP 1 DONE: Extracted flights data (rows={len(df)})")
print(df.head())

log_event(
    f"Loaded {len(df)} rows from flights.csv",
    layer="E",
    component="extract",
    table_name="flights.csv",
    level="INFO",
)

log_event(
    "E",
    "extract",
    "flights.csv",
    "INFO",
    "Extracted flights data",
    {"rows": len(df)},
)

# ============================================================
# STEP 2 – TRANSFORM (clean & prepare)
# ============================================================
set_phase("TRANSFORMATION")
log_event(
    "Started transformation process for flights data",
    layer="T",
    component="transform",
    table_name="staging_flights",
    level="INFO",
)

# Table checker
def table_exists(table_name):
    try:
        supabase.table(table_name).select("*", count="exact").limit(1).execute()
        return True
    except Exception:
        return False

# Fetch valid airport keys
def fetch_airportkeys():
    if not table_exists("staging_airports"):
        print("⚠️ staging_airports table does NOT exist in Supabase.")
        return None

    res = supabase.table("staging_airports").select("airportkey").execute()

    if not res.data:
        print("⚠️ staging_airports exists but has NO DATA.")
        return None

    return [row["airportkey"].upper() for row in res.data]

# Fetch valid airline keys
def fetch_airlinekeys():
    if not table_exists("staging_airlines"):
        print("⚠️ staging_airlines table does NOT exist in Supabase.")
        return None

    res = supabase.table("staging_airlines").select("airlinekey").execute()

    if not res.data:
        print("⚠️ staging_airlines exists but has NO DATA.")
        return None

    return [row["airlinekey"].upper() for row in res.data]

# Fuzzy match helper
def fuzzy_fix(value, valid_list):
    if not valid_list or pd.isna(value):
        return value

    best_match = process.extractOne(value, valid_list, scorer=fuzz.WRatio)
    if best_match and best_match[1] >= 85:
        return best_match[0]
    return value

# Basic normalization
df["FlightKey"] = df["FlightKey"].astype(str).str.strip().str.upper()
df["OriginAirportKey"] = df["OriginAirportKey"].astype(str).str.strip().str.upper()
df["DestinationAirportKey"] = df["DestinationAirportKey"].astype(str).str.strip().str.upper()

df["AircraftType"] = (
    df["AircraftType"]
    .astype(str)
    .str.strip()
    .str.title()
    .str.replace(r"\s+", " ", regex=True)
)

log_event(
    "Basic normalization complete for FlightKey, OriginAirportKey, DestinationAirportKey, AircraftType",
    layer="T",
    component="transform",
    table_name="staging_flights",
    level="INFO",
)

# Fetch reference values
valid_airports = fetch_airportkeys()
valid_airlines = fetch_airlinekeys()

print(f"Fetched airport keys: {valid_airports}")
print(f"Fetched airline keys: {valid_airlines}")
log_event(
    "T",
    "references",
    "staging_airports/staging_airlines",
    "INFO",
    "Fetched reference keys",
    {
        "airports_loaded": bool(valid_airports),
        "airlines_loaded": bool(valid_airlines),
        "airport_count": len(valid_airports) if valid_airports else 0,
        "airline_count": len(valid_airlines) if valid_airlines else 0,
    },
)

# Origin and DestinationAirportKey validation using AirportKey
if valid_airports:
    df["OriginAirportKey"] = df["OriginAirportKey"].apply(lambda x: fuzzy_fix(x, valid_airports))
    df["DestinationAirportKey"] = df["DestinationAirportKey"].apply(lambda x: fuzzy_fix(x, valid_airports))
else:
    print("⚠️ Skipping fuzzy airport correction (no valid airports loaded).")

# Flightkey prefix validation using AirlineKey
def fix_flightkey_prefix(flightkey, airlinekeys):
    if pd.isna(flightkey):
        return flightkey

    # Extract prefix (first 2 alphanumeric)
    chars = [c for c in flightkey if c.isalnum()]
    if len(chars) < 2:
        return flightkey

    prefix = "".join(chars[:2]).upper()

    # If prefix already valid → OK
    if airlinekeys and prefix in airlinekeys:
        return flightkey

    # Try fuzzy correction
    if airlinekeys:
        new_prefix = fuzzy_fix(prefix, airlinekeys)
        if new_prefix != prefix:
            return new_prefix + flightkey[len(prefix):]

    return flightkey

if valid_airlines:
    df["FlightKey"] = df["FlightKey"].apply(lambda fk: fix_flightkey_prefix(fk, valid_airlines))
else:
    print("⚠️ Skipping airline validation (no airline data loaded).")

# ============================================================
# VALIDATION PHASE
# ============================================================
set_phase("VALIDATION")
log_event(
    "Starting validation process",
    layer="T",
    component="validation",
    table_name="staging_flights",
    level="INFO",
)

# Validation rules
duplicate_mask = df["FlightKey"].duplicated(keep="first")
flight_valid = df["FlightKey"].str.match(r"^[A-Za-z0-9]{2}\d+$")
origin_valid = df["OriginAirportKey"].str.match(r"^[A-Za-z]{3}$")
dest_valid = df["DestinationAirportKey"].str.match(r"^[A-Za-z]{3}$")
origin_dest_valid = df["OriginAirportKey"] != df["DestinationAirportKey"]

all_valid = flight_valid & origin_valid & dest_valid & origin_dest_valid & (~duplicate_mask)

df_clean = df[all_valid].copy()
df_quarantine = df[~all_valid].copy()

log_event(
    f"Quarantined {len(df_quarantine)} invalid or duplicate rows",
    layer="T",
    component="validation",
    table_name="staging_flights",
    level="INFO",
    details={"quarantine_rows": len(df_quarantine)},
)

# ============================================================
# TRANSFORM-CLEANED PHASE
# ============================================================
set_phase("TRANSFORM-CLEANED")
log_event(
    f"Prepared {len(df_clean)} cleaned rows for loading",
    layer="T",
    component="transform-cleaned",
    table_name="staging_flights",
    level="INFO",
)

print(f"STEP 2 DONE: Cleaned data (rows={len(df_clean)})")
print(f"VALID ROWS: {len(df_clean)}")
print(f"QUARANTINE ROWS: {len(df_quarantine)}")
log_event(
    "T",
    "transform",
    "staging_flights",
    "INFO",
    "Data transformed",
    {"valid_rows": len(df_clean), "quarantine_rows": len(df_quarantine)},
)

# ============================================================
# STEP 3 – LOAD (store into Supabase)
# ============================================================
set_phase("LOAD")
log_event(
    "Starting upload to Supabase",
    layer="L",
    component="load",
    table_name="staging_flights",
    level="INFO",
)

# Lowercase columns to fit Supabase naming convention
df_clean.columns = df_clean.columns.str.lower()

# Load the rows into Supabase
for _, row in df_clean.iterrows():
    record = {
        "flightkey": row["flightkey"],
        "originairportkey": row["originairportkey"],
        "destinationairportkey": row["destinationairportkey"],
        "aircrafttype": row["aircrafttype"],
    }
    supabase.table("staging_flights").upsert(record).execute()

print(f"STEP 3 DONE: Loaded {len(df_clean)} valid rows into Supabase.")
log_event(
    "L",
    "load",
    "staging_flights",
    "INFO",
    "Loaded valid rows into Supabase",
    {"loaded_rows": len(df_clean)},
)

# ============================================================
# SUMMARY PHASE
# ============================================================
set_phase("SUMMARY")
log_event(
    f"Total original rows: {len(df)}",
    layer="S",
    component="summary",
    table_name="staging_flights",
    level="INFO",
)
log_event(
    f"Valid cleaned rows: {len(df_clean)}",
    layer="S",
    component="summary",
    table_name="staging_flights",
    level="INFO",
)
log_event(
    f"Quarantined rows: {len(df_quarantine)}",
    layer="S",
    component="summary",
    table_name="staging_flights",
    level="INFO",
)
