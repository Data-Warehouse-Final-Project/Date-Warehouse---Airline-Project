"""
unified_pipelines.py

Contains:
- run_airports_pipeline(input_path="airports.csv")
- run_airlines_pipeline(input_path="airline.csv")
- run_flights_pipeline(input_path="flights.csv")
- run_facttravelagency_pipeline(input_path="travel__agency_sales_001.csv")
- clean_passengers_data(df: pd.DataFrame) -> dict
- run_passengers_pipeline(input_path="passengers.csv")

CLI:
    python unified_pipelines.py airports path/to/airports.csv
    python unified_pipelines.py airlines path/to/airline.csv
    python unified_pipelines.py flights path/to/flights.csv
    python unified_pipelines.py facttravel path/to/travel__agency_sales_001.csv
    python unified_pipelines.py passengers path/to/passengers.csv
"""

import uuid
import json
import os
import sys
from datetime import datetime

import pandas as pd
import numpy as np
from supabase import create_client, Client
from rapidfuzz import process, fuzz


# ============================================================
# AIRPORTS PIPELINE (from airports.py)
# ============================================================

def run_airports_pipeline(input_path: str = "airports.csv"):
    # ============================================================
    # logging_module logic (with Supabase) INLINE
    # ============================================================
    SUPABASE_URL = "https://zitrseennwgvdzmmavdt.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InppdHJzZWVubndndmR6bW1hdmR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUxOTQ1MTIsImV4cCI6MjA4MDc3MDUxMn0.SBVVYD3g4enewAKmnyAgEzz2Lxdj5muhTILFqCnjCv4"

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    RUN_ID = str(uuid.uuid4())
    LOG_FILE_PATH = f"pipeline_run_log_airports_{RUN_ID}.txt"
    CURRENT_PHASE = None

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
        nonlocal CURRENT_PHASE
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
        f"Started loading {input_path}",
        component="airports",
        table_name="airports_raw"
    )

    try:
        df = pd.read_csv(input_path, encoding="utf-8-sig", on_bad_lines="skip")
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
        return

    # ---------- TRANSFORMATION ----------
    set_phase("TRANSFORMATION")
    log_event(
        "Cleaning whitespace, internal spaces, and standardizing text for airports.",
        component="airports",
        table_name="staging_airports"
    )

    cols_to_strip = ["AirportKey", "AirportName", "City", "Country"]
    for col in cols_to_strip:
        df[col] = df[col].astype(str).str.strip()

    for col in ["AirportName", "City", "Country"]:
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)

    df["AirportKey"] = df["AirportKey"].str.upper()
    df["AirportName"] = df["AirportName"].str.title()
    df["City"] = df["City"].str.title()
    df["Country"] = df["Country"].str.title()

    df["City"] = df["City"].str.replace(r"'S\b", "'s", regex=True)
    df["AirportName"] = df["AirportName"].str.replace(r"'S\b", "'s", regex=True)

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

    MISSING_STRINGS = {"", "nan", "none", "null", "NaN", "None", "Null"}

    def is_missing(series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        return s.isin(MISSING_STRINGS)

    airportkey_missing = is_missing(df["AirportKey"])
    airportkey_format_valid = df["AirportKey"].str.match(r"^[A-Z]{3}$", na=False)
    airportkey_valid = (~airportkey_missing) & airportkey_format_valid

    name_missing = is_missing(df["AirportName"])
    city_missing = is_missing(df["City"])
    country_missing = is_missing(df["Country"])

    name_valid = ~name_missing
    city_valid = ~city_missing
    country_valid = ~country_missing

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

    # ---------- LOAD ----------
    set_phase("LOAD")

    column_map = {
        "AirportKey": "airportkey",
        "AirportName": "airportname",
        "City": "city",
        "Country": "country",
    }
    clean_for_supabase = clean_df.rename(columns=column_map)

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

    print("✔ Cleaning Pipeline Completed (AIRPORTS)")
    print(f"   Clean rows      : {len(clean_df)}")
    print(f"   Quarantine rows : {len(quarantine_df)}")


# ============================================================
# AIRLINES PIPELINE (from airlines.py)
# ============================================================

def run_airlines_pipeline(input_path: str = r"C:\Users\Randy\airline-project\raw-data\airline.csv"):
    SUPABASE_URL = "https://eoyopmstogazaflwisbg.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVveW9wbXN0b2dhemFmbHdpc2JnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI3NjM5OTUsImV4cCI6MjA3ODMzOTk5NX0.JJC9myvRVLAk_7OSW57JtnbcMyB1xb2F0cDrbOpOZi8"

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    RUN_ID = str(uuid.uuid4())
    LOG_FILE_PATH = f"pipeline_run_log_airlines_{RUN_ID}.txt"
    CURRENT_PHASE = None

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
            supabase.table("pipeline_run_log").insert(record).execute()
        except Exception as e:
            print("Warning: Failed to write log to Supabase:", str(e))
            if not log_table_exists():
                print("Run this SQL in Supabase SQL editor to create the log table:")
                print(CREATE_LOG_TABLE_SQL)

    def _timestamp():
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def set_phase(phase_name):
        nonlocal CURRENT_PHASE
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

    def table_exists(table_name):
        try:
            supabase.table(table_name).select("*", count="exact").limit(1).execute()
            return True
        except Exception:
            return False

    # ---------- EXTRACT ----------
    set_phase("EXTRACT")
    log_event("Starting airlines pipeline", table_name="airline.csv", details={"run_id": RUN_ID})

    try:
        df = pd.read_csv(input_path)
        log_event(f"STEP 1 DONE: Extracted {len(df)} rows from {input_path}", table_name="airline.csv")
        log_event("Data preview:", table_name="airline.csv")
        print(df.head())
        log_event("File loaded successfully", table_name="airline.csv",
                  details={"rows": len(df), "columns": df.columns.tolist()})
    except Exception as e:
        error_msg = f"Failed to read file: {str(e)}"
        log_event(f"ERROR: {error_msg}", table_name="airline.csv", level="ERROR", details={"file_path": input_path})
        print(f"ERROR: {error_msg}")
        return

    # ---------- TRANSFORMATION ----------
    set_phase("TRANSFORMATION")

    def fetch_existing_airlinekeys():
        if not table_exists("staging_airlines"):
            log_event("staging_airlines table not found", component="references", table_name="staging_airlines",
                      level="INFO")
            return []

        try:
            res = supabase.table("staging_airlines").select("airlinekey").execute()
            return [row["airlinekey"].upper() for row in res.data] if res.data else []
        except Exception as e:
            log_event("Failed to fetch existing keys", component="references", table_name="staging_airlines",
                      level="WARN", details={"error": str(e)})
            return []

    log_event("Cleaning column names...", table_name="airline.csv")
    df.columns = df.columns.str.strip().str.lower()
    log_event("Standardized column names", table_name="airline.csv",
              details={"columns": df.columns.tolist()})

    required_columns = ['airlinekey', 'airlinename', 'alliance']
    for col in required_columns:
        if col not in df.columns:
            if col == 'airlinename' and 'airlinekey' in df.columns:
                df['airlinename'] = df['airlinekey']
                log_event(f"Warning: Created '{col}' column from airlinekey", table_name="staging_airlines",
                          level="WARN")
            elif col == 'alliance':
                df['alliance'] = 'None'
                log_event(f"Warning: Created '{col}' column with default 'None'", table_name="staging_airlines",
                          level="WARN")
            else:
                log_event(f"ERROR: Missing required column: {col}", table_name="staging_airlines", level="ERROR")
                print(f"ERROR: Missing required column: {col}")
                return

    df["airlinekey"] = df["airlinekey"].astype(str).str.strip().str.upper()
    df["airlinename"] = df["airlinename"].astype(str).str.strip().str.title()
    df["airlinename"] = df["airlinename"].str.replace(r"\s+", " ", regex=True)

    df["alliance"] = df["alliance"].fillna('None')
    df["alliance"] = df["alliance"].astype(str).str.strip()

    alliance_mapping = {
        '': 'None',
        'nan': 'None',
        'none': 'None',
        'oneworld': 'Oneworld',
        'sky team': 'SkyTeam',
        'skyteam': 'SkyTeam',
        'star alliance': 'Star Alliance',
        'staralliance': 'Star Alliance'
    }
    df["alliance"] = df["alliance"].replace(alliance_mapping)
    df["alliance"] = df["alliance"].fillna('None')

    valid_alliances = ['Oneworld', 'SkyTeam', 'Star Alliance', 'None']
    df["alliance"] = df["alliance"].apply(
        lambda x: x if x in valid_alliances else 'None'
    )

    alliance_fixes = {
        'VS': 'SkyTeam',
        'AZ': 'None'
    }

    for airline_key, correct_alliance in alliance_fixes.items():
        mask = df["airlinekey"] == airline_key
        if mask.any():
            df.loc[mask, "alliance"] = correct_alliance
            log_event(f"Fixed alliance for {airline_key}: {correct_alliance}",
                      table_name="staging_airlines",
                      details={"airlinekey": airline_key, "new_alliance": correct_alliance})

    airlinekey_valid = df["airlinekey"].str.match(r"^[A-Z0-9]{2,3}$")
    airlinename_valid = df["airlinename"].str.match(r"^[A-Za-z0-9\s\.\-\&]+$")
    duplicate_mask = df["airlinekey"].duplicated(keep="first")

    all_valid = airlinekey_valid & airlinename_valid & (~duplicate_mask)
    df_clean = df[all_valid].copy()
    df_quarantine = df[~all_valid].copy()

    log_event(f"STEP 2 DONE: Cleaned {len(df_clean)} valid rows", table_name="staging_airlines")
    log_event(f"Quarantined: {len(df_quarantine)} rows", table_name="staging_airlines", level="WARN")

    log_event("Unique alliance values after cleaning:", table_name="staging_airlines")
    print(df_clean["alliance"].value_counts())

    if not df_quarantine.empty:
        log_event("Quarantined rows:", table_name="staging_airlines", level="WARN")
        print(df_quarantine[["airlinekey", "airlinename", "alliance"]].to_string())
        log_event("Found invalid rows", component="quarantine", table_name="staging_airlines",
                  level="WARN", details={"count": len(df_quarantine)})

    log_event("Data cleaning completed", table_name="staging_airlines",
              details={"valid": len(df_clean), "quarantine": len(df_quarantine)})

    # ---------- LOAD ----------
    set_phase("LOAD")

    if not table_exists("staging_airlines"):
        log_event("staging_airlines table does not exist in Supabase!", table_name="staging_airlines", level="ERROR")
        print("ERROR: staging_airlines table does not exist in Supabase!")
        print("Please create the table first with this SQL:")
        print("""
        CREATE TABLE staging_airlines (
            airlinekey VARCHAR(3) PRIMARY KEY,
            airlinename VARCHAR(100) NOT NULL,
            alliance alliance NOT NULL
        );
        """)
        return

    log_event("Clearing existing data from staging_airlines...", table_name="staging_airlines")
    try:
        supabase.table("staging_airlines").delete().neq("airlinekey", "___dummy___").execute()
        log_event("Existing data cleared", table_name="staging_airlines")
    except Exception as e:
        log_event(f"Warning: Could not clear table (might be empty): {str(e)}", table_name="staging_airlines",
                  level="WARN")

    log_event("Uploading to staging_airlines...", table_name="staging_airlines")

    log_event("First 5 records being uploaded:", table_name="staging_airlines")
    for _, row in df_clean.head(5).iterrows():
        log_event(f"  {row['airlinekey']}: '{row['airlinename']}', alliance: '{row['alliance']}'",
                  table_name="staging_airlines")

    successful = 0
    failed = []

    for _, row in df_clean.iterrows():
        try:
            alliance_value = row["alliance"]
            if alliance_value not in ['Oneworld', 'SkyTeam', 'Star Alliance', 'None']:
                alliance_value = 'None'

            record = {
                "airlinekey": row["airlinekey"],
                "airlinename": row["airlinename"],
                "alliance": alliance_value
            }
            supabase.table("staging_airlines").upsert(record).execute()
            successful += 1
            if successful % 10 == 0:
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

    # ---------- VALIDATION ----------
    set_phase("VALIDATION")
    log_event("Verifying upload...", table_name="staging_airlines")
    try:
        result = supabase.table("staging_airlines").select("count", count="exact").execute()
        db_count = result.count if hasattr(result, 'count') else len(result.data)
        log_event(f"Total records in database: {db_count}", table_name="staging_airlines")

        sample = supabase.table("staging_airlines").select("*").limit(5).execute()
        if sample.data:
            log_event("Sample from database:", table_name="staging_airlines")
            for row in sample.data:
                log_event(f"  {row['airlinekey']}: {row['airlinename']} - Alliance: {row['alliance']}",
                          table_name="staging_airlines")
    except Exception as e:
        log_event(f"Warning: Could not verify: {str(e)}", table_name="staging_airlines", level="WARN")

    # ---------- SUMMARY ----------
    set_phase("SUMMARY")
    log_event("=" * 60, table_name="staging_airlines")
    log_event("AIRLINES CLEANING PIPELINE COMPLETED", table_name="staging_airlines")
    log_event("=" * 60, table_name="staging_airlines")
    log_event("Summary:", table_name="staging_airlines")
    log_event(f"  Total processed: {len(df)} rows", table_name="staging_airlines")
    log_event(f"  Successfully uploaded: {successful} rows", table_name="staging_airlines")
    log_event(f"  Quarantined: {len(df_quarantine)} rows", table_name="staging_airlines")
    log_event(f"  Failed uploads: {len(failed)} rows", table_name="staging_airlines")
    log_event(f"Log file: {LOG_FILE_PATH}", table_name="staging_airlines")
    log_event("=" * 60, table_name="staging_airlines")

    output_file = "cleaned_airlines.csv"
    df_clean.to_csv(output_file, index=False)
    log_event(f"Clean data also saved to: {output_file}", table_name="staging_airlines")

    log_event("Pipeline completed", table_name="staging_airlines",
              details={"total": len(df), "uploaded": successful, "quarantine": len(df_quarantine), "failed": len(failed)})

    print("\n" + "=" * 60)
    print("AIRLINES CLEANING PIPELINE COMPLETED")
    print("=" * 60)
    print(f"Summary:")
    print(f"  Total processed: {len(df)} rows")
    print(f"  Successfully uploaded: {successful} rows")
    print(f"  Quarantined: {len(df_quarantine)} rows")
    print(f"  Failed uploads: {len(failed)} rows")
    print(f"Log file: {LOG_FILE_PATH}")
    print("=" * 60)
    print(f"Clean data also saved to: {output_file}")

# ============================================================
# PASSENGERS CLEANING FUNCTION (from passengers.py)
# ============================================================

def clean_passengers_data(df: pd.DataFrame) -> dict:
    """
    Takes a raw DataFrame, cleans it, validates it, and separates invalid rows.
    Returns a dictionary: {'clean': pd.DataFrame, 'quarantined': pd.DataFrame}
    """

    original_df = df.copy()

    def remove_key_from_email(email, raw_key):
        key_digits = ''.join(filter(str.isdigit, str(raw_key)))
        if key_digits:
            email = email.replace(key_digits, '')
            try:
                no_leading_zeros = str(int(key_digits))
                email = email.replace(no_leading_zeros, '')
            except ValueError:
                pass
        return email

    df['Email'] = df['Email'].astype(str).str.lower().str.strip()
    df['Email'] = df.apply(lambda row: remove_key_from_email(row['Email'], row['PassengerKey']), axis=1)

    df['PassengerKey'] = df['PassengerKey'].astype(str).str.strip()
    df['FullName'] = df['FullName'].astype(str).str.strip().replace(r'\s+', ' ', regex=True).str.title()

    valid_statuses = ['Bronze', 'Silver', 'Gold', 'Platinum']
    df['LoyaltyStatus'] = df['LoyaltyStatus'].astype(str).str.strip().str.replace(r'[^a-zA-Z]', '', regex=True).str.capitalize()

    required = ['PassengerKey', 'FullName', 'Email', 'LoyaltyStatus']

    missing_data_idx = df[df[required].isnull().any(axis=1)].index

    dup_cols = ['FullName', 'Email', 'LoyaltyStatus']
    duplicates_idx = df[df.duplicated(subset=dup_cols, keep='first')].index

    invalid_conditions = (
        df['PassengerKey'].isnull() |
        ~df['FullName'].astype(str).str.match(r'^[A-Za-z]+(?:\s+[A-Za-z]+)+$', na=False) |
        ~df['Email'].str.match(r'^[a-z0-9]+(?:[._][a-z0-9]+)*@example\.com$', na=False) |
        ~df['LoyaltyStatus'].isin(valid_statuses)
    )
    invalid_logic_idx = df[invalid_conditions].index

    invalid_idx_all = set(missing_data_idx) | set(duplicates_idx) | set(invalid_logic_idx)

    quarantined_df = original_df.loc[sorted(invalid_idx_all)].reset_index(drop=True)
    clean_df = df.drop(list(invalid_idx_all), errors='ignore').reset_index(drop=True)

    if not clean_df.empty:
        clean_df['PassengerKey'] = [f"P{1000 + n}" for n in range(1, len(clean_df) + 1)]

    clean_df = clean_df[['PassengerKey', 'FullName', 'Email', 'LoyaltyStatus']]

    clean_df = clean_df.rename(columns={
        "PassengerKey": "passengerkey",
        "FullName": "fullname",
        "Email": "email",
        "LoyaltyStatus": "loyaltystatus"
    })

    return {
        "clean": clean_df,
        "quarantined": quarantined_df
    }


def run_passengers_pipeline(input_path: str = "passengers.csv"):
    df = pd.read_csv(input_path)
    result = clean_passengers_data(df)
    clean_df = result["clean"]
    quarantine_df = result["quarantined"]

    clean_df.to_csv("cleaned_passengers.csv", index=False, encoding="utf-8-sig")
    quarantine_df.to_csv("quarantined_passengers.csv", index=False, encoding="utf-8-sig")

    print("✔ Passengers cleaning completed")
    print(f"   Clean rows      : {len(clean_df)}")
    print(f"   Quarantine rows : {len(quarantine_df)}")

# ============================================================
# FLIGHTS PIPELINE (from flights.py)
# ============================================================

def run_flights_pipeline(input_path: str = "flights.csv"):
    SUPABASE_URL = "https://zitrseennwgvdzmmavdt.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InppdHJzZWVubndndmR6bW1hdmR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUxOTQ1MTIsImV4cCI6MjA4MDc3MDUxMn0.SBVVYD3g4enewAKmnyAgEzz2Lxdj5muhTILFqCnjCv4"

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    RUN_ID = str(uuid.uuid4())
    LOG_FILE_PATH = f"pipeline_run_log_flights_{RUN_ID}.txt"
    CURRENT_PHASE = None

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
            supabase.table("pipeline_run_log").insert(record).execute()
        except Exception as e:
            print("⚠️ Failed to write log to Supabase:", str(e))
            if not log_table_exists():
                print("Run this SQL in Supabase SQL editor to create the log table:")
                print(CREATE_LOG_TABLE_SQL)

    def _timestamp():
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def set_phase(phase_name):
        nonlocal CURRENT_PHASE
        CURRENT_PHASE = phase_name.upper()
        header = f"\n==================== {CURRENT_PHASE} ====================\n"

        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(header)
        print(header.strip())

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
        table_name = "pipeline"
        message = f"--- Entering phase: {CURRENT_PHASE} ---"
        log_event_supabase(layer, component, table_name, "INFO", message)

    def log_event(*args, **kwargs):
        layer_map = {
            "EXTRACT": "E",
            "TRANSFORMATION": "T",
            "VALIDATION": "T",
            "TRANSFORM-CLEANED": "T",
            "LOAD": "L",
            "SUMMARY": "S"
        }

        if len(args) >= 5 and isinstance(args[0], str) and isinstance(args[4], str):
            layer, component, table_name, level, message = args[:5]
            details = args[5] if len(args) > 5 else kwargs.get("details")
        else:
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

        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(line)
        print(line.strip())

        sup_layer = layer or layer_map.get(CURRENT_PHASE, "X")
        sup_component = component or (CURRENT_PHASE.lower() if CURRENT_PHASE else "pipeline")
        sup_table_name = table_name or "pipeline"
        log_event_supabase(sup_layer, sup_component, sup_table_name, level, message, details)

    # ---------- EXTRACT ----------

    set_phase("EXTRACT")

    log_event("E", "extract", "flights.csv", "INFO", "Started pipeline run", {"rows_in_source": None})

    log_event(
        f"Started loading {input_path}",
        layer="E",
        component="extract",
        table_name="flights.csv",
        level="INFO",
    )

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

    # ---------- TRANSFORMATION ----------
    set_phase("TRANSFORMATION")
    log_event(
        "Started transformation process for flights data",
        layer="T",
        component="transform",
        table_name="staging_flights",
        level="INFO",
    )

    def table_exists(table_name):
        try:
            supabase.table(table_name).select("*", count="exact").limit(1).execute()
            return True
        except Exception:
            return False

    def fetch_airportkeys():
        if not table_exists("staging_airports"):
            print("⚠️ staging_airports table does NOT exist in Supabase.")
            return None

        res = supabase.table("staging_airports").select("airportkey").execute()

        if not res.data:
            print("⚠️ staging_airports exists but has NO DATA.")
            return None

        return [row["airportkey"].upper() for row in res.data]

    def fetch_airlinekeys():
        if not table_exists("staging_airlines"):
            print("⚠️ staging_airlines table does NOT exist in Supabase.")
            return None

        res = supabase.table("staging_airlines").select("airlinekey").execute()

        if not res.data:
            print("⚠️ staging_airlines exists but has NO DATA.")
            return None

        return [row["airlinekey"].upper() for row in res.data]

    def fuzzy_fix(value, valid_list):
        if not valid_list or pd.isna(value):
            return value

        best_match = process.extractOne(value, valid_list, scorer=fuzz.WRatio)
        if best_match and best_match[1] >= 85:
            return best_match[0]
        return value

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

    if valid_airports:
        df["OriginAirportKey"] = df["OriginAirportKey"].apply(lambda x: fuzzy_fix(x, valid_airports))
        df["DestinationAirportKey"] = df["DestinationAirportKey"].apply(lambda x: fuzzy_fix(x, valid_airports))
    else:
        print("⚠️ Skipping fuzzy airport correction (no valid airports loaded).")

    def fix_flightkey_prefix(flightkey, airlinekeys):
        if pd.isna(flightkey):
            return flightkey

        chars = [c for c in flightkey if c.isalnum()]
        if len(chars) < 2:
            return flightkey

        prefix = "".join(chars[:2]).upper()

        if airlinekeys and prefix in airlinekeys:
            return flightkey

        if airlinekeys:
            new_prefix = fuzzy_fix(prefix, airlinekeys)
            if new_prefix != prefix:
                return new_prefix + flightkey[len(prefix):]

        return flightkey

    if valid_airlines:
        df["FlightKey"] = df["FlightKey"].apply(lambda fk: fix_flightkey_prefix(fk, valid_airlines))
    else:
        print("⚠️ Skipping airline validation (no airline data loaded).")

    # ---------- VALIDATION ----------
    set_phase("VALIDATION")
    log_event(
        "Starting validation process",
        layer="T",
        component="validation",
        table_name="staging_flights",
        level="INFO",
    )

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

    # ---------- TRANSFORM-CLEANED ----------
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

    # ---------- LOAD ----------
    set_phase("LOAD")
    log_event(
        "Starting upload to Supabase",
        layer="L",
        component="load",
        table_name="staging_flights",
        level="INFO",
    )

    df_clean.columns = df_clean.columns.str.lower()

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

    # ---------- SUMMARY ----------
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

    print("✔ Cleaning Pipeline Completed (FLIGHTS)")
    print(f"   Clean rows      : {len(df_clean)}")
    print(f"   Quarantine rows : {len(df_quarantine)}")

# ============================================================
# FACT TRAVEL AGENCY PIPELINE (from facttravelagency.py)
# ============================================================

def run_facttravelagency_pipeline(input_path: str = r"C:\Users\Administrator\Downloads\travel__agency_sales_001.csv"):
    SUPABASE_URL = "https://eoyopmstogazaflwisbg.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVveW9wbXN0b2dhemFmbHdpc2JnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI3NjM5OTUsImV4cCI6MjA3ODMzOTk5NX0.JJC9myvRVLAk_7OSW57JtnbcMyB1xb2F0cDrbOpOZi8"

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    RUN_ID = str(uuid.uuid4())
    LOG_FILE_PATH = f"pipeline_run_log_facttravel_{RUN_ID}.txt"
    CURRENT_PHASE = None

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
            supabase.table("pipeline_run_log").insert(record).execute()
        except Exception as e:
            print("⚠️ Failed to write log to Supabase:", str(e))
            if not log_table_exists():
                print("Run this SQL in Supabase SQL editor to create the log table:")
                print(CREATE_LOG_TABLE_SQL)

    def _timestamp():
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def set_phase(phase_name):
        nonlocal CURRENT_PHASE
        CURRENT_PHASE = phase_name.upper()
        header = f"\n# {CURRENT_PHASE}\n"

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

        if "Started pipeline run" in message or "Started transformation process" in message or "Starting validation process" in message:
            line = f"\n{line}"

        with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
            f.write(line)
        print(line.strip())

        layer_map = {"EXTRACT": "E", "TRANSFORMATION": "T", "VALIDATION": "T",
                     "TRANSFORM-CLEANED": "T", "LOAD": "L", "SUMMARY": "S"}
        sup_layer = layer or layer_map.get(CURRENT_PHASE, "X")
        sup_component = component or (CURRENT_PHASE.lower() if CURRENT_PHASE else "pipeline")
        sup_table_name = table_name or "pipeline"
        log_event_supabase(sup_layer, sup_component, sup_table_name, level, message, details)

    print(f"DEBUG: Creating log file at: {os.path.abspath(LOG_FILE_PATH)}")

    # ---------- EXTRACT ----------
    set_phase("EXTRACT")

    df = pd.read_csv(input_path, dtype=str)
    df.columns = df.columns.str.lower()

    log_event("Started pipeline run")
    log_event(f"Started pipeline {os.path.basename(input_path)}")
    log_event(f"Loaded {len(df)} rows from {os.path.basename(input_path)}")
    log_event("Extracted travel agency sales data")

    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write("\n---\n\n")

    print(f"STEP 1 DONE: Extracted {len(df)} rows.")

    # ---------- TRANSFORMATION ----------
    set_phase("TRANSFORMATION")
    log_event("Started transformation process for travel agency sales data")

    is_numeric = df['transactionid'].apply(lambda x: str(x).isdigit())
    non_numeric_mask = ~is_numeric
    df['transactionid_fixed'] = df['transactionid'].copy()
    try:
        numeric_ids = df.loc[is_numeric, 'transactionid_fixed'].astype(int)
        start_id = numeric_ids.max()
    except Exception:
        start_id = 40000
    df['transactionid_fixed'] = pd.to_numeric(df['transactionid_fixed'], errors='coerce')
    df['prev_id'] = df['transactionid_fixed'].ffill()
    df.loc[non_numeric_mask, 'transactionid_fixed'] = df.loc[non_numeric_mask, 'prev_id'].astype(int) + 1
    df['transactionid'] = df['transactionid_fixed'].astype(int).astype(str)
    df = df.drop(columns=['transactionid_fixed', 'prev_id'])

    log_event("Basic normalization complete for travel__agency_sales_001.csv, transactionid, passengerid, flightid")
    log_event("Fetched reference keys")

    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write("\n---\n\n")

    numeric_cols = ['ticketprice', 'taxes', 'baggagefees', 'totalamount']
    df[numeric_cols] = df[numeric_cols].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
    for col in numeric_cols:
        df[col] = df[col].round(2).clip(upper=99999999.99)

    row_sum = df[['ticketprice', 'taxes', 'baggagefees']].sum(axis=1).round(2)
    totals_match = (row_sum == df['totalamount'].round(2))
    num_total_errors = (~totals_match).sum()

    def to_standard_date(x):
        try:
            val = str(x).strip().replace('-', '/').title()
            dt = pd.to_datetime(val, format='%Y/%b/%d', errors='raise')
            return int(dt.strftime('%Y%m%d'))
        except Exception:
            try:
                dt = pd.to_datetime(val, errors='raise', infer_datetime_format=True)
                return int(dt.strftime('%Y%m%d'))
            except Exception:
                return np.nan

    df['transactiondate'] = df['transactiondate'].apply(to_standard_date)

    # ---------- VALIDATION ----------
    set_phase("VALIDATION")
    log_event("Starting validation process")

    exact_dupes_mask = df.duplicated(keep='first')
    id_dupes_mask = df['transactionid'].duplicated(keep='first')
    duplicate_mask = exact_dupes_mask | id_dupes_mask

    patterns = {
        'transactionid': r'^4\d{4}$',
        'passengerid': r'^P[0-8]\d{4}$',
        'flightid': r'^[A-Z]{1,2}\d{1,5}$'
    }
    invalid_mask = (
        df[list(patterns)].isna().any(axis=1) |
        ~df['transactionid'].str.match(patterns['transactionid'], na=False) |
        ~df['passengerid'].str.match(patterns['passengerid'], na=False) |
        ~df['flightid'].str.match(patterns['flightid'], na=False) |
        (~totals_match) |
        df['transactiondate'].isna() |
        duplicate_mask
    )

    df_quarantine = df[invalid_mask].copy()
    df_cleaned = df[~invalid_mask].copy()

    log_event(f"Quarantined {len(df_quarantine)} invalid or duplicate rows")

    with open(LOG_FILE_PATH, "a", encoding="utf-8") as f:
        f.write("\n---\n\n")

    # ---------- TRANSFORM-CLEANED ----------
    set_phase("TRANSFORM-CLEANED")
    log_event(f"Prepared {len(df_cleaned)} cleaned rows for loading")
    log_event("Data transformed")

    print(f"STEP 2 DONE: Cleaned={len(df_cleaned)} | Quarantine={len(df_quarantine)}")

    # ---------- LOAD ----------
    set_phase("LOAD")

    df_cleaned.fillna('').to_csv("cleaned_travel_agency_sales.csv", index=False, encoding='utf-8-sig')
    df_quarantine.fillna('').to_csv("quarantined_travel_agency_sales.csv", index=False, encoding='utf-8-sig')

    log_event("Starting upload to Supabase")

    print("\nCleaning and Export Complete!")
    print(f"Cleaned rows: {len(df_cleaned)} | Quarantined rows: {len(df_quarantine)}")

    loaded_count = 0
    for _, row in df_cleaned.iterrows():
        record = {
            "transactionid": row["transactionid"],
            "transactiondate": row["transactiondate"],
            "passengerid": row["passengerid"],
            "flightid": row["flightid"],
            "ticketprice": row["ticketprice"],
            "taxes": row["taxes"],
            "baggagefees": row["baggagefees"],
            "totalamount": row["totalamount"],
        }
        try:
            supabase.table("staging_facttravelagencysales_source2_agency").upsert(record).execute()
            loaded_count += 1
        except Exception as e:
            log_event(f"Failed to upsert row {row['transactionid']}: {str(e)}",
                      level="ERROR")

    log_event(f"Upserted {loaded_count} rows into Supabase")

    # ---------- SUMMARY ----------
    set_phase("SUMMARY")
    log_event("Pipeline execution completed successfully",
              layer="S",
              component="summary",
              table_name="pipeline",
              level="INFO",
              details={
                  "total_rows_processed": len(df),
                  "cleaned_rows": len(df_cleaned),
                  "quarantine_rows": len(df_quarantine),
                  "loaded_rows": loaded_count,
                  "run_id": RUN_ID,
                  "log_file": LOG_FILE_PATH
              })

    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION COMPLETE (FACT TRAVEL AGENCY)")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print(f"Log file location: {os.path.abspath(LOG_FILE_PATH)}")
    print(f"Log file contents preview:")
    print("=" * 60)

    try:
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
            print(f.read())
    except Exception as e:
        print(f"Could not read log file: {e}")

    print("=" * 60)
    print(f"Total rows processed: {len(df)}")
    print(f"Cleaned rows: {len(df_cleaned)}")
    print(f"Quarantined rows: {len(df_quarantine)}")
    print(f"Loaded to Supabase: {loaded_count}")
    print("=" * 60)

# ============================================================
# SIMPLE CLI DISPATCH
# ============================================================

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python unified_pipelines.py airports [airports.csv]")
        print("  python unified_pipelines.py airlines [airline.csv]")
        print("  python unified_pipelines.py flights [flights.csv]")
        print("  python unified_pipelines.py facttravel [travel__agency_sales_001.csv]")
        print("  python unified_pipelines.py passengers [passengers.csv]")
        return

    pipeline = sys.argv[1].lower()
    input_path = sys.argv[2] if len(sys.argv) >= 3 else None

    if pipeline == "airports":
        run_airports_pipeline(input_path or "airports.csv")
    elif pipeline == "airlines":
        run_airlines_pipeline(input_path or r"C:\Users\Randy\airline-project\raw-data\airline.csv")
    elif pipeline == "flights":
        run_flights_pipeline(input_path or "flights.csv")
    elif pipeline in ("facttravel", "facttravelagency", "fact"):
        run_facttravelagency_pipeline(input_path or r"C:\Users\Administrator\Downloads\travel__agency_sales_001.csv")
    elif pipeline == "passengers":
        run_passengers_pipeline(input_path or "passengers.csv")
    else:
        print(f"Unknown pipeline: {pipeline}")
        print("Valid options: airports, airlines, flights, facttravel, passengers")


if __name__ == "__main__":
    main()
