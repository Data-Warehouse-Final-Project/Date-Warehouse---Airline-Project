import pandas as pd
import uuid
import io
import json
import numpy as np
import os
import sys
import shutil
import subprocess
from datetime import datetime
from supabase import create_client, Client

# Import your clean strategy
# Make sure this file exists at backend/cleaners/passengers.py
from cleaners.passengers import clean_passengers_data

# ============================================================
# 🔌 CONFIGURATION
# ============================================================
SUPABASE_URL = "https://eoyopmstogazaflwisbg.supabase.co"
# ⚠️ SECURITY TIP: Use os.getenv in production!
SUPABASE_KEY = "YOUR_KEY_HERE" 

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
RUN_ID = str(uuid.uuid4())

# ============================================================
# 🛠️ HELPER FUNCTIONS
# ============================================================

def log_to_supabase(level, message, details=None):
    """Structured Logger that writes to your Supabase log table"""
    print(f"[{level}] {message}")
    try:
        supabase.table("pipeline_run_log").insert({
            "run_id": RUN_ID,
            "event_time": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "details_json": json.dumps(details) if details else None
        }).execute()
    except Exception as e:
        print(f"⚠️ Internal Logging Failed: {e}")

def run_warehouse_pipeline(table_name: str):
    """
    Triggers the SQL Stored Procedure in Supabase to move data 
    from Staging -> Dimensions -> Facts.
    """
    try:
        log_to_supabase("INFO", f"🔄 Triggering SQL ETL for {table_name}...")
        
        # IMPORTANT: Make sure this name matches the function you created in SQL Editor!
        rpc_function_name = 'start_passenger_etl_pipeline' 
        
        # Execute the Stored Procedure
        response = supabase.rpc(rpc_function_name, {}).execute()
        
        log_to_supabase("SUCCESS", "✅ Warehouse SQL Pipeline executed successfully.")
        return True
        
    except Exception as e:
        log_to_supabase("ERROR", f"❌ SQL Pipeline Failed: {str(e)}")
        raise e

# ============================================================
# 🚀 MAIN PIPELINE
# ============================================================

def process_pipeline(file_path_or_buffer, output_cleaned_path=None):
    try:
        # --- STEP 1: EXTRACT ---
        log_to_supabase("INFO", "Starting Extraction")
        df = pd.read_csv(file_path_or_buffer)
        log_to_supabase("INFO", f"Extracted {len(df)} rows")

        # --- STEP 2: TRANSFORM (Pure Python Logic) ---
        log_to_supabase("INFO", "Starting Python Transformation")
        
        # Call the imported cleaner
        result = clean_passengers_data(df) 
        
        clean_df = result['clean']
        quarantine_df = result['quarantined']

        log_to_supabase("INFO", "Cleaning Complete", details={
            "clean_rows": len(clean_df),
            "quarantined_rows": len(quarantine_df)
        })

        # --- STEP 3: LOAD (Upload to Staging) ---
        if not clean_df.empty:
            log_to_supabase("INFO", "Uploading to Supabase Staging...")
            
            # Convert NaN to None for SQL compatibility
            records = clean_df.replace({np.nan: None}).to_dict(orient='records')
            
            # Upsert into staging
            supabase.table('staging_passengers').upsert(records).execute()
            log_to_supabase("SUCCESS", "Upload to Staging Complete")
            # Also optionally export cleaned CSV to provided path for downstream steps
            try:
                if output_cleaned_path:
                    clean_df.to_csv(output_cleaned_path, index=False, encoding='utf-8')
                    log_to_supabase("INFO", f"Exported cleaned CSV to {output_cleaned_path}")
            except Exception as e:
                log_to_supabase("WARNING", f"Failed to write cleaned CSV to {output_cleaned_path}: {e}")
            
            # --- STEP 4: TRIGGER SQL WAREHOUSE PIPELINE ---
            # This logic was missing from the execution flow in your snippet!
            run_warehouse_pipeline("passengers")

        else:
            log_to_supabase("WARNING", "No valid rows to upload.")

        # --- STEP 5: HANDLE QUARANTINE (Optional) ---
        if not quarantine_df.empty:
            log_to_supabase("WARNING", f"Found {len(quarantine_df)} invalid rows. Check logs.")
            # Optional: Upload quarantine_df to a 'quarantine_passengers' table here

    except Exception as e:
        log_to_supabase("FATAL", f"Pipeline Crashed: {str(e)}")
        raise e

# ============================================================
# 🏁 ENTRY POINT
# ============================================================
if __name__ == "__main__":
    # Usage: python -m staging_script.cleaners <input_path> [<output_cleaned_path>] [<table>]
    if len(sys.argv) < 2:
        print("Usage: python -m staging_script.cleaners <input_path> [<output_cleaned_path>] [<table>]")
        sys.exit(2)
    inp = sys.argv[1]
    outp = sys.argv[2] if len(sys.argv) > 2 else None
    table_arg = sys.argv[3] if len(sys.argv) > 3 else None

    # If a table arg is provided, prefer dispatching to the unified cleaning pipelines
    if table_arg:
        # map incoming table names to the unified pipeline CLI name used in cleaning.py
        mapping = {
            'facttravelagency': 'facttravel',
            'facttravel': 'facttravel',
            'fact': 'facttravel',
            'passengers': 'passengers',
            'passenger': 'passengers',
            'airports': 'airports',
            'airport': 'airports',
            'airlines': 'airlines',
            'airline': 'airlines',
            'flights': 'flights',
            'flight': 'flights'
        }
        pipeline = mapping.get(table_arg.lower())
        cleaning_py = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'cleaning.py'))
        if pipeline and os.path.exists(cleaning_py):
            try:
                cmd = [sys.executable, cleaning_py, pipeline, inp]
                print(f"Invoking unified pipeline: {' '.join(cmd)}")
                # Run in the etl_scripts directory so cleaned files are written there
                proc = subprocess.run(cmd, cwd=os.path.dirname(cleaning_py))
                if proc.returncode != 0:
                    print(f"Unified pipeline returned code {proc.returncode}", file=sys.stderr)
                    sys.exit(proc.returncode)

                # Map pipeline -> expected cleaned filename (if created by the pipeline)
                cleaned_name_map = {
                    'facttravel': 'cleaned_travel_agency_sales.csv',
                    'passengers': 'cleaned_passengers.csv',
                    'airports': 'cleaned_airports.csv',
                    'airlines': 'cleaned_airlines.csv',
                    'flights': 'cleaned_flights.csv'
                }
                expected = cleaned_name_map.get(pipeline)
                if expected and outp:
                    src = os.path.join(os.path.dirname(cleaning_py), expected)
                    if os.path.exists(src):
                        shutil.copyfile(src, outp)
                        print(f"Copied cleaned file {src} -> {outp}")
                        sys.exit(0)

                # Fallback: copy input -> outp if requested
                if outp:
                    shutil.copyfile(inp, outp)
                sys.exit(0)
            except Exception as e:
                print(f"Unified pipeline invocation failed: {e}", file=sys.stderr)
                sys.exit(1)

    # Default behavior: run the existing passenger-focused pipeline (backwards compatible)
    process_pipeline(inp, outp)