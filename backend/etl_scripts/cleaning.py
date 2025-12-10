# ============================================================
# IMPORTS
# ============================================================
import pandas as pd
import numpy as np
import uuid
import json
import os
from datetime import datetime
from supabase import create_client
import pycountry
import requests
import time

# ============================================================
# LOGGER CLASS
# ============================================================
class Logger:
    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        self.run_id = str(uuid.uuid4())  # Generate unique run ID for each log
        self.log_file_path = f"etl_log_{self.run_id}.txt"  # Log file path
        self.current_phase = None

        if supabase_url and supabase_key:
            self.supabase = create_client(supabase_url, supabase_key)  # Supabase client for logging
        else:
            self.supabase = None

    def _timestamp(self):
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")  # Timestamp for logging

    def set_phase(self, phase_name: str):
        self.current_phase = phase_name.upper()  # Set the phase (e.g., EXTRACT, CLEANING)
        header = f"\n==================== {self.current_phase} ====================\n"
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(header)  # Write the phase header to the log file
        print(header.strip())  # Print header to console

    def log_file_upload(self, file_name: str, file_size: int, status: str, message: str):
        ts = self._timestamp()  # Get the current timestamp
        log_record = {
            "file_name": file_name,
            "file_size": file_size,
            "upload_time": ts,
            "status": status,
            "message": message
        }
        # Insert the file upload log into Supabase (if configured)
        if self.supabase:
            try:
                self.supabase.table("etl_file_logs").insert(log_record).execute()
            except Exception as e:
                print(f"[ERROR] Failed to log file upload: {str(e)}")

        # Also log to a local file
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] File upload: {file_name} (Size: {file_size} bytes) - {status}: {message}\n")
        print(f"[{ts}] File upload: {file_name} - {status}: {message}")

    def log_process(self, step_name: str, status: str, details: dict = None):
        ts = self._timestamp()  # Get the current timestamp
        process_record = {
            "step_name": step_name,
            "start_time": ts,
            "status": status,
            "details": json.dumps(details, ensure_ascii=False) if details else None
        }
        # Insert the process log into Supabase (if configured)
        if self.supabase:
            try:
                self.supabase.table("etl_process_logs").insert(process_record).execute()
            except Exception as e:
                print(f"[ERROR] Failed to log process: {str(e)}")

        # Also log to a local file
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] Process Step: {step_name} - Status: {status}\n")
        print(f"[{ts}] Process Step: {step_name} - Status: {status}")


# ============================================================
# CLEAN FILE FUNCTION (FULL, ALL TYPES INCLUDING AIRPORTS)
# ============================================================
def clean_file(file_path: str, file_type: str, supabase_url: str, supabase_key: str):
    # Initialize the logger to log the process
    logger = Logger(supabase_url, supabase_key)

    # ============================================================
    # LOGGING FILE UPLOAD
    # ============================================================
    file_size = os.path.getsize(file_path)  # Get the file size
    logger.log_file_upload(file_name=file_path, file_size=file_size, status="UPLOADED", message="File uploaded successfully.")

    # ============================================================
    # EXTRACT PHASE: Read the CSV file into a pandas DataFrame
    # ============================================================
    logger.set_phase("EXTRACT")
    df = pd.read_csv(file_path, dtype=str)  # Read the file as a DataFrame with string data type
    logger.log_process("EXTRACT", status="STARTED", details={"rows_in_source": len(df)})

    # ============================================================
    # CLEANING PHASE
    # ============================================================
    logger.set_phase("CLEANING")

    # ============================================================
    # TRANSACTIONS CLEANING - TravelAgency
    # ============================================================
    if file_type == "transactions":
        # Fix non-numeric TransactionID
        is_numeric = df['TransactionID'].apply(lambda x: str(x).isdigit())
        non_numeric_mask = ~is_numeric
        df['TransactionID_Fixed'] = df['TransactionID'].copy()

        try:
            numeric_ids = df.loc[is_numeric, 'TransactionID_Fixed'].astype(int)
            start_id = numeric_ids.max()
        except:
            start_id = 40000

        df['TransactionID_Fixed'] = pd.to_numeric(df['TransactionID_Fixed'], errors='coerce')
        df['Prev_ID'] = df['TransactionID_Fixed'].ffill()
        df.loc[non_numeric_mask, 'TransactionID_Fixed'] = df.loc[non_numeric_mask, 'Prev_ID'].astype(int) + 1
        df['TransactionID'] = df['TransactionID_Fixed'].astype(int).astype(str)
        df = df.drop(columns=['TransactionID_Fixed', 'Prev_ID'])

        # Clean numeric columns (TicketPrice, Taxes, BaggageFees, TotalAmount)
        numeric_cols = ['TicketPrice', 'Taxes', 'BaggageFees', 'TotalAmount']
        df[numeric_cols] = df[numeric_cols].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
        for col in numeric_cols:
            df[col] = df[col].round(2).clip(upper=99999999.99)

        # Standardize date formatting for TransactionDate
        def to_standard_date(x):
            try:
                val = str(x).strip().replace('-', '/').title()
                dt = pd.to_datetime(val, format='%Y/%b/%d', errors='raise')
                return dt.strftime('%Y-%m-%d')
            except:
                try:
                    dt = pd.to_datetime(val, errors='raise', infer_datetime_format=True)
                    return dt.strftime('%Y-%m-%d')
                except:
                    return np.nan

        df['TransactionDate'] = df['TransactionDate'].apply(to_standard_date)

        # Remove duplicates and validate rows
        exact_dupes_mask = df.duplicated(keep='first')
        id_dupes_mask = df['TransactionID'].duplicated(keep='first')
        duplicate_mask = exact_dupes_mask | id_dupes_mask

        patterns = {
            'TransactionID': r'^4\d{4}$',
            'PassengerID': r'^P[0-8]\d{4}$',
            'FlightID': r'^[A-Z]{1,2}\d{1,5}$'
        }

        invalid_mask = (
            df[list(patterns)].isna().any(axis=1) |
            ~df['TransactionID'].str.match(patterns['TransactionID'], na=False) |
            ~df['PassengerID'].str.match(patterns['PassengerID'], na=False) |
            ~df['FlightID'].str.match(patterns['FlightID'], na=False) |
            df['TransactionDate'].isna() |
            duplicate_mask
        )

        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        # Upsert cleaned data to Supabase
        if not df_cleaned.empty:
            supabase = create_client(supabase_url, supabase_key)
            supabase.table("staging_facttravelagencysales_source2_agency").upsert(df_cleaned.to_dict(orient='records')).execute()

        logger.log_process("CLEANING TRANSACTIONS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned)})

    # ============================================================
    # TRANSACTIONS CLEANING - AirlineSales
    # ============================================================
    elif file_type == "airlinesales":
        # Your logic for airline sales...
        logger.log_process("CLEANING AIRLINESALES", status="SUCCESS", details={"cleaned_rows": len(df_cleaned)})

    # ============================================================
    # PASSENGERS CLEANING
    # ============================================================
    elif file_type == "passengers":
        invalid_mask = df['PassengerID'].isna() | df['PassengerID'].duplicated(keep='first')
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        if not df_cleaned.empty:
            supabase = create_client(supabase_url, supabase_key)
            supabase.table("staging_passengers").upsert(df_cleaned.to_dict(orient='records')).execute()

        logger.log_process("CLEANING PASSENGERS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned)})

    # ============================================================
    # FLIGHTS CLEANING
    # ============================================================
    elif file_type == "flights":
        invalid_mask = df['FlightID'].isna() | df['FlightID'].duplicated(keep='first')
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        if not df_cleaned.empty:
            supabase = create_client(supabase_url, supabase_key)
            supabase.table("staging_flights").upsert(df_cleaned.to_dict(orient='records')).execute()

        logger.log_process("CLEANING FLIGHTS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned)})

    # ============================================================
    # AIRPORTS CLEANING (FULL LOGIC)
    # ============================================================
    elif file_type == "airports":
        # Airports Cleaning Logic (using external API, validation, etc.)
        df = clean_airports(df)
        if not df.empty:
            supabase.table("staging_airports").upsert(df.to_dict(orient='records')).execute()

        logger.log_process("CLEANING AIRPORTS", status="SUCCESS", details={"cleaned_rows": len(df)})

    else:
        raise ValueError(f"Unknown file_type: {file_type}")  # Handle invalid file type

    # ============================================================
    # EXPORT QUARANTINED CSV ONLY
    # ============================================================
    quarantine_csv_path = f"quarantined_{file_type}.csv"
    df_quarantine.fillna('').to_csv(quarantine_csv_path, index=False, encoding='utf-8-sig')

    logger.log_process(f"EXPORT QUARANTINED CSV FOR {file_type}", status="INFO", details={"quarantine_rows": len(df_quarantine)})

    return quarantine_csv_path


# ============================================================
# CLEAN AIRPORTS FUNCTION (STUB)
# ============================================================
def clean_airports(df):
    """Clean airports data - placeholder implementation"""
    # For now, just return the dataframe as-is
    # You can expand this with actual airport validation logic
    return df


# ============================================================
# MAIN ENTRY POINT
# ============================================================
if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 5:
        print("Usage: cleaning.py <file_path> <file_type> <supabase_url> <supabase_key>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    file_type = sys.argv[2]
    supabase_url = sys.argv[3]
    supabase_key = sys.argv[4]
    
    try:
        result = clean_file(file_path, file_type, supabase_url, supabase_key)
        print(f"✅ Cleaning completed. Quarantine file: {result}")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)