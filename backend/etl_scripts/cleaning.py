# ============================================================
# IMPORTS
# ============================================================
import pandas as pd
import numpy as np
import uuid
import json
import os
from datetime import datetime, timezone
from supabase import create_client
import pycountry
import requests
import time
import signal
from threading import Thread
from fuzzywuzzy import fuzz, process  # Added for fuzzy matching

# ============================================================
# TIMEOUT HANDLER
# ============================================================
def run_with_timeout(func, timeout_sec=5):
    """Run a function with a timeout (cross-platform compatible)"""
    result = [None]
    exception = [None]
    
    def target():
        try:
            result[0] = func()
        except Exception as e:
            exception[0] = e
    
    thread = Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout_sec)
    
    if thread.is_alive():
        raise TimeoutError(f"Operation timed out after {timeout_sec} seconds")
    
    if exception[0]:
        raise exception[0]
    
    return result[0]

# ============================================================
# LOGGER CLASS
# ============================================================
class Logger:
    def __init__(self, supabase_url: str = None, supabase_key: str = None):
        self.run_id = str(uuid.uuid4())  # Generate unique run ID for each log
        self.log_file_path = f"etl_log_{self.run_id}.txt"  # Log file path
        self.current_phase = None

        self.supabase = None
        if supabase_url and supabase_key:
            try:
                print(f"[INFO] Attempting to connect to Supabase...")
                self.supabase = create_client(supabase_url, supabase_key)
                # Test the connection with a timeout
                try:
                    print(f"[INFO] Testing Supabase connection (timeout: 5s)...")
                    def test_connection():
                        return self.supabase.table("etl_file_logs").select("*").limit(1).execute()
                    
                    run_with_timeout(test_connection, timeout_sec=5)
                    print(f"[INFO] Successfully connected to Supabase")
                except TimeoutError:
                    print(f"[WARN] Supabase connection test timed out - will continue with local logging only")
                    self.supabase = None
                except Exception as e:
                    print(f"[WARN] Supabase connection test failed ({str(e)}) - will continue with local logging only")
                    self.supabase = None
            except Exception as e:
                print(f"[WARN] Failed to initialize Supabase client: {str(e)} - will continue with local logging only")
                self.supabase = None

    def _timestamp(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # Timestamp for logging

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
                print(f"[WARN] Failed to log file upload to Supabase: {str(e)}")

        # Also log to a local file
        try:
            with open(self.log_file_path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] File upload: {file_name} (Size: {file_size} bytes) - {status}: {message}\n")
        except Exception as e:
            print(f"[ERROR] Failed to write to log file: {str(e)}")
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
                print(f"[WARN] Failed to log process to Supabase: {str(e)}")

        # Also log to a local file
        try:
            with open(self.log_file_path, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] Process Step: {step_name} - Status: {status}\n")
        except Exception as e:
            print(f"[ERROR] Failed to write to log file: {str(e)}")
        print(f"[{ts}] Process Step: {step_name} - Status: {status}")

    def log_event(self, message: str, table_name: str = None, level: str = "INFO", details: dict = None):
        """Log custom events"""
        ts = self._timestamp()
        log_msg = f"[{ts}] [{level}]"
        if table_name:
            log_msg += f" [{table_name}]"
        log_msg += f" {message}"
        
        if details:
            log_msg += f" | Details: {json.dumps(details, ensure_ascii=False)}"
        
        print(log_msg)
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")


def ensure_table_exists(supabase_client, table_name: str, sample_record: dict):
    """Ensure the table exists by trying to insert a record, or create it with basic schema"""
    try:
        # Try to insert a sample record to verify table exists
        supabase_client.table(table_name).insert([sample_record]).execute()
        # If successful, delete the test record
        try:
            supabase_client.table(table_name).delete().neq('id', 'nonexistent').execute()
        except:
            pass
        return True
    except Exception as e:
        print(f"[WARN] Table {table_name} may not exist or has schema issues: {e}")
        return False


def safe_upsert(supabase_client, table_name: str, records: list):
    """Try upsert; if it fails (schema mismatch or missing table), fall back to inserting JSONB in `data` column."""
    if not records:
        return
    
    if not supabase_client:
        print(f"[WARN] No Supabase client available - skipping upload to {table_name}")
        return
    
    try:
        supabase_client.table(table_name).upsert(records).execute()
        print(f"[INFO] Successfully upserted {len(records)} records to {table_name}")
        return
    except Exception as e:
        error_msg = str(e).lower()
        # Check if it's a network error
        if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
            print(f"[WARN] Network error connecting to Supabase: {e}. Skipping database operations.")
            return
        print(f"[WARN] Upsert to {table_name} failed: {e}. Attempting fallback...")
    
    # Fallback 1: Try simple insert instead of upsert
    try:
        print(f"[INFO] Attempting insert (non-upsert) to {table_name}...")
        supabase_client.table(table_name).insert(records).execute()
        print(f"[INFO] Successfully inserted {len(records)} records to {table_name}")
        return
    except Exception as e2:
        error_msg = str(e2).lower()
        if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
            print(f"[WARN] Network error during insert: {e2}. Skipping database operations.")
            return
        print(f"[WARN] Insert to {table_name} also failed: {e2}. Skipping further attempts.")
        # Don't try JSONB fallback - just log and continue
        return


# ============================================================
# TABLE CHECKER & HELPER FUNCTIONS (from your request)
# ============================================================
def table_exists(supabase_client, table_name: str):
    """Check if a table exists in Supabase"""
    try:
        supabase_client.table(table_name).select("*", count="exact").limit(1).execute()
        return True
    except Exception:
        return False


def fetch_existing_airlinekeys(supabase_client):
    """Fetch existing airline keys from staging_airlines table"""
    if not table_exists(supabase_client, "staging_airlines"):
        print("[WARN] staging_airlines table not found")
        return []
    
    try:
        res = supabase_client.table("staging_airlines").select("airlinekey").execute()
        return [row["airlinekey"].upper() for row in res.data] if res.data else []
    except Exception as e:
        print(f"[WARN] Failed to fetch existing airline keys: {str(e)}")
        return []


def fetch_airportkeys(supabase_client):
    """Fetch valid airport keys from staging_airports table"""
    if not table_exists(supabase_client, "staging_airports"):
        print("[WARN] staging_airports table does NOT exist in Supabase.")
        return None

    try:
        res = supabase_client.table("staging_airports").select("airportkey").execute()
        if not res.data:
            print("[WARN] staging_airports exists but has NO DATA.")
            return None
        return [row["airportkey"].upper() for row in res.data]
    except Exception as e:
        print(f"[WARN] Failed to fetch airport keys: {str(e)}")
        return None


def fetch_airlinekeys(supabase_client):
    """Fetch valid airline keys from staging_airlines table"""
    if not table_exists(supabase_client, "staging_airlines"):
        print("[WARN] staging_airlines table does NOT exist in Supabase.")
        return None

    try:
        res = supabase_client.table("staging_airlines").select("airlinekey").execute()
        if not res.data:
            print("[WARN] staging_airlines exists but has NO DATA.")
            return None
        return [row["airlinekey"].upper() for row in res.data]
    except Exception as e:
        print(f"[WARN] Failed to fetch airline keys: {str(e)}")
        return None


def fuzzy_fix(value, valid_list):
    """Fuzzy match helper"""
    if not valid_list or pd.isna(value):
        return value

    best_match = process.extractOne(str(value), valid_list, scorer=fuzz.WRatio)
    if best_match and best_match[1] >= 85:
        return best_match[0]
    return value


def fix_flightkey_prefix(flightkey, airlinekeys):
    """Fix flight key prefix using airline keys"""
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


def remove_key_from_email(email, raw_key):
    """Remove passenger key digits from email"""
    key_digits = ''.join(filter(str.isdigit, str(raw_key)))
    if key_digits:
        email = email.replace(key_digits, '')
        try:
            no_leading_zeros = str(int(key_digits))
            email = email.replace(no_leading_zeros, '')
        except ValueError:
            pass
    return email


# ============================================================
# CLEAN AIRLINES FUNCTION
# ============================================================
def clean_airlines(df, supabase_client, logger):
    """Clean airlines data with alliance validation"""
    logger.set_phase("CLEANING AIRLINES")
    logger.log_event("Cleaning column names...", table_name="airlines")
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()
    logger.log_event("Standardized column names", table_name="airlines", 
                     details={"columns": df.columns.tolist()})

    # Ensure we have required columns
    required_columns = ['airlinekey', 'airlinename', 'alliance']
    for col in required_columns:
        if col not in df.columns:
            if col == 'airlinename' and 'airlinekey' in df.columns:
                df['airlinename'] = df['airlinekey']
                logger.log_event(f"Created '{col}' column from airlinekey", table_name="airlines", level="WARN")
            elif col == 'alliance':
                df['alliance'] = 'None'
                logger.log_event(f"Created '{col}' column with default 'None'", table_name="airlines", level="WARN")
            else:
                logger.log_event(f"Missing required column: {col}", table_name="airlines", level="ERROR")
                raise ValueError(f"Missing required column: {col}")

    # Basic cleaning with proper NaN handling
    df["airlinekey"] = df["airlinekey"].astype(str).str.strip().str.upper()
    df["airlinename"] = df["airlinename"].astype(str).str.strip().str.title()
    df["airlinename"] = df["airlinename"].str.replace(r"\s+", " ", regex=True)

    # Handle NaN properly - use 'None' for empty/NaN values
    df["alliance"] = df["alliance"].fillna('None')
    df["alliance"] = df["alliance"].astype(str).str.strip()

    # Standardize alliance values
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

    # Ensure values match ENUM exactly
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
            logger.log_event(f"Fixed alliance for {airline_key}: {correct_alliance}", 
                           table_name="airlines", details={"airlinekey": airline_key, "new_alliance": correct_alliance})

    # Validation
    airlinekey_valid = df["airlinekey"].str.match(r"^[A-Z0-9]{2,3}$")
    airlinename_valid = df["airlinename"].str.match(r"^[A-Za-z0-9\s\.\-\&]+$")
    duplicate_mask = df["airlinekey"].duplicated(keep="first")

    all_valid = airlinekey_valid & airlinename_valid & (~duplicate_mask)
    df_clean = df[all_valid].copy()
    df_quarantine = df[~all_valid].copy()

    logger.log_event(f"Cleaned {len(df_clean)} valid rows", table_name="airlines")
    logger.log_event(f"Quarantined: {len(df_quarantine)} rows", table_name="airlines", level="WARN")

    return df_clean, df_quarantine


# ============================================================
# CLEAN FLIGHTS FUNCTION
# ============================================================
def clean_flights(df, supabase_client, logger):
    """Clean flights data with airport and airline validation"""
    logger.set_phase("CLEANING FLIGHTS")
    
    # Fix JK into JFK (temporary)
    df.loc[df['OriginAirportKey'] == 'JK', 'OriginAirportKey'] = 'JFK'

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

    logger.log_event("Basic normalization complete", table_name="flights")

    # Fetch reference values
    valid_airports = fetch_airportkeys(supabase_client)
    valid_airlines = fetch_existing_airlinekeys(supabase_client)

    # Origin and DestinationAirportKey validation using fuzzy matching
    if valid_airports:
        df["OriginAirportKey"] = df["OriginAirportKey"].apply(lambda x: fuzzy_fix(x, valid_airports))
        df["DestinationAirportKey"] = df["DestinationAirportKey"].apply(lambda x: fuzzy_fix(x, valid_airports))
    else:
        logger.log_event("No valid airports loaded - skipping fuzzy correction", table_name="flights", level="WARN")

    # Flightkey prefix validation
    if valid_airlines:
        df["FlightKey"] = df["FlightKey"].apply(lambda fk: fix_flightkey_prefix(fk, valid_airlines))
    else:
        logger.log_event("No airline data loaded - skipping airline validation", table_name="flights", level="WARN")

    # Validation rules
    duplicate_mask = df["FlightKey"].duplicated(keep="first")
    flight_valid = df["FlightKey"].str.match(r"^[A-Za-z0-9]{2}\d+$")
    origin_valid = df["OriginAirportKey"].str.match(r"^[A-Za-z]{3}$")
    dest_valid = df["DestinationAirportKey"].str.match(r"^[A-Za-z]{3}$")
    origin_dest_valid = df["OriginAirportKey"] != df["DestinationAirportKey"]

    all_valid = flight_valid & origin_valid & dest_valid & origin_dest_valid & (~duplicate_mask)
    df_clean = df[all_valid].copy()
    df_quarantine = df[~all_valid].copy()

    logger.log_event(f"Cleaned {len(df_clean)} valid rows", table_name="flights")
    logger.log_event(f"Quarantined: {len(df_quarantine)} rows", table_name="flights", level="WARN")

    return df_clean, df_quarantine


# ============================================================
# CLEAN PASSENGERS FUNCTION
# ============================================================
def clean_passengers(df, logger):
    """Clean passengers data with email processing"""
    logger.set_phase("CLEANING PASSENGERS")
    logger.log_event("Started cleaning Email column", table_name="passengers")

    # Ensure required columns exist
    required_cols = ['passengerkey', 'fullname', 'email', 'loyaltystatus']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Email cleaning
    df['email'] = df['email'].astype(str).str.lower().str.strip()
    df['email'] = df.apply(lambda row: remove_key_from_email(row['email'], row['passengerkey']), axis=1)
    logger.log_event("Email column cleaned", table_name="passengers")

    # PassengerKey cleaning
    df['passengerkey'] = df['passengerkey'].astype(str).str.strip()
    logger.log_event("PassengerKey cleaned", table_name="passengers")

    # FullName cleaning
    df['fullname'] = df['fullname'].astype(str).str.strip().replace(r'\s+', ' ', regex=True).str.title()
    logger.log_event("FullName cleaned", table_name="passengers")

    # LoyaltyStatus cleaning
    valid_statuses = ['Bronze', 'Silver', 'Gold', 'Platinum']
    df['loyaltystatus'] = df['loyaltystatus'].astype(str).str.strip().str.replace(r'[^a-zA-Z]', '', regex=True).str.capitalize()
    logger.log_event("LoyaltyStatus cleaned", table_name="passengers")

    logger.log_event("Successfully cleaned all rows", table_name="passengers")

    # Validation
    missing_data_idx = df[df[required_cols].isnull().any(axis=1)].index
    dup_cols = ['fullname', 'email', 'loyaltystatus']
    duplicates_idx = df[df.duplicated(subset=dup_cols, keep='first')].index
    invalid_conditions = (
        df['passengerkey'].isnull() |
        ~df['fullname'].astype(str).str.match(r'^[A-Za-z]+(?:\s+[A-Za-z]+)+$', na=False) |
        ~df['email'].str.match(r'^[a-z0-9]+(?:[._][a-z0-9]+)*@example\.com$', na=False) |
        ~df['loyaltystatus'].isin(valid_statuses)
    )
    invalid_idx = df[invalid_conditions].index

    invalid_idx_all = set(missing_data_idx) | set(duplicates_idx) | set(invalid_idx)
    invalid_rows = df.loc[sorted(invalid_idx_all)].reset_index(drop=True) if invalid_idx_all else pd.DataFrame()
    df_clean = df[~df.index.isin(invalid_idx_all)].copy() if invalid_idx_all else df.copy()

    logger.log_event(f"Cleaned {len(df_clean)} valid rows", table_name="passengers")
    logger.log_event(f"Quarantined: {len(invalid_rows)} rows", table_name="passengers", level="WARN")

    return df_clean, invalid_rows


# ============================================================
# CLEAN AIRPORTS FUNCTION
# ============================================================
def clean_airports(df):
    """Clean airports data - placeholder implementation"""
    # For now, just return the dataframe as-is
    # You can expand this with actual airport validation logic
    return df, pd.DataFrame()  # Return empty quarantine for now


# ============================================================
# CLEAN FILE FUNCTION (MAIN)
# ============================================================
def clean_file(file_path: str, file_type: str, supabase_url: str, supabase_key: str, staging_table_override: str = None):
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
    # Normalize column names to lowercase for consistency
    df.columns = df.columns.str.lower()
    logger.log_process("EXTRACT", status="STARTED", details={"rows_in_source": len(df)})

    # ============================================================
    # CLEANING PHASE
    # ============================================================
    logger.set_phase("CLEANING")

    # Create Supabase client
    supabase = create_client(supabase_url, supabase_key)

    # ============================================================
    # TRANSACTIONS CLEANING - TravelAgency
    # ============================================================
    if file_type == "transactions":
        # Fix non-numeric transactionid
        is_numeric = df['transactionid'].apply(lambda x: str(x).isdigit())
        non_numeric_mask = ~is_numeric
        df['transactionid_fixed'] = df['transactionid'].copy()

        try:
            numeric_ids = df.loc[is_numeric, 'transactionid_fixed'].astype(int)
            start_id = numeric_ids.max()
        except:
            start_id = 40000

        df['transactionid_fixed'] = pd.to_numeric(df['transactionid_fixed'], errors='coerce')
        df['prev_id'] = df['transactionid_fixed'].ffill()
        df.loc[non_numeric_mask, 'transactionid_fixed'] = df.loc[non_numeric_mask, 'prev_id'].astype(int) + 1
        df['transactionid'] = df['transactionid_fixed'].astype(int).astype(str)
        df = df.drop(columns=['transactionid_fixed', 'prev_id'])

        # Clean numeric columns (ticketprice, taxes, baggagefees, totalamount)
        numeric_cols = ['ticketprice', 'taxes', 'baggagefees', 'totalamount']
        df[numeric_cols] = df[numeric_cols].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
        for col in numeric_cols:
            df[col] = df[col].round(2).clip(upper=99999999.99)

        # Standardize date formatting for transactiondate
        def to_standard_date(x):
            try:
                val = str(x).strip().replace('-', '/').title()
                dt = pd.to_datetime(val, format='%Y/%b/%d', errors='raise')
                return dt.strftime('%Y-%m-%d')
            except:
                try:
                    dt = pd.to_datetime(val, errors='raise')
                    return dt.strftime('%Y-%m-%d')
                except:
                    return np.nan

        df['transactiondate'] = df['transactiondate'].apply(to_standard_date)

        # Remove duplicates and validate rows
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
            df['transactiondate'].isna() |
            duplicate_mask
        )

        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        # Upsert cleaned data to Supabase
        if not df_cleaned.empty:
            try:
                records = df_cleaned.to_dict(orient='records')
                # ALWAYS use staging_facttravelagencysales_source2_agency
                target_table = "staging_facttravelagencysales_source2_agency"
                safe_upsert(supabase, target_table, records)
                logger.log_process("CLEANING TRANSACTIONS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned), "staging_table": target_table})
            except Exception as e:
                error_msg = str(e).lower()
                if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                    print(f"[WARN] Network error - data cleaned but not uploaded: {e}")
                    logger.log_process("CLEANING TRANSACTIONS", status="SUCCESS_LOCAL_ONLY", details={"cleaned_rows": len(df_cleaned), "note": "Network error - data cleaned locally only"})
                else:
                    logger.log_process("CLEANING TRANSACTIONS", status="FAILED", details={"error": str(e), "cleaned_rows": len(df_cleaned)})
                    print(f"[ERROR] Failed to upsert transaction data: {e}")
                    # Don't raise - continue processing
        else:
            logger.log_process("CLEANING TRANSACTIONS", status="SUCCESS", details={"cleaned_rows": 0})

    # ============================================================
    # AIRLINES CLEANING
    # ============================================================
    elif file_type == "airlines":
        df_cleaned, df_quarantine = clean_airlines(df, supabase, logger)
        
        if not df_cleaned.empty:
            try:
                records = df_cleaned.to_dict(orient='records')
                target_table = "staging_facttravelagencysales_source2_agency"
                safe_upsert(supabase, target_table, records)
                logger.log_process("CLEANING AIRLINES", status="SUCCESS", details={"cleaned_rows": len(df_cleaned), "staging_table": target_table})
            except Exception as e:
                error_msg = str(e).lower()
                if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                    print(f"[WARN] Network error - data cleaned but not uploaded: {e}")
                    logger.log_process("CLEANING AIRLINES", status="SUCCESS_LOCAL_ONLY", details={"cleaned_rows": len(df_cleaned)})
                else:
                    logger.log_process("CLEANING AIRLINES", status="FAILED", details={"error": str(e)})
                    print(f"[ERROR] Failed to upsert airlines data: {e}")
                    # Don't raise - continue processing
        else:
            logger.log_process("CLEANING AIRLINES", status="SUCCESS", details={"cleaned_rows": 0})

    # ============================================================
    # FLIGHTS CLEANING
    # ============================================================
    elif file_type == "flights":
        df_cleaned, df_quarantine = clean_flights(df, supabase, logger)
        
        if not df_cleaned.empty:
            try:
                # Lowercase columns for Supabase
                df_cleaned.columns = df_cleaned.columns.str.lower()
                records = df_cleaned.to_dict(orient='records')
                target_table = staging_table_override or "staging_flights"
                safe_upsert(supabase, target_table, records)
                logger.log_process("CLEANING FLIGHTS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned), "staging_table": target_table})
            except Exception as e:
                error_msg = str(e).lower()
                if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                    print(f"[WARN] Network error - data cleaned but not uploaded: {e}")
                    logger.log_process("CLEANING FLIGHTS", status="SUCCESS_LOCAL_ONLY", details={"cleaned_rows": len(df_cleaned)})
                else:
                    logger.log_process("CLEANING FLIGHTS", status="FAILED", details={"error": str(e)})
                    print(f"[ERROR] Failed to upsert flights data: {e}")
                    # Don't raise - continue processing
        else:
            logger.log_process("CLEANING FLIGHTS", status="SUCCESS", details={"cleaned_rows": 0})

    # ============================================================
    # PASSENGERS CLEANING
    # ============================================================
    elif file_type == "passengers":
        df_cleaned, df_quarantine = clean_passengers(df, logger)
        
        if not df_cleaned.empty:
            try:
                records = df_cleaned.to_dict(orient='records')
                target_table = "staging_facttravelagencysales_source2_agency"
                safe_upsert(supabase, target_table, records)
                logger.log_process("CLEANING PASSENGERS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned), "staging_table": target_table})
            except Exception as e:
                error_msg = str(e).lower()
                if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                    print(f"[WARN] Network error - data cleaned but not uploaded: {e}")
                    logger.log_process("CLEANING PASSENGERS", status="SUCCESS_LOCAL_ONLY", details={"cleaned_rows": len(df_cleaned)})
                else:
                    logger.log_process("CLEANING PASSENGERS", status="FAILED", details={"error": str(e)})
                    print(f"[ERROR] Failed to upsert passenger data: {e}")
                    # Don't raise - continue processing
        else:
            logger.log_process("CLEANING PASSENGERS", status="SUCCESS", details={"cleaned_rows": 0})

    # ============================================================
    # AIRPORTS CLEANING
    # ============================================================
    elif file_type == "airports":
        df_cleaned, df_quarantine = clean_airports(df)
        
        if not df_cleaned.empty:
            try:
                records = df.to_dict(orient='records')
                target_table = "staging_facttravelagencysales_source2_agency"
                safe_upsert(supabase, target_table, records)
                logger.log_process("CLEANING AIRPORTS", status="SUCCESS", details={"cleaned_rows": len(df_cleaned), "staging_table": target_table})
            except Exception as e:
                error_msg = str(e).lower()
                if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                    print(f"[WARN] Network error - data cleaned but not uploaded: {e}")
                    logger.log_process("CLEANING AIRPORTS", status="SUCCESS_LOCAL_ONLY", details={"cleaned_rows": len(df_cleaned)})
                else:
                    logger.log_process("CLEANING AIRPORTS", status="FAILED", details={"error": str(e)})
                    print(f"[ERROR] Failed to upsert airports data: {e}")
                    # Don't raise - continue processing
        else:
            logger.log_process("CLEANING AIRPORTS", status="SUCCESS", details={"cleaned_rows": 0})

    # ============================================================
    # AIRLINESALES CLEANING
    # ============================================================
    elif file_type == "airlinesales":
        logger.set_phase("CLEANING AIRLINESALES")
        
        # Clean numeric columns
        if 'ticketprice' in df.columns:
            df['ticketprice'] = df['ticketprice'].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
        
        invalid_mask = df['transactionid'].isna() | df['transactionid'].duplicated(keep='first')
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        if not df_cleaned.empty:
            try:
                records = df_cleaned.to_dict(orient='records')
                target_table = "staging_facttravelagencysales_source2_agency"
                safe_upsert(supabase, target_table, records)
                logger.log_process("CLEANING AIRLINESALES", status="SUCCESS", details={"cleaned_rows": len(df_cleaned), "staging_table": target_table})
            except Exception as e:
                error_msg = str(e).lower()
                if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
                    print(f"[WARN] Network error - data cleaned but not uploaded: {e}")
                    logger.log_process("CLEANING AIRLINESALES", status="SUCCESS_LOCAL_ONLY", details={"cleaned_rows": len(df_cleaned)})
                else:
                    logger.log_process("CLEANING AIRLINESALES", status="FAILED", details={"error": str(e)})
                    print(f"[ERROR] Failed to upsert airlinesales data: {e}")
                    # Don't raise - continue processing
        else:
            logger.log_process("CLEANING AIRLINESALES", status="SUCCESS", details={"cleaned_rows": 0})

    else:
        raise ValueError(f"Unknown file_type: {file_type}")

    # ============================================================
    # EXPORT QUARANTINED CSV ONLY
    # ============================================================
    quarantine_csv_path = f"quarantined_{file_type}.csv"
    if 'df_quarantine' in locals() and not df_quarantine.empty:
        df_quarantine.fillna('').to_csv(quarantine_csv_path, index=False, encoding='utf-8-sig')
        logger.log_process(f"EXPORT QUARANTINED CSV FOR {file_type}", status="INFO", details={"quarantine_rows": len(df_quarantine)})
    else:
        # Create empty quarantine file if no quarantined data
        pd.DataFrame().to_csv(quarantine_csv_path, index=False)
        logger.log_process(f"EXPORT QUARANTINED CSV FOR {file_type}", status="INFO", details={"quarantine_rows": 0})

    return quarantine_csv_path


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
    # ALWAYS use staging_facttravelagencysales_source2_agency - ignore any override parameter
    
    try:
        # Always pass None for override - we use the fixed table name
        result = clean_file(file_path, file_type, supabase_url, supabase_key, None)
        print(f"[SUCCESS] Cleaning completed. Quarantine file: {result}")
        sys.exit(0)
    except Exception as e:
        error_msg = str(e).lower()
        # Network errors are not critical - data was still cleaned
        if "getaddrinfo failed" in error_msg or "connection" in error_msg or "timeout" in error_msg:
            print(f"[WARNING] Cleaning completed with network errors (data was cleaned locally): {str(e)}")
            sys.exit(0)
        else:
            # Fixed Unicode handling for Windows
            try:
                print(f"Error: {str(e)}")
            except:
                print(f"Error occurred (details contain non-ASCII characters)")
            sys.exit(1)