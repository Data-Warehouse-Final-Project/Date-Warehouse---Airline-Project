# ============================================================
# IMPORTS
# ============================================================
import pandas as pd
import numpy as np
import uuid
import json
from datetime import datetime
from supabase import create_client

# ============================================================
# LOGGER CLASS
# ============================================================
class Logger:
    def __init__(self, supabase_url: str = None, supabase_key: str = None, log_prefix: str = "pipeline_run_log"):
        self.run_id = str(uuid.uuid4())
        self.log_file_path = f"{log_prefix}_{self.run_id}.txt"
        self.current_phase = None

        if supabase_url and supabase_key:
            self.supabase = create_client(supabase_url, supabase_key)
        else:
            self.supabase = None

    def _timestamp(self):
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    def set_phase(self, phase_name: str):
        self.current_phase = phase_name.upper()
        header = f"\n==================== {self.current_phase} ====================\n"
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(header)
        print(header.strip())

    def log(self, message: str, layer: str = None, component: str = None, table_name: str = None, level: str = "INFO", details: dict = None):
        ts = self._timestamp()
        line = f"[{ts}] {message}\n"
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(line)
        print(line.strip())

        if self.supabase:
            details_json = json.dumps(details, ensure_ascii=False) if details else None
            record = {
                "run_id": self.run_id,
                "event_time": ts,
                "layer": layer,
                "component": component,
                "table_name": table_name,
                "level": level,
                "message": message,
                "details_json": details_json,
            }
            try:
                self.supabase.table("pipeline_run_log").insert(record).execute()
            except Exception as e:
                fallback = f"Supabase insert failed: {str(e)}"
                print(f"[ERROR] {fallback}")
                with open(self.log_file_path, "a", encoding="utf-8") as f:
                    f.write(f"[ERROR] {fallback}\n")

    def get_log_file_path(self):
        return self.log_file_path


# ============================================================
# CLEAN FILE FUNCTION (UPDATED)
# ============================================================
def clean_file(file_path: str, file_type: str, supabase_url: str, supabase_key: str):
    logger = Logger(supabase_url, supabase_key)
    logger.set_phase("EXTRACT")

    # Read CSV
    df = pd.read_csv(file_path, dtype=str)
    logger.log(f"Extracted raw CSV file", layer="E", component="extract", table_name=file_path, level="INFO", details={"rows_in_source": len(df)})

    logger.set_phase("CLEANING")

    supabase = create_client(supabase_url, supabase_key)

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

        # Numeric columns
        numeric_cols = ['TicketPrice', 'Taxes', 'BaggageFees', 'TotalAmount']
        df[numeric_cols] = df[numeric_cols].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
        for col in numeric_cols:
            df[col] = df[col].round(2).clip(upper=99999999.99)

        # Date
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

        # Duplicates & validation
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

        # Batch upsert to Supabase
        if not df_cleaned.empty:
            supabase.table("staging_facttravelagencysales_source2_agency").upsert(df_cleaned.to_dict(orient='records')).execute()
        logger.log(f"Upserted {len(df_cleaned)} cleaned transactions to Supabase", layer="L", component="upsert", table_name="transactions")

    # ============================================================
    # TRANSACTIONS CLEANING - AirlineSales
    # ============================================================
    elif file_type == "airlinesales":
        # Airline transactions start from TransactionID >= 10000
        df['TransactionID'] = pd.to_numeric(df['TransactionID'], errors='coerce').fillna(10000).astype(int)
        df = df[df['TransactionID'] >= 10000]

        numeric_cols = ['TicketPrice', 'Taxes', 'BaggageFees', 'TotalAmount']
        df[numeric_cols] = df[numeric_cols].replace(r'[\$,]', '', regex=True).apply(pd.to_numeric, errors='coerce')
        for col in numeric_cols:
            df[col] = df[col].round(2).clip(upper=99999999.99)

        # Duplicates & validation
        duplicate_mask = df.duplicated(subset=['TransactionID'], keep='first')
        df_quarantine = df[duplicate_mask | df['PassengerKey'].isna() | df['FlightKey'].isna()].copy()
        df_cleaned = df[~duplicate_mask & df['PassengerKey'].notna() & df['FlightKey'].notna()].copy()

        # Upsert cleaned Airline sales to Supabase
        if not df_cleaned.empty:
            supabase.table("staging_factairlinesales_source1_corporate").upsert(df_cleaned.to_dict(orient='records')).execute()
        logger.log(f"Upserted {len(df_cleaned)} cleaned AirlineSales to Supabase", layer="L", component="upsert", table_name="airlinesales")

    # ============================================================
    # PASSENGERS CLEANING
    # ============================================================
    elif file_type == "passengers":
        invalid_mask = df['PassengerID'].isna() | df['PassengerID'].duplicated(keep='first')
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        if not df_cleaned.empty:
            supabase.table("staging_passengers").upsert(df_cleaned.to_dict(orient='records')).execute()
        logger.log(f"Upserted {len(df_cleaned)} cleaned passengers to Supabase", layer="L", component="upsert", table_name="passengers")

    # ============================================================
    # FLIGHTS CLEANING
    # ============================================================
    elif file_type == "flights":
        invalid_mask = df['FlightID'].isna() | df['FlightID'].duplicated(keep='first')
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()

        if not df_cleaned.empty:
            supabase.table("staging_flights").upsert(df_cleaned.to_dict(orient='records')).execute()
        logger.log(f"Upserted {len(df_cleaned)} cleaned flights to Supabase", layer="L", component="upsert", table_name="flights")

    # ============================================================
    # AIRLINES CLEANING
    # ============================================================
    elif file_type == "airlines":
        invalid_mask = df.isna().any(axis=1)
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()
        logger.log(f"No upsert for airlines, only quarantine CSV saved", layer="L", component="export", table_name="airlines")

    # ============================================================
    # AIRPORTS CLEANING
    # ============================================================
    elif file_type == "airports":
        invalid_mask = df.isna().any(axis=1)
        df_quarantine = df[invalid_mask].copy()
        df_cleaned = df[~invalid_mask].copy()
        logger.log(f"No upsert for airports, only quarantine CSV saved", layer="L", component="export", table_name="airports")

    else:
        raise ValueError(f"Unknown file_type: {file_type}")

    # ============================================================
    # EXPORT QUARANTINED CSV ONLY
    # ============================================================
    quarantine_csv_path = f"quarantined_{file_type}.csv"
    df_quarantine.fillna('').to_csv(quarantine_csv_path, index=False, encoding='utf-8-sig')
    logger.log(f"Exported quarantined CSV for {file_type}", layer="L", component="export", table_name=file_type, level="INFO", details={"quarantine_rows": len(df_quarantine)})

    return quarantine_csv_path