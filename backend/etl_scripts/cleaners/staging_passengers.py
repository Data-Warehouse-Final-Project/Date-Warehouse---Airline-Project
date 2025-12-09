import pandas as pd
import numpy as np

def clean_passengers_data(df: pd.DataFrame) -> dict:
    """
    Takes a raw DataFrame, cleans it, validates it, and separates invalid rows.
    Returns a dictionary: {'clean': pd.DataFrame, 'quarantined': pd.DataFrame}
    """
    
    # 1. Keep a copy for quarantine
    original_df = df.copy()

    # ---------------------------------------------------------
    # TRANSFORMATION LOGIC
    # ---------------------------------------------------------
    
    # --- Helper Function for Email ---
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

    # --- Apply Cleaning Rules ---
    df['Email'] = df['Email'].astype(str).str.lower().str.strip()
    df['Email'] = df.apply(lambda row: remove_key_from_email(row['Email'], row['PassengerKey']), axis=1)
    
    df['PassengerKey'] = df['PassengerKey'].astype(str).str.strip()
    
    df['FullName'] = df['FullName'].astype(str).str.strip().replace(r'\s+', ' ', regex=True).str.title()
    
    valid_statuses = ['Bronze', 'Silver', 'Gold', 'Platinum']
    df['LoyaltyStatus'] = df['LoyaltyStatus'].astype(str).str.strip().str.replace(r'[^a-zA-Z]', '', regex=True).str.capitalize()

    # ---------------------------------------------------------
    # VALIDATION & QUARANTINE LOGIC
    # ---------------------------------------------------------
    
    required = ['PassengerKey', 'FullName', 'Email', 'LoyaltyStatus']
    
    # 1. Check for Nulls
    missing_data_idx = df[df[required].isnull().any(axis=1)].index
    
    # 2. Check for Duplicates
    dup_cols = ['FullName', 'Email', 'LoyaltyStatus']
    duplicates_idx = df[df.duplicated(subset=dup_cols, keep='first')].index
    
    # 3. Check for Regex/Logic Violations
    invalid_conditions = (
        df['PassengerKey'].isnull() |
        ~df['FullName'].astype(str).str.match(r'^[A-Za-z]+(?:\s+[A-Za-z]+)+$', na=False) |
        ~df['Email'].str.match(r'^[a-z0-9]+(?:[._][a-z0-9]+)*@example\.com$', na=False) |
        ~df['LoyaltyStatus'].isin(valid_statuses)
    )
    invalid_logic_idx = df[invalid_conditions].index

    # Combine all invalid indices
    invalid_idx_all = set(missing_data_idx) | set(duplicates_idx) | set(invalid_logic_idx)

    # ---------------------------------------------------------
    # SPLIT DATA
    # ---------------------------------------------------------
    
    # Get invalid rows from the ORIGINAL dataframe (so we see the raw bad data)
    quarantined_df = original_df.loc[sorted(invalid_idx_all)].reset_index(drop=True)
    
    # Get clean rows from the CLEANED dataframe
    clean_df = df.drop(list(invalid_idx_all), errors='ignore').reset_index(drop=True)

    # Generate new IDs if required (Business Rule)
    if not clean_df.empty:
        clean_df['PassengerKey'] = [f"P{1000 + n}" for n in range(1, len(clean_df) + 1)]

    # Select final columns for Warehouse
    clean_df = clean_df[['PassengerKey', 'FullName', 'Email', 'LoyaltyStatus']]
    
    # Rename for Supabase (Postgres is case sensitive usually prefers lowercase)
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