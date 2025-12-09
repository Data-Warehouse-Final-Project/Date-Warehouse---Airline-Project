from datetime import datetime, timedelta
import re
import hashlib
from typing import Dict, List, Tuple, Optional, Any
import json

# ==================== DATE FUNCTIONS ====================

def validate_date(date_str: str, formats: List[str] = None) -> Tuple[bool, Optional[datetime]]:
    """
    Validate a date string against multiple formats.
    
    Args:
        date_str: Date string to validate
        formats: List of datetime formats to try. Defaults to common formats.
    
    Returns:
        Tuple of (is_valid, datetime_object)
    """
    if not formats:
        formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return True, dt
        except ValueError:
            continue
    return False, None


def calculate_age(dob_str: str) -> Optional[int]:
    """Calculate age from date of birth string."""
    is_valid, dob = validate_date(dob_str)
    if not is_valid:
        return None
    today = datetime.now()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def add_business_days(start_date: datetime, days: int) -> datetime:
    """Add business days to a date (excluding weekends)."""
    current = start_date
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Monday=0, Friday=4
            added += 1
    return current


def date_range_overlap(start1, end1, start2, end2) -> bool:
    """Check if two date ranges overlap."""
    return start1 <= end2 and start2 <= end1


# ==================== AIRLINES FUNCTIONS ====================

def validate_airline_code(code: str) -> bool:
    """Validate IATA airline code (2 letters) or ICAO code (3 letters)."""
    code = code.upper().strip()
    return bool(re.match(r'^[A-Z]{2}$|^[A-Z]{3}$', code))


def standardize_airline_name(name: str) -> str:
    """Standardize airline names by removing extra spaces and proper casing."""
    return ' '.join(name.strip().title().split())


def hash_airline(airline_code: str, airline_name: str) -> str:
    """Generate a unique hash for an airline for duplicate detection."""
    combined = f"{airline_code.upper().strip()}{standardize_airline_name(airline_name)}"
    return hashlib.md5(combined.encode()).hexdigest()


# ==================== AIRPORTS FUNCTIONS ====================

def validate_airport_code(code: str) -> Tuple[bool, str]:
    """
    Validate airport codes.
    
    Returns:
        Tuple of (is_valid, code_type) where code_type is 'IATA', 'ICAO', or 'INVALID'
    """
    code = code.upper().strip()
    if re.match(r'^[A-Z]{3}$', code):
        return True, 'IATA'
    elif re.match(r'^[A-Z]{4}$', code):
        return True, 'ICAO'
    return False, 'INVALID'


def calculate_distance_coordinates(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance between two coordinates in kilometers.
    Uses Haversine formula.
    """
    from math import radians, sin, cos, sqrt, atan2
    
    R = 6371  # Earth's radius in km
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c


AIRPORT_TIMEZONES = {
    'JFK': 'America/New_York',
    'LAX': 'America/Los_Angeles',
    'LHR': 'Europe/London',
    'CDG': 'Europe/Paris',
    'NRT': 'Asia/Tokyo',
    'SIN': 'Asia/Singapore',
    'AUS': 'America/Chicago',
    'ORD': 'America/Chicago',
}


def get_airport_timezone(airport_code: str) -> Optional[str]:
    """Get timezone for an airport code."""
    return AIRPORT_TIMEZONES.get(airport_code.upper())


def standardize_airport_code(code: str) -> Optional[str]:
    """Standardize airport code, preferring IATA (3-letter) format."""
    is_valid, code_type = validate_airport_code(code)
    return code.upper() if is_valid else None


# ==================== FLIGHTS FUNCTIONS ====================

def validate_flight_number(flight_number: str) -> bool:
    """
    Validate flight number format (e.g., AA123, BA1234, UA001).
    Format: 2-3 letter airline code + 1-4 digits.
    """
    flight_number = flight_number.upper().strip()
    return bool(re.match(r'^[A-Z]{2,3}\d{1,4}[A-Z]?$', flight_number))


def validate_flight_times(departure_time: str, arrival_time: str) -> Tuple[bool, str]:
    """
    Validate that arrival is after departure.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    is_valid_dep, dep_dt = validate_date(departure_time)
    is_valid_arr, arr_dt = validate_date(arrival_time)
    
    if not is_valid_dep or not is_valid_arr:
        return False, "Invalid date format"
    
    if arr_dt <= dep_dt:
        return False, "Arrival time must be after departure time"
    
    return True, ""


def calculate_flight_duration(departure_time: str, arrival_time: str) -> Optional[float]:
    """Calculate flight duration in hours."""
    is_valid, _ = validate_flight_times(departure_time, arrival_time)
    if not is_valid:
        return None
    
    is_valid_dep, dep_dt = validate_date(departure_time)
    is_valid_arr, arr_dt = validate_date(arrival_time)
    
    duration = arr_dt - dep_dt
    return duration.total_seconds() / 3600


FLIGHT_STATUS_MAPPING = {
    'ON TIME': 'on_time',
    'ONTIME': 'on_time',
    'DELAYED': 'delayed',
    'DELAY': 'delayed',
    'CANCELLED': 'cancelled',
    'CANCELED': 'cancelled',
    'BOARDING': 'boarding',
    'DEPARTED': 'departed',
    'ARRIVED': 'arrived',
}


def standardize_flight_status(status: str) -> Optional[str]:
    """Standardize flight status to canonical form."""
    return FLIGHT_STATUS_MAPPING.get(status.upper().strip())


def generate_flight_key(airline_code: str, flight_number: str, date: str) -> str:
    """Generate unique key for a flight occurrence."""
    key = f"{airline_code.upper()}{flight_number.upper()}{date}"
    return hashlib.md5(key.encode()).hexdigest()


# ==================== PASSENGERS FUNCTIONS ====================

def validate_passenger_id(passenger_id: str) -> bool:
    """Validate passenger ID format (typically alphanumeric, 6-15 chars)."""
    passenger_id = str(passenger_id).strip()
    return bool(re.match(r'^[A-Z0-9]{6,15}$', passenger_id.upper()))


def standardize_name(name: str) -> str:
    """Standardize passenger name: trim, proper case."""
    return ' '.join(name.strip().title().split())


def detect_name_format(full_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Attempt to split name into first and last.
    
    Returns:
        Tuple of (first_name, last_name)
    """
    parts = standardize_name(full_name).split()
    if len(parts) == 0:
        return None, None
    elif len(parts) == 1:
        return parts[0], None
    else:
        return parts[0], ' '.join(parts[1:])


def mask_passenger_pii(passenger_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mask sensitive PII fields in passenger data.
    Keeps: ID, first 2 chars of name
    Masks: email, phone, address
    """
    masked = passenger_data.copy()
    
    if 'email' in masked:
        email_parts = masked['email'].split('@')
        if len(email_parts) == 2:
            masked['email'] = f"{email_parts[0][:2]}***@{email_parts[1]}"
    
    if 'phone' in masked:
        phone = str(masked['phone'])
        masked['phone'] = f"***{phone[-4:]}"
    
    if 'address' in masked:
        masked['address'] = masked['address'][:10] + '***'
    
    return masked


def hash_passenger_identity(first_name: str, last_name: str, dob: str) -> str:
    """Generate unique hash for passenger identity (for duplicate detection)."""
    identity = f"{standardize_name(first_name)}{standardize_name(last_name)}{dob}"
    return hashlib.md5(identity.encode()).hexdigest()


# ==================== BOOKING SALES FUNCTIONS ====================

CURRENCY_CONVERSION = {
    'USD': 1.0,
    'EUR': 1.08,
    'GBP': 1.27,
    'JPY': 0.0068,
    'AUD': 0.66,
    'CAD': 0.73,
}


def convert_currency(amount: float, from_currency: str, to_currency: str = 'USD') -> Optional[float]:
    """Convert amount between currencies."""
    from_curr = from_currency.upper()
    to_curr = to_currency.upper()
    
    if from_curr not in CURRENCY_CONVERSION or to_curr not in CURRENCY_CONVERSION:
        return None
    
    in_usd = amount / CURRENCY_CONVERSION[from_curr]
    return in_usd * CURRENCY_CONVERSION[to_curr]


def validate_booking_amount(base_fare: float, taxes: float, fees: float, total: float, tolerance: float = 0.01) -> Tuple[bool, str]:
    """
    Validate that total booking amount equals base fare + taxes + fees.
    
    Args:
        tolerance: Allowed difference in currency units (default 0.01 for cents)
    
    Returns:
        Tuple of (is_valid, message)
    """
    calculated_total = base_fare + taxes + fees
    difference = abs(calculated_total - total)
    
    if difference <= tolerance:
        return True, ""
    else:
        return False, f"Total mismatch: calculated {calculated_total}, received {total}"


def standardize_booking_status(status: str) -> Optional[str]:
    """Standardize booking status."""
    status_map = {
        'CONFIRMED': 'confirmed',
        'PENDING': 'pending',
        'CANCELLED': 'cancelled',
        'CANCELED': 'cancelled',
        'REFUNDED': 'refunded',
        'COMPLETED': 'completed',
    }
    return status_map.get(status.upper().strip())


def calculate_refund_amount(total_paid: float, cancellation_fee_percent: float = 0.0) -> float:
    """Calculate refund amount after applying cancellation fees."""
    return max(0, total_paid * (1 - cancellation_fee_percent / 100))


def generate_booking_key(booking_ref: str, passenger_id: str) -> str:
    """Generate unique key for a booking record."""
    key = f"{booking_ref.upper()}{passenger_id.upper()}"
    return hashlib.md5(key.encode()).hexdigest()


# ==================== DATA QUALITY & VALIDATION ====================

def validate_row_completeness(row: Dict[str, Any], required_fields: List[str]) -> Tuple[bool, List[str]]:
    """
    Check if a row has all required fields with non-null values.
    
    Returns:
        Tuple of (is_complete, missing_fields)
    """
    missing = [field for field in required_fields if field not in row or row[field] is None]
    return len(missing) == 0, missing


def validate_numeric_range(value: Any, min_val: float = None, max_val: float = None) -> bool:
    """Validate a numeric value is within acceptable range."""
    try:
        num = float(value)
        if min_val is not None and num < min_val:
            return False
        if max_val is not None and num > max_val:
            return False
        return True
    except (ValueError, TypeError):
        return False


def clean_string_field(value: str) -> str:
    """Clean and normalize string fields."""
    if not isinstance(value, str):
        return ""
    return value.strip()


def detect_duplicate_records(records: List[Dict[str, Any]], key_fields: List[str]) -> List[Tuple[int, int]]:
    """
    Detect duplicate records based on key fields.
    
    Returns:
        List of tuples containing indices of duplicate record pairs
    """
    seen = {}
    duplicates = []
    
    for idx, record in enumerate(records):
        key = tuple(record.get(field) for field in key_fields)
        if key in seen:
            duplicates.append((seen[key], idx))
        else:
            seen[key] = idx
    
    return duplicates
