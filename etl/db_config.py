from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine


# --- CENTRALIZED DATABASE CONFIGURATION ---
# Replace these with your actual MySQL credentials and connection details
DB_USER = 'root'
DB_PASS = 'Silvers123@_@'  
DB_HOST = 'localhost'
DB_NAME = 'midterm_vn_firm_panel'

def get_connection() -> Optional[Engine]:
    """
    Provision a reusable SQLAlchemy engine for the ETL suite.

    Usage:
        engine = get_connection()

    Args:
        None

    Returns:
        Optional[Engine]: Live engine when the connection succeeds; None otherwise.
    """
    try:
        engine_url = URL.create(
            drivername="mysql+mysqlconnector",
            username=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            database=DB_NAME,
        )        
        engine = create_engine(engine_url)
        return engine
    except Exception as e:
        print(f"Unable to initialize MySQL engine: {e}")
        return None