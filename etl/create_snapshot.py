import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from db_config import get_connection


def create_new_snapshot(
    engine: Engine, 
    source_id: int, 
    fiscal_year: int, 
    version_tag: str
) -> Optional[int]:
    """
    Create or reuse a snapshot for the given fiscal year.

    Usage:
        snapshot_id = create_new_snapshot(engine, 4, 2024, "panel_import_v1")

    Args:
        engine (Engine): Active SQLAlchemy engine connected to the warehouse.
        source_id (int): Identifier describing the data source feeding the snapshot.
        fiscal_year (int): Fiscal year to associate with the snapshot.
        version_tag (str): Human-readable label that distinguishes snapshot batches.

    Returns:
        Optional[int]: Snapshot identifier when the operation succeeds; None otherwise.
    """
    print(f"\nChecking snapshot metadata for fiscal year {fiscal_year}...")

    # Use the calendar date so the lookup aligns with the DATE column in MySQL
    snapshot_date = datetime.datetime.now().strftime('%Y-%m-%d')

    with engine.connect() as conn:
        try:
            check_query = text("""
                SELECT snapshot_id 
                FROM fact_data_snapshot 
                WHERE DATE(snapshot_date) = :date 
                  AND fiscal_year = :year 
                  AND source_id = :source 
                  AND version_tag = :tag
            """)
            existing_id = conn.execute(check_query, {
                "date": snapshot_date,
                "year": fiscal_year,
                "source": source_id,
                "tag": version_tag
            }).scalar()

            if existing_id:
                print(f"Existing snapshot located. Reusing id {existing_id}.")
                return existing_id

            insert_query = text("""
                INSERT INTO fact_data_snapshot (snapshot_date, fiscal_year, source_id, version_tag, created_by) 
                VALUES (:date, :year, :source, :tag, 'python_script')
            """)
            conn.execute(insert_query, {
                "date": snapshot_date,
                "year": fiscal_year,
                "source": source_id,
                "tag": version_tag
            })
            conn.commit()
            
            get_id_query = text("SELECT LAST_INSERT_ID()")
            new_id = conn.execute(get_id_query).scalar()
            
            print(f"Created snapshot id {new_id} for fiscal year {fiscal_year}.")
            return new_id
            
        except Exception as e:
            print(f"Snapshot management failed: {e}")
            return None


if __name__ == "__main__":
    print("Validating snapshot creation utility\n" + "-" * 30)

    # Call the imported function to get the engine
    db_engine = get_connection()
    
    if db_engine:
        my_snapshot_id = create_new_snapshot(
            engine=db_engine,
            source_id=2,
            fiscal_year=2024,
            version_tag="v1.0_panel_load"
        )