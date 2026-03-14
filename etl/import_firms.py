from typing import Optional

import pandas as pd
from pandas import DataFrame
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db_config import get_connection


def read_firm_excel() -> Optional[DataFrame]:
    """
    Load firm attributes from the Excel workbook.

    Usage:
        firms_df = read_firm_excel()

    Args:
        None

    Returns:
        Optional[DataFrame]: Populated dataframe when the file exists; None otherwise.
    """
    file_path = '../data/firms.xlsx'

    try:
        df = pd.read_excel(file_path)
        print("Firm metadata workbook loaded; previewing first five rows.")
        print(df.head())
        return df
    except Exception as e:
        print("Unable to read firms.xlsx. Verify the file name and path.")
        print(f"Error details: {e}")
        return None


# --- PART 2: IMPORT DATA TO MYSQL (INSERT AND UPDATE) ---
def import_firms_to_db(df: DataFrame, engine: Engine) -> None:
    """
    Persist firm metadata into dim_firm with idempotent inserts.

    Usage:
        import_firms_to_db(firms_df, engine)

    Args:
        df (DataFrame): Firm master data sourced from the Excel workbook.
        engine (Engine): SQLAlchemy engine connected to the warehouse.

    Returns:
        None
    """
    print("Loading firm metadata into dim_firm...")

    exchange_map = {
        'HOSE': 1,
        'HNX': 2
    }
    
    industry_map = {
        'Materials': 1,                         
        'Food, Bev. & Tobacco': 2,              
        'Pharma., Bio. & Life Sciences': 9,     
        'Automobiles & Components': 8,          
        'Capital Goods': 5,                     
        'Transportation': 10,                   
        'Telecommunication': 11                 
    }

    with engine.connect() as conn:
        success_count = 0
        
        for index, row in df.iterrows():
            try:
                ticker_val = row['Ticker']               
                name_val = row['Company Name']           
                exchange_text = row['Exchange']           
                industry_text = row['Industry Level 2']   

                exchange_id_val = exchange_map.get(exchange_text, 1) 
                industry_id_val = industry_map.get(industry_text, 1)

                sql_query = text("""
                    INSERT INTO dim_firm (ticker, company_name, exchange_id, industry_l2_id) 
                    VALUES (:ticker, :name, :exchange_id, :industry_id)
                    ON DUPLICATE KEY UPDATE 
                    company_name = VALUES(company_name), 
                    exchange_id = VALUES(exchange_id), 
                    industry_l2_id = VALUES(industry_l2_id);
                """)
                
                conn.execute(sql_query, {
                    "ticker": ticker_val, 
                    "name": name_val, 
                    "exchange_id": exchange_id_val, 
                    "industry_id": industry_id_val
                })
                conn.commit() 
                success_count += 1
                
            except Exception as e:
                print(f"Row {index + 2} (ticker={row.get('Ticker', 'Unknown')}) failed: {e}")
        
            print(f"Completed dim_firm load. {success_count} rows inserted or updated.")

# --- PART 3: MAIN EXECUTION SCRIPT ---
if __name__ == "__main__":
    print("Initializing firm import workflow\n" + "-" * 30)

    db_engine = get_connection()
    
    df_firms = read_firm_excel()
    
    print("-" * 30)
    if db_engine is not None and df_firms is not None:
        import_firms_to_db(df_firms, db_engine)
        print("Firm dimension import script completed successfully.")