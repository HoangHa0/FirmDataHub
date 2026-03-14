from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from sqlalchemy import text

from create_snapshot import create_new_snapshot
from db_config import get_connection      
from datetime import date
from pathlib import Path
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.sql.elements import TextClause


DEFAULT_UNIT_SCALE = 1
DEFAULT_CURRENCY = 'VND'
INNOVATION_EVIDENCE_SOURCE_ID = 4
HEADER_MARKERS = {"StockCode", "YearEnd"}
SNAPSHOT_SOURCE_ID = 4
SNAPSHOT_VERSION_TAG = 'panel_import_v1'
SNAPSHOT_CREATED_BY = 'create_snapshot.py'
DATA_FILE = Path(__file__).resolve().parent.parent / 'data' / 'panel_2020_2024.xlsx'

# Mapping dictionaries keep all headers in one place for quick edits.
OWNERSHIP_MAP = {
    'managerial_inside_own': ('Managerial/Inside ownership', True),
    'state_own': ('State ownership', True),
    'institutional_own': ('Institutional ownership', True),
    'foreign_own': ('Foreign ownership', True),
    'note': ('Note', False),
}

FINANCIAL_MAP = {
    'net_sales': 'Net sales revenue',
    'total_assets': 'Total assets',
    'selling_expenses': 'Selling expenses',
    'general_admin_expenses': 'General and administrative expenditure',
    'intangible_assets_net': 'Value of intangible assets',
    'manufacturing_overhead': 'Manufacturing overhead (Indirect cost)',
    'net_operating_income': 'Net operating income',
    'raw_material_consumption': 'Consumption of raw material',
    'merchandise_purchase_year': 'Merchandise purchase of the year',
    'wip_goods_purchase': 'Work-in-progess goods purchase',
    'outside_manufacturing_expenses': 'Outside manufacturing expenses',
    'production_cost': 'Production cost',
    'rnd_expenses': 'R&D expenditure',
    'net_income': 'Net Income',
    'total_equity': "Total shareholders' equity",
    'total_liabilities': 'Total liabilities',
    'cash_and_equivalents': 'Cash and cash equivalent',
    'long_term_debt': 'Long-term debt',
    'current_assets': 'Current assets',
    'current_liabilities': 'Current liabiltiies',
    'growth_ratio': ('Growth ratio', True),
    'inventory': 'Total inventory',
    'net_ppe': 'Net plant, property and equipment',
}

CASHFLOW_MAP = {
    'net_cfo': 'Net cash from operating activities',
    'capex': 'Capital expenditure',
    'net_cfi': 'Cash flows from investing activities',
}

MARKET_MAP = {
    'shares_outstanding': 'Total share outstanding',
    'price_reference': 'Price reference',
    'share_price': 'Share price',
    'market_value_equity': 'Market value of equity',
    'dividend_cash_paid': 'Divident payment',
    'eps_basic': 'EPS',
}

META_MAP = {
    'employees_count': 'Number of employees',
    'firm_age': 'Firm age',
}

INNOVATION_MAP = {
    'product_innovation': 'Product innovation',
    'process_innovation': 'Process innovation',
    'evidence_note': 'Innovation note',
}


def read_panel_excel(file_path: Union[str, Path]) -> DataFrame:
    """Read the panel workbook and normalize obvious placeholders.

    Usage:
        df = read_panel_excel(Path('../data/panel_2020_2024.xlsx'))

    Args:
        file_path (Union[str, Path]): Location of the Excel workbook containing the panel data.

    Returns:
        DataFrame: Cleaned dataframe starting at the detected header row.
    """
    preview = pd.read_excel(file_path, header=None, nrows=15)
    header_row = None

    for idx, row in preview.iterrows():
        values = {
            str(value).strip()
            for value in row.tolist()
            if isinstance(value, str) and value.strip()
        }
        if HEADER_MARKERS.issubset(values):
            header_row = idx
            break

    if header_row is None:
        raise ValueError(
            "Could not locate a header row that includes StockCode and YearEnd."
        )

    df = pd.read_excel(file_path, header=header_row)
    df.columns = [str(col).strip() if col is not None else col for col in df.columns]
    df = df.replace({np.nan: None, "-": None})
    df = df.dropna(how="all")
    return df


def clean_numeric(value: Any, is_pct: bool = False) -> Optional[float]:
    """Normalize numeric inputs, optionally scaling percentages.

    Usage:
        ratio = clean_numeric('45%', is_pct=True)

    Args:
        value (Any): Source value to normalize.
        is_pct (bool): Flag indicating whether the value represents a percentage.

    Returns:
        Optional[float]: Parsed float or None when parsing fails.
    """
    if value is None or value == '' or value == '-':
        return None

    try:
        if isinstance(value, str):
            clean_val = value.replace(',', '').replace('%', '').strip()
            result = float(clean_val)
        else:
            result = float(value)

        if is_pct and (result > 1 or (isinstance(value, str) and '%' in value)):
            return result / 100

        return result
    except Exception:
        return None


def clean_boolean(value: Any) -> Optional[int]:
    """Convert common truthy and falsy tokens into 1/0 integers.

    Usage:
        flag = clean_boolean('Yes')

    Args:
        value (Any): Source value to interpret.

    Returns:
        Optional[int]: 1 for truthy inputs, 0 for falsy inputs, or None if indeterminate.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None

    if isinstance(value, (int, float)):
        return 1 if value > 0 else 0

    lowered = str(value).strip().lower()
    if lowered in {'1', 'y', 'yes', 'true', 'có', 'co'}:
        return 1
    if lowered in {'0', 'n', 'no', 'false', 'không', 'khong'}:
        return 0
    return None


def fetch_firm_map(conn: Connection) -> Dict[str, int]:
    """
    Create a ticker-to-firm-id mapping for fast lookups.

    Usage:
        firm_map = fetch_firm_map(conn)

    Args:
        conn (Connection): Active SQLAlchemy connection.

    Returns:
        Dict[str, int]: Uppercase ticker symbol mapped to firm_id.
    """ 
    result = conn.execute(text("SELECT ticker, firm_id FROM dim_firm"))
    return {row.ticker.strip().upper(): row.firm_id for row in result}


def normalize_ticker(value: Any) -> Optional[str]:
    """
    Normalize ticker symbols to uppercase strings.

    Usage:
        ticker = normalize_ticker(' VNM ')

    Args:
        value (Any): Raw field pulled from the Excel workbook.

    Returns:
        Optional[str]: Uppercase ticker or None when the input is missing.
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return str(value).strip().upper()


def normalize_year(value: Any) -> Optional[int]:
    """
    Parse fiscal year values from the workbook.

    Usage:
        year = normalize_year('2023')

    Args:
        value (Any): Year column entry, typically numeric or string.

    Returns:
        Optional[int]: Parsed fiscal year or None when parsing fails.
    """ 
    try:
        if value is None or value == '':
            return None
        return int(str(value).strip())
    except Exception:
        return None


def upsert(
    conn: Connection, 
    sql: TextClause, 
    payload: Dict[str, Any]
) -> None:
    """Execute an UPSERT statement with the provided payload.

    Usage:
        upsert(conn, sql, payload)

    Args:
        conn (Connection): Open connection used to execute the statement.
        sql (TextClause): Prepared SQL text containing the upsert logic.
        payload (Dict[str, Any]): Parameter dictionary bound to the SQL statement.

    Returns:
        None
    """
    conn.execute(sql, payload)


def infer_snapshot_fiscal_year(df: DataFrame) -> int:
    """Identify the most recent fiscal year represented in the dataframe.

    Usage:
        latest_year = infer_snapshot_fiscal_year(df)

    Args:
        df (DataFrame): Panel dataframe containing the YearEnd column.

    Returns:
        int: Most recent fiscal year found, or the current year when none exist.
    """ 
    years = [normalize_year(y) for y in df.get('YearEnd', [])]
    valid_years = [year for year in years if year is not None]
    return max(valid_years) if valid_years else date.today().year


def insert_ownership(
    conn: Connection, 
    firm_id: int, 
    year: int, 
    snapshot_id: int, 
    row: Series
) -> None:
    """Upsert ownership ratios for a specific firm-year.

    Usage:
        insert_ownership(conn, 1, 2024, snapshot_id, row)

    Args:
        conn (Connection): Open connection to the warehouse.
        firm_id (int): Identifier of the firm being processed.
        year (int): Fiscal year for the ownership attributes.
        snapshot_id (int): Snapshot identifier applied to the row.
        row (Series): Source data extracted from the Excel workbook.

    Returns:
        None
    """
    sql = text("""
        INSERT INTO fact_ownership_year (
            snapshot_id, firm_id, fiscal_year,
            managerial_inside_own, state_own, institutional_own, foreign_own, note
        )
        VALUES (
            :snapshot_id, :firm_id, :fiscal_year,
            :managerial_inside_own, :state_own, :institutional_own, :foreign_own, :note
        )
        ON DUPLICATE KEY UPDATE
            managerial_inside_own = VALUES(managerial_inside_own),
            state_own = VALUES(state_own),
            institutional_own = VALUES(institutional_own),
            foreign_own = VALUES(foreign_own),
            note = VALUES(note);
    """)

    payload = {
        'snapshot_id': snapshot_id,
        'firm_id': firm_id,
        'fiscal_year': year,
        'managerial_inside_own': clean_numeric(row.get(OWNERSHIP_MAP['managerial_inside_own'][0]), True),
        'state_own': clean_numeric(row.get(OWNERSHIP_MAP['state_own'][0]), True),
        'institutional_own': clean_numeric(row.get(OWNERSHIP_MAP['institutional_own'][0]), True),
        'foreign_own': clean_numeric(row.get(OWNERSHIP_MAP['foreign_own'][0]), True),
        'note': row.get(OWNERSHIP_MAP['note'][0]),
    }
    upsert(conn, sql, payload)


def insert_financial(
    conn: Connection, 
    firm_id: int, 
    year: int, 
    snapshot_id: int, 
    row: Series
) -> None:
    """
    Upsert the financial statement metrics for a firm-year.

    Usage:
        insert_financial(conn, 1, 2024, snapshot_id, row)

    Args:
        conn (Connection): Open connection to the warehouse.
        firm_id (int): Identifier of the firm being processed.
        year (int): Fiscal year for the financial metrics.
        snapshot_id (int): Snapshot identifier applied to the row.
        row (Series): Source data extracted from the Excel workbook.

    Returns:
        None
    """
    sql = text("""
        INSERT INTO fact_financial_year (
            firm_id, fiscal_year, snapshot_id, unit_scale, currency_code,
            net_sales, total_assets, selling_expenses, general_admin_expenses,
            intangible_assets_net, manufacturing_overhead, net_operating_income,
            raw_material_consumption, merchandise_purchase_year, wip_goods_purchase,
            outside_manufacturing_expenses, production_cost, rnd_expenses,
            net_income, total_equity, total_liabilities, cash_and_equivalents,
            long_term_debt, current_assets, current_liabilities, growth_ratio,
            inventory, net_ppe
        )
        VALUES (
            :firm_id, :fiscal_year, :snapshot_id, :unit_scale, :currency_code,
            :net_sales, :total_assets, :selling_expenses, :general_admin_expenses,
            :intangible_assets_net, :manufacturing_overhead, :net_operating_income,
            :raw_material_consumption, :merchandise_purchase_year, :wip_goods_purchase,
            :outside_manufacturing_expenses, :production_cost, :rnd_expenses,
            :net_income, :total_equity, :total_liabilities, :cash_and_equivalents,
            :long_term_debt, :current_assets, :current_liabilities, :growth_ratio,
            :inventory, :net_ppe
        )
        ON DUPLICATE KEY UPDATE
            unit_scale = VALUES(unit_scale),
            currency_code = VALUES(currency_code),
            net_sales = VALUES(net_sales),
            total_assets = VALUES(total_assets),
            selling_expenses = VALUES(selling_expenses),
            general_admin_expenses = VALUES(general_admin_expenses),
            intangible_assets_net = VALUES(intangible_assets_net),
            manufacturing_overhead = VALUES(manufacturing_overhead),
            net_operating_income = VALUES(net_operating_income),
            raw_material_consumption = VALUES(raw_material_consumption),
            merchandise_purchase_year = VALUES(merchandise_purchase_year),
            wip_goods_purchase = VALUES(wip_goods_purchase),
            outside_manufacturing_expenses = VALUES(outside_manufacturing_expenses),
            production_cost = VALUES(production_cost),
            rnd_expenses = VALUES(rnd_expenses),
            net_income = VALUES(net_income),
            total_equity = VALUES(total_equity),
            total_liabilities = VALUES(total_liabilities),
            cash_and_equivalents = VALUES(cash_and_equivalents),
            long_term_debt = VALUES(long_term_debt),
            current_assets = VALUES(current_assets),
            current_liabilities = VALUES(current_liabilities),
            growth_ratio = VALUES(growth_ratio),
            inventory = VALUES(inventory),
            net_ppe = VALUES(net_ppe);
    """)

    payload = {
        'firm_id': firm_id,
        'fiscal_year': year,
        'snapshot_id': snapshot_id,
        'unit_scale': DEFAULT_UNIT_SCALE,
        'currency_code': DEFAULT_CURRENCY,
    }

    for db_col, header in FINANCIAL_MAP.items():
        if isinstance(header, tuple):
            payload[db_col] = clean_numeric(row.get(header[0]), header[1])
        else:
            payload[db_col] = clean_numeric(row.get(header))

    upsert(conn, sql, payload)


def insert_cashflow(
    conn: Connection, 
    firm_id: int, 
    year: int, 
    snapshot_id: int, 
    row: Series
) -> None:
    """
    Upsert the cash flow statement metrics for a firm-year.

    Usage:
        insert_cashflow(conn, 1, 2024, snapshot_id, row)

    Args:
        conn (Connection): Open connection to the warehouse.
        firm_id (int): Identifier of the firm being processed.
        year (int): Fiscal year for the cash flow metrics.
        snapshot_id (int): Snapshot identifier applied to the row.
        row (Series): Source data extracted from the Excel workbook.

    Returns:
        None
    """
    sql = text("""
        INSERT INTO fact_cashflow_year (
            firm_id, fiscal_year, snapshot_id, unit_scale, currency_code,
            net_cfo, capex, net_cfi
        )
        VALUES (
            :firm_id, :fiscal_year, :snapshot_id, :unit_scale, :currency_code,
            :net_cfo, :capex, :net_cfi
        )
        ON DUPLICATE KEY UPDATE
            unit_scale = VALUES(unit_scale),
            currency_code = VALUES(currency_code),
            net_cfo = VALUES(net_cfo),
            capex = VALUES(capex),
            net_cfi = VALUES(net_cfi);
    """)

    payload = {
        'firm_id': firm_id,
        'fiscal_year': year,
        'snapshot_id': snapshot_id,
        'unit_scale': DEFAULT_UNIT_SCALE,
        'currency_code': DEFAULT_CURRENCY,
        'net_cfo': clean_numeric(row.get(CASHFLOW_MAP['net_cfo'])),
        'capex': clean_numeric(row.get(CASHFLOW_MAP['capex'])),
        'net_cfi': clean_numeric(row.get(CASHFLOW_MAP['net_cfi'])),
    }
    upsert(conn, sql, payload)


def insert_market(
    conn: Connection, 
    firm_id: int, 
    year: int, 
    snapshot_id: int, 
    row: Series
) -> None:
    """
    Upsert market data attributes for a firm-year.

    Usage:
        insert_market(conn, 1, 2024, snapshot_id, row)

    Args:
        conn (Connection): Open connection to the warehouse.
        firm_id (int): Identifier of the firm being processed.
        year (int): Fiscal year for the market metrics.
        snapshot_id (int): Snapshot identifier applied to the row.
        row (Series): Source data extracted from the Excel workbook.

    Returns:
        None
    """ 
    sql = text("""
        INSERT INTO fact_market_year (
            firm_id, fiscal_year, snapshot_id,
            shares_outstanding, price_reference, share_price,
            market_value_equity, dividend_cash_paid, eps_basic, currency_code
        )
        VALUES (
            :firm_id, :fiscal_year, :snapshot_id,
            :shares_outstanding, :price_reference, :share_price,
            :market_value_equity, :dividend_cash_paid, :eps_basic, :currency_code
        )
        ON DUPLICATE KEY UPDATE
            shares_outstanding = VALUES(shares_outstanding),
            price_reference = VALUES(price_reference),
            share_price = VALUES(share_price),
            market_value_equity = VALUES(market_value_equity),
            dividend_cash_paid = VALUES(dividend_cash_paid),
            eps_basic = VALUES(eps_basic),
            currency_code = VALUES(currency_code);
    """)

    payload = {
        'firm_id': firm_id,
        'fiscal_year': year,
        'snapshot_id': snapshot_id,
        'currency_code': DEFAULT_CURRENCY,
        'shares_outstanding': clean_numeric(row.get(MARKET_MAP['shares_outstanding'])),
        'price_reference': row.get(MARKET_MAP['price_reference']),
        'share_price': clean_numeric(row.get(MARKET_MAP['share_price'])),
        'market_value_equity': clean_numeric(row.get(MARKET_MAP['market_value_equity'])),
        'dividend_cash_paid': clean_numeric(row.get(MARKET_MAP['dividend_cash_paid'])),
        'eps_basic': clean_numeric(row.get(MARKET_MAP['eps_basic'])),
    }
    upsert(conn, sql, payload)


def insert_meta(
    conn: Connection, 
    firm_id: int, 
    year: int, 
    snapshot_id: int, 
    row: Series
) -> None:
    """
    Upsert employment and meta attributes for a firm-year.

    Usage:
        insert_meta(conn, 1, 2024, snapshot_id, row)

    Args:
        conn (Connection): Open connection to the warehouse.
        firm_id (int): Identifier of the firm being processed.
        year (int): Fiscal year for the metadata metrics.
        snapshot_id (int): Snapshot identifier applied to the row.
        row (Series): Source data extracted from the Excel workbook.

    Returns:
        None
    """
    sql = text("""
        INSERT INTO fact_firm_year_meta (
            firm_id, fiscal_year, snapshot_id, employees_count, firm_age
        )
        VALUES (
            :firm_id, :fiscal_year, :snapshot_id, :employees_count, :firm_age
        )
        ON DUPLICATE KEY UPDATE
            employees_count = VALUES(employees_count),
            firm_age = VALUES(firm_age);
    """)

    payload = {
        'firm_id': firm_id,
        'fiscal_year': year,
        'snapshot_id': snapshot_id,
        'employees_count': clean_numeric(row.get(META_MAP['employees_count'])),
        'firm_age': clean_numeric(row.get(META_MAP['firm_age'])),
    }
    upsert(conn, sql, payload)


def insert_innovation(
    conn: Connection, 
    firm_id: int, 
    year: int, 
    snapshot_id: int, 
    row: Series
) -> None:
    """Upsert innovation indicators and supporting evidence.

    Usage:
        insert_innovation(conn, 1, 2024, snapshot_id, row)

    Args:
        conn (Connection): Open connection to the warehouse.
        firm_id (int): Identifier of the firm being processed.
        year (int): Fiscal year for the innovation metrics.
        snapshot_id (int): Snapshot identifier applied to the row.
        row (Series): Source data extracted from the Excel workbook.

    Returns:
        None
    """
    sql = text("""
        INSERT INTO fact_innovation_year (
            firm_id, fiscal_year, snapshot_id,
            product_innovation, process_innovation,
            evidence_source_id, evidence_note
        )
        VALUES (
            :firm_id, :fiscal_year, :snapshot_id,
            :product_innovation, :process_innovation,
            :evidence_source_id, :evidence_note
        )
        ON DUPLICATE KEY UPDATE
            product_innovation = VALUES(product_innovation),
            process_innovation = VALUES(process_innovation),
            evidence_source_id = VALUES(evidence_source_id),
            evidence_note = VALUES(evidence_note);
    """)

    payload = {
        'firm_id': firm_id,
        'fiscal_year': year,
        'snapshot_id': snapshot_id,
        'product_innovation': clean_boolean(row.get(INNOVATION_MAP['product_innovation'])),
        'process_innovation': clean_boolean(row.get(INNOVATION_MAP['process_innovation'])),
        'evidence_source_id': INNOVATION_EVIDENCE_SOURCE_ID,
        'evidence_note': row.get(INNOVATION_MAP['evidence_note']),
    }
    upsert(conn, sql, payload)


def import_panel_data(
    engine: Engine, 
    file_path: Union[str, Path], 
    snapshot_dict: Dict[int, int]
) -> None:
    """
    Load the panel workbook and write each firm-year to the fact tables.

    Usage:
        import_panel_data(engine, '../data/panel.xlsx', snapshot_dict)

    Args:
        engine (Engine): Active SQLAlchemy engine connected to the warehouse.
        file_path (Union[str, Path]): File system path to the panel workbook.
        snapshot_dict (Dict[int, int]): Mapping of fiscal year to snapshot identifiers.

    Returns:
        None
    """
    print("Reading panel workbook and preparing payloads...")

    try:
        df = read_panel_excel(file_path)
    except Exception as exc:
        print(f"Unable to read panel workbook: {exc}")
        return

    with engine.connect() as conn:
        firm_map = fetch_firm_map(conn)
        success_count = 0
        total_rows = len(df)
        missing_ticker = 0
        missing_year = 0
        unknown_ticker = 0

        for _, row in df.iterrows():
            ticker = normalize_ticker(row.get('StockCode'))
            year = normalize_year(row.get('YearEnd'))

            if ticker is None:
                missing_ticker += 1
                continue

            if year is None:
                missing_year += 1
                continue

            firm_id = firm_map.get(ticker)
            if not firm_id:
                unknown_ticker += 1
                continue

            current_snapshot_id = snapshot_dict.get(year)

            if not current_snapshot_id:
                print(f"Snapshot id for fiscal year {year} was not provided. Skipping row.")
                continue

            try:
                insert_ownership(conn, firm_id, year, current_snapshot_id, row)
                insert_financial(conn, firm_id, year, current_snapshot_id, row)
                insert_cashflow(conn, firm_id, year, current_snapshot_id, row)
                insert_market(conn, firm_id, year, current_snapshot_id, row)
                insert_meta(conn, firm_id, year, current_snapshot_id, row)
                insert_innovation(conn, firm_id, year, current_snapshot_id, row)
                conn.commit()
                success_count += 1
            except Exception as exc:
                conn.rollback()
                print(f"Error while importing {ticker} {year}: {exc}")

        print(f"\nPanel import summary")
        print(f"Successful firm-years : {success_count}")
        print(f"Total rows processed  : {total_rows}")
        print(f"Missing ticker rows   : {missing_ticker}")
        print(f"Missing year rows     : {missing_year}")
        print(f"Unknown ticker rows   : {unknown_ticker}")

# Script entry point
if __name__ == "__main__":
    db_engine = get_connection()
    
    if db_engine:
        path = "../data/panel_2020_2024.xlsx"

        print("Analyzing the panel workbook to identify fiscal years...")

        try:
            temp_df = pd.read_excel(path)

            unique_years = temp_df["YearEnd"].dropna().unique()

            snapshot_dict = {}

            print(f"Detected {len(unique_years)} fiscal years: {unique_years}")
            print("Creating or reusing snapshot ids for each fiscal year...")

            for y in unique_years:
                year_val = int(y)

                snap_id = create_new_snapshot(
                    engine=db_engine,
                    source_id=4,
                    fiscal_year=year_val,  # Pass the correct year into the database
                    version_tag="panel_import_v1"
                )

                if snap_id:
                    snapshot_dict[year_val] = snap_id
                else:
                    raise Exception(f"Unable to create Snapshot for year {year_val}")

            print(f"Snapshot readiness complete: {snapshot_dict}")
            print("Starting panel import...")

            import_panel_data(db_engine, path, snapshot_dict)

        except Exception as e:
            print(f"Panel import failed during snapshot preparation: {e}")