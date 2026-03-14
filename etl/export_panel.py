import os
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db_config import get_connection


OUTPUT_PATH = Path(__file__).resolve().parents[1] / "outputs" / "panel_latest.csv"

LATEST_PANEL_QUERY = """
WITH ranked_fin AS (
    SELECT
        ff.*,
        ROW_NUMBER() OVER (
            PARTITION BY ff.firm_id, ff.fiscal_year
            ORDER BY ds.snapshot_date DESC, ff.snapshot_id DESC
        ) AS rn
    FROM fact_financial_year ff
    JOIN fact_data_snapshot ds ON ds.snapshot_id = ff.snapshot_id
),
latest_fin AS (
    SELECT *
    FROM ranked_fin
    WHERE rn = 1
),
ranked_own AS (
    SELECT
        fo.*,
        ROW_NUMBER() OVER (
            PARTITION BY fo.firm_id, fo.fiscal_year
            ORDER BY ds.snapshot_date DESC, fo.snapshot_id DESC
        ) AS rn
    FROM fact_ownership_year fo
    JOIN fact_data_snapshot ds ON ds.snapshot_id = fo.snapshot_id
),
latest_own AS (
    SELECT *
    FROM ranked_own
    WHERE rn = 1
),
ranked_market AS (
    SELECT
        fm.*,
        ROW_NUMBER() OVER (
            PARTITION BY fm.firm_id, fm.fiscal_year
            ORDER BY ds.snapshot_date DESC, fm.snapshot_id DESC
        ) AS rn
    FROM fact_market_year fm
    JOIN fact_data_snapshot ds ON ds.snapshot_id = fm.snapshot_id
),
latest_market AS (
    SELECT *
    FROM ranked_market
    WHERE rn = 1
),
ranked_cf AS (
    SELECT
        fc.*,
        ROW_NUMBER() OVER (
            PARTITION BY fc.firm_id, fc.fiscal_year
            ORDER BY ds.snapshot_date DESC, fc.snapshot_id DESC
        ) AS rn
    FROM fact_cashflow_year fc
    JOIN fact_data_snapshot ds ON ds.snapshot_id = fc.snapshot_id
),
latest_cf AS (
    SELECT *
    FROM ranked_cf
    WHERE rn = 1
),
ranked_meta AS (
    SELECT
        fm.*,
        ROW_NUMBER() OVER (
            PARTITION BY fm.firm_id, fm.fiscal_year
            ORDER BY ds.snapshot_date DESC, fm.snapshot_id DESC
        ) AS rn
    FROM fact_firm_year_meta fm
    JOIN fact_data_snapshot ds ON ds.snapshot_id = fm.snapshot_id
),
latest_meta AS (
    SELECT *
    FROM ranked_meta
    WHERE rn = 1
)
SELECT
    df.ticker,
    lf.fiscal_year,
    lf.net_sales,
    lf.total_assets,
    lf.selling_expenses,
    lf.general_admin_expenses,
    lf.intangible_assets_net,
    lf.manufacturing_overhead,
    lf.net_operating_income,
    lf.raw_material_consumption,
    lf.merchandise_purchase_year,
    lf.wip_goods_purchase,
    lf.outside_manufacturing_expenses,
    lf.production_cost,
    lf.rnd_expenses,
    lf.net_income,
    lf.total_equity,
    lf.total_liabilities,
    lf.cash_and_equivalents,
    lf.long_term_debt,
    lf.current_assets,
    lf.current_liabilities,
    lf.growth_ratio,
    lf.inventory,
    lf.net_ppe,
    lo.managerial_inside_own,
    lo.state_own,
    lo.institutional_own,
    lo.foreign_own,
    lm.shares_outstanding,
    lm.price_reference,
    lm.share_price,
    lm.market_value_equity,
    lm.dividend_cash_paid,
    lm.eps_basic,
    lcf.net_cfo,
    lcf.capex,
    lcf.net_cfi,
    lmeta.employees_count,
    lmeta.firm_age
FROM latest_fin lf
JOIN dim_firm df ON df.firm_id = lf.firm_id
LEFT JOIN latest_own lo ON lo.firm_id = lf.firm_id AND lo.fiscal_year = lf.fiscal_year
LEFT JOIN latest_market lm ON lm.firm_id = lf.firm_id AND lm.fiscal_year = lf.fiscal_year
LEFT JOIN latest_cf lcf ON lcf.firm_id = lf.firm_id AND lcf.fiscal_year = lf.fiscal_year
LEFT JOIN latest_meta lmeta ON lmeta.firm_id = lf.firm_id AND lmeta.fiscal_year = lf.fiscal_year
ORDER BY df.ticker, lf.fiscal_year;
"""

def fetch_latest_panel(engine: Engine) -> DataFrame:
    """
    Retrieve the newest firm panel across all fact tables.

    Usage:
        panel_df = fetch_latest_panel(engine)

    Args:
        engine (Engine): Active SQLAlchemy engine targeting the warehouse.

    Returns:
        DataFrame: Flattened dataset constrained to the latest snapshot per firm-year.
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(LATEST_PANEL_QUERY), conn)
    return df


def export_to_csv(df: DataFrame, output_path: Path = OUTPUT_PATH) -> Path:
    """
    Persist the DataFrame to disk and ensure the folder exists.

    Usage:
        export_to_csv(panel_df, Path("../outputs/panel_latest.csv"))

    Args:
        df (DataFrame): Data to serialize.
        output_path (Path): Destination path for the CSV artifact.

    Returns:
        Path: Absolute path to the written CSV file.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return output_path


def main() -> None:
    """
    Orchestrate the export of the latest firm panel to CSV.

    Usage:
        main()

    Args:
        None

    Returns:
        None
    """
    engine = get_connection()
    if engine is None:
        return

    try:
        panel_df = fetch_latest_panel(engine)
    except Exception as exc:
        print(f"Failed to build panel dataset: {exc}")
        return

    if panel_df.empty:
        print("No rows returned from the latest snapshot query; skipping export.")
        return

    try:
        output_path = export_to_csv(panel_df)
        
        print("Panel export completed successfully.")
        print(f"Output file : {os.path.abspath(output_path)}")
        print(f"Firm count  : {panel_df['ticker'].nunique():>6,}")
        print(f"Years       : {sorted(panel_df['fiscal_year'].unique())}")
        print(f"Row count   : {len(panel_df):>6,}")
        print(f"Column count: {len(panel_df.columns):>6,}")
    except Exception as exc:
        print(f"Failed to write CSV: {exc}")


if __name__ == "__main__":
    main()