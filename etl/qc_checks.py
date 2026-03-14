import os
import pandas as pd
from db_config import get_connection

def run_qc_checks() -> None:
    """Evaluate basic accounting rules and export any exceptions.

    Usage:
        run_qc_checks()

    Args:
        None

    Returns:
        None
    """
    print(f"\n{'=' * 35}\nStarting data quality checks\n{'=' * 35}")

    engine = get_connection()
    qc_errors = []

    try:
        # Fetching data using JOINs across Financial, Ownership, and Market tables
        query = """
            SELECT 
                f.ticker, fin.fiscal_year,
                own.managerial_inside_own, own.state_own, own.institutional_own, own.foreign_own,
                fin.total_assets, fin.current_liabilities, fin.growth_ratio,
                mkt.shares_outstanding, mkt.share_price, mkt.market_value_equity
            FROM dim_firm f
            JOIN fact_financial_year fin ON f.firm_id = fin.firm_id
            LEFT JOIN fact_ownership_year own ON f.firm_id = own.firm_id AND fin.fiscal_year = own.fiscal_year
            LEFT JOIN fact_market_year mkt ON f.firm_id = mkt.firm_id AND fin.fiscal_year = mkt.fiscal_year
        """
        df = pd.read_sql(query, engine)
        print(f"Loaded {len(df)} firm-year rows for evaluation.")
        print("Evaluating QC rules...")
        
        # Iterate through each row to check financial logic
        for index, row in df.iterrows():
            ticker = row['ticker']
            year = row['fiscal_year']

            # RULE 1: Ownership ratios must be in the range [0, 1]
            own_cols = ['managerial_inside_own', 'state_own', 'institutional_own', 'foreign_own']
            for col in own_cols:
                val = row.get(col)
                if pd.notnull(val) and (val < 0 or val > 1):
                    qc_errors.append([ticker, year, col, 'Invalid Range', f'Ownership ratio {val} is not in range [0,1]'])

            # RULE 2: Shares outstanding > 0 (if available)
            shares = row.get('shares_outstanding')
            if pd.notnull(shares) and shares <= 0:
                qc_errors.append([ticker, year, 'shares_outstanding', 'Zero or Negative', f'Shares outstanding must be strictly > 0, got: {shares}'])

            # RULE 3: Total assets >= 0 (Assets cannot be negative)
            assets = row.get('total_assets')
            if pd.notnull(assets) and assets < 0:
                qc_errors.append([ticker, year, 'total_assets', 'Negative Value', f'Total assets cannot be negative: {assets}'])

            # RULE 4: Current liabilities >= 0 (Liabilities cannot be negative)
            liab = row.get('current_liabilities')
            if pd.notnull(liab) and liab < 0:
                qc_errors.append([ticker, year, 'current_liabilities', 'Negative Value', f'Current liabilities cannot be negative: {liab}'])

            # RULE 5: Growth ratio must be within a reasonable range [-0.95 to 5.0]
            growth = row.get('growth_ratio')
            if pd.notnull(growth) and (growth < -0.95 or growth > 5.0):
                qc_errors.append([ticker, year, 'growth_ratio', 'Outlier', f'Abnormal growth ratio detected: {growth}'])

            # RULE 6: market_value_equity ≈ shares_outstanding * share_price
            price = row.get('share_price')
            mve = row.get('market_value_equity')

            if pd.notnull(shares) and pd.notnull(price) and pd.notnull(mve):
                calculated_mve = shares * price
                
                # Prevent division by zero just in case
                if calculated_mve > 0:
                    # Allowing a 5% margin of error (0.05) due to rounding or average pricing differences
                    error_margin = abs(mve - calculated_mve) / calculated_mve
                    if error_margin > 0.05:
                        qc_errors.append([
                            ticker, year, 'market_value_equity', 'Calculation Mismatch', 
                            f'Reported MVE ({mve}) deviates >5% from Calculated MVE (Shares * Price = {calculated_mve})'
                        ])

        # Export report to CSV
        output_folder = "../outputs"
        os.makedirs(output_folder, exist_ok=True)
        report_path = os.path.join(output_folder, "qc_report.csv")

        # Create DataFrame strictly adhering to the 5 requested columns
        df_report = pd.DataFrame(qc_errors, columns=['ticker', 'fiscal_year', 'field_name', 'error_type', 'message'])
        
        # Save to CSV
        df_report.to_csv(report_path, index=False, encoding='utf-8-sig')

        if len(qc_errors) > 0:
            print(f"Detected {len(qc_errors)} QC exceptions.")
            print(f"Detailed report saved to {report_path}.")
        else:
            print("No QC exceptions were detected; a placeholder report was generated.")

    except Exception as e:
        print(f"QC execution failed: {e}")

if __name__ == "__main__":
    run_qc_checks()