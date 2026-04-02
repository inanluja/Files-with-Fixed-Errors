import sqlite3
import pandas as pd
from datetime import datetime


def import_to_positions(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'positions'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        column_mapping = {
            'DEAL_ID': 'deal_id',
            'ISIN_CD': 'isin_deal',
            'START_DATE': 'start_date',
            'MATURITY_DATE': 'maturity_date',
            'QUANTITY': 'quantity',
            'EMITENT': 'issuer',
            'CLEAN_PRICE': 'initial_clean_price',
            'DIRTY_PRICE': 'initial_dirty_price',
            'CURRENCY_CD': 'currency',
            'BOND_TYPE': 'bond_type',
            'COUPON_RATE': 'coupon_decimal',
            'NOMINAL': 'total_notional',
            'PR_DSC': 'PR_DSC',
            'QUOTE_MTM': 'current_clean_price',
            'PRINCIPAL': 'invested_amount',
            'PRINCIPAL_AZN': 'invested_amount_azn',
            'PRINCIPAL_ACC_DEF': 'PRINCIPAL_ACC_DEF',
            'ACCRUAL_EIR': 'accrued_interest',
            'ACCRUAL_EIR_AZN': 'accrued_interest_azn',
            'ACCRUAL_ACC_DEF': 'ACCRUAL_ACC_DEF',
            'MTM': 'revaluation',
            'MTM_AZN': 'revaluation_azn',
            'MTM_ACC_DEF': 'MTM_ACC_DEF',
            'REPORT_DATE': 'RepDate',
            'PRODUCT_ISSUE': 'PRODUCT_ISSUE',
            'EMITENT_RATING': 'internal_rating',
            'EXTERNAL_ID': 'EXTERNAL_ID',
            'COUNTRY': 'country',
            'EXPENSE_AMOUNT': 'EXPENSE_AMOUNT',
            'INCOME_AMOUNT': 'INCOME_AMOUNT',
            'OCI': 'OCI',
            'SETTLEMENT_AMOUNT': 'SETTLEMENT_AMOUNT',
            'CLASSIFICATION': 'portfolio_type',
            'PROVISION_AMOUNT': 'provisions',
            'PROVISION_AMOUNT_AZN': 'provisions_azn',
            'time_stamp' : 'time_stamp'
         }

        df = df.rename(columns=column_mapping)

        # Prices in new portfolio are in percentage format (e.g. 98.5),
        # but formulas expect decimal (e.g. 0.985). Convert here.
        for price_col in ['initial_clean_price', 'initial_dirty_price', 'current_clean_price']:
            if price_col in df.columns:
                df[price_col] = pd.to_numeric(df[price_col], errors='coerce') / 100

        # total_notional: new NOMINAL = par value per bond (e.g. 100 or 1000).
        # Recalculate as total face value = quantity × par_value so downstream
        # check_bond_par_value gives: SUM(total_notional)/SUM(quantity) = par_value.
        df['total_notional'] = (pd.to_numeric(df['quantity'], errors='coerce') *
                                pd.to_numeric(df['total_notional'], errors='coerce'))

        # coupon_decimal: old format stored as "5.000000%" string which fails
        # inserting into REAL column. Strip "%" and convert to decimal (0.05).
        def _parse_coupon(x):
            if pd.isna(x) or str(x).strip() == '':
                return float('nan')
            s = str(x).strip()
            if s.endswith('%'):
                return float(s[:-1]) / 100
            return pd.to_numeric(s, errors='coerce')

        df['coupon_decimal'] = df['coupon_decimal'].apply(_parse_coupon)

        # portfolio_type: new CLASSIFICATION uses short names ("AFS", "HTM").
        # Normalize to full names expected by all downstream notebooks and reports.
        _portfolio_type_map = {
            'AFS': 'AFS PORTFOLIO',
            'HTM': 'HTM PORTFOLIO',
            'FVTPL': 'FVTPL PORTFOLIO',
            'TRADING': 'TRADING PORTFOLIO',
        }
        df['portfolio_type'] = df['portfolio_type'].replace(_portfolio_type_map)

        required_columns = [
            'deal_id', 'isin_deal', 'start_date', 'maturity_date', 'quantity', 'issuer', 'initial_clean_price',
            'initial_dirty_price', 'currency', 'bond_type', 'coupon_decimal', 'total_notional', 'PR_DSC',
            'current_clean_price', 'invested_amount', 'invested_amount_azn', 'PRINCIPAL_ACC_DEF', 'accrued_interest',
            'accrued_interest_azn', 'ACCRUAL_ACC_DEF', 'revaluation', 'revaluation_azn', 'MTM_ACC_DEF', 'RepDate',
            'PRODUCT_ISSUE', 'internal_rating', 'EXTERNAL_ID', 'country', 'EXPENSE_AMOUNT', 'INCOME_AMOUNT',
            'OCI', 'SETTLEMENT_AMOUNT', 'portfolio_type', 'provisions', 'provisions_azn', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

        # Convert date columns to string format if they're datetime objects
        if 'datetime' in str(df['RepDate'].dtype):
            df['RepDate'] = df['RepDate'].dt.strftime('%Y-%m-%d')
        df['time_stamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO positions (
                            deal_id, isin_deal, start_date, maturity_date, quantity, issuer, initial_clean_price,
                            initial_dirty_price, currency, bond_type, coupon_decimal, total_notional, PR_DSC,
                            current_clean_price, invested_amount, invested_amount_azn, PRINCIPAL_ACC_DEF, 
                            accrued_interest, accrued_interest_azn, ACCRUAL_ACC_DEF, revaluation, revaluation_azn, 
                            MTM_ACC_DEF, RepDate, PRODUCT_ISSUE, internal_rating, EXTERNAL_ID, country, 
                            EXPENSE_AMOUNT, INCOME_AMOUNT, OCI, SETTLEMENT_AMOUNT, portfolio_type, provisions, 
                            provisions_azn, 'time_stamp') 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?) 
                     ON CONFLICT(deal_id, RepDate) DO UPDATE SET
                        isin_deal = excluded.isin_deal,
                        start_date = excluded.start_date,
                        maturity_date = excluded.maturity_date,
                        quantity = excluded.quantity,
                        issuer = excluded.issuer,
                        initial_clean_price = excluded.initial_clean_price,
                        initial_dirty_price = excluded.initial_dirty_price,
                        currency = excluded.currency,
                        bond_type = excluded.bond_type,
                        coupon_decimal = excluded.coupon_decimal,
                        total_notional = excluded.total_notional,
                        PR_DSC = excluded.PR_DSC,
                        current_clean_price = excluded.current_clean_price,
                        invested_amount = excluded.invested_amount,
                         invested_amount_azn = excluded.invested_amount_azn,
                        PRINCIPAL_ACC_DEF = excluded.PRINCIPAL_ACC_DEF,
                        accrued_interest = excluded.accrued_interest,
                        accrued_interest_azn = excluded.accrued_interest_azn,
                        ACCRUAL_ACC_DEF = excluded.ACCRUAL_ACC_DEF,
                        revaluation = excluded.revaluation,
                         revaluation_azn = excluded.revaluation_azn,
                        MTM_ACC_DEF = excluded.MTM_ACC_DEF,
                        PRODUCT_ISSUE = excluded.PRODUCT_ISSUE,
                        internal_rating = excluded.internal_rating,
                        EXTERNAL_ID = excluded.EXTERNAL_ID,
                        country = excluded.country,
                        EXPENSE_AMOUNT = excluded.EXPENSE_AMOUNT,
                        INCOME_AMOUNT = excluded.INCOME_AMOUNT,
                        OCI = excluded.OCI,
                        SETTLEMENT_AMOUNT = excluded.SETTLEMENT_AMOUNT,
                        portfolio_type = excluded.portfolio_type,
                        provisions = excluded.provisions,
                        provisions_azn = excluded.provisions_azn
                    WHERE excluded.time_stamp > positions.time_stamp
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row)
                         for row in df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-----------------------------------------------------------------------------------


def import_to_dbOAS_Global(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'dbOAS_Global'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'RepDate', 'index_code', 'metric_type', 'DtM', 'bucket',
            'Aaa', 'Aa1', 'Aa2', 'Aa3', 'A1', 'A2', 'A3',
            'Baa1', 'Baa2', 'Baa3', 'Ba1', 'Ba2', 'Ba3',
            'B1', 'B2', 'B3', 'Caa1', 'Caa2', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO dbOAS_Global (RepDate, index_code, metric_type, DtM, bucket, 
                                               Aaa, Aa1, Aa2, Aa3, A1, A2, A3, 
                                               Baa1, Baa2, Baa3, Ba1, Ba2, Ba3, 
                                               B1, B2, B3, Caa1, Caa2, time_stamp) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT(RepDate, index_code, metric_type, bucket) DO UPDATE SET
                         DtM = excluded.DtM,
                         Aaa = excluded.Aaa,
                         Aa1 = excluded.Aa1,
                         Aa2 = excluded.Aa2,
                         Aa3 = excluded.Aa3,
                         A1 = excluded.A1,
                         A2 = excluded.A2,
                         A3 = excluded.A3,
                         Baa1 = excluded.Baa1,
                         Baa2 = excluded.Baa2,
                         Baa3 = excluded.Baa3,
                         Ba1 = excluded.Ba1,
                         Ba2 = excluded.Ba2,
                         Ba3 = excluded.Ba3,
                         B1 = excluded.B1,
                         B2 = excluded.B2,
                         B3 = excluded.B3,
                         Caa1 = excluded.Caa1,
                         Caa2 = excluded.Caa2,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > dbOAS_Global.time_stamp 
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in
                          df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-------------------------------------------------------------------------


def import_to_dbOAS_EM(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'dbOAS_EM'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'RepDate', 'index_code', 'metric_type', 'DtM', 'bucket',
            'Aa2', 'Aa3', 'A1', 'A2', 'A3',
            'Baa1', 'Baa2', 'Baa3', 'Ba1', 'Ba2', 'Ba3',
            'B1', 'B2', 'B3', 'Caa1', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO dbOAS_EM (RepDate, index_code, metric_type, DtM, bucket,
                                           Aa2, Aa3, A1, A2, A3,
                                           Baa1, Baa2, Baa3, Ba1, Ba2, Ba3,
                                           B1, B2, B3, Caa1, time_stamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT(RepDate, index_code, metric_type, bucket) DO UPDATE SET
                         DtM = excluded.DtM,
                         Aa2 = excluded.Aa2,
                         Aa3 = excluded.Aa3,
                         A1 = excluded.A1,
                         A2 = excluded.A2,
                         A3 = excluded.A3,
                         Baa1 = excluded.Baa1,
                         Baa2 = excluded.Baa2,
                         Baa3 = excluded.Baa3,
                         Ba1 = excluded.Ba1,
                         Ba2 = excluded.Ba2,
                         Ba3 = excluded.Ba3,
                         B1 = excluded.B1,
                         B2 = excluded.B2,
                         B3 = excluded.B3,
                         Caa1 = excluded.Caa1,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > dbOAS_EM.time_stamp
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in
                          df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#------------------------------------------------------------------------------


def import_to_yield_curves(excel_file_path, file_sheet_name, db_file_path):
    """Import from Excel file to SQLite database table 'yield_curves'"""

        # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

        # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'RepDate','currency','component_id','index_id','tenor',
            'rate_percent','time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return
    
    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Perform UPSERT operation
    try:
        # SQL for UPSERT (INSERT OR UPDATE)
        insert_sql = """
                    INSERT INTO yield_curves ('RepDate','currency','component_id','index_id','tenor','rate_percent','time_stamp')
                    VALUES (?, ?, ?, ?, ?, ?, ?) 
                    ON CONFLICT(RepDate, Currency, tenor) DO UPDATE SET
                        component_id = excluded.component_id,
                        index_id = excluded.index_id,
                        rate_percent = excluded.rate_percent,
                        time_stamp = excluded.time_stamp
                    WHERE excluded.time_stamp > yield_curves.time_stamp
                    """
        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in
                          df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-------------------------------------------------------------------------------------------


def import_to_dic_issuers(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'dic_issuers'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        # Ensure bloom_company_id is string type to match database
        df['bloom_company_id'] = df['bloom_company_id'].astype(str)

        required_columns = [
            'bloom_company_id', 'company_name', 'equity_ticker', 'company_is_private',
            'country_iso', 'parent_name', 'parent_equity_ticker', 'parent_is_private',
            'parent_country_of_risk', 'bloom_parent_id', 'bloom_class_level1',
            'bloom_class_level2', 'bloom_class_level3', 'bloom_class_level4',
            'ultimate_equity_ticker', 'equity_index', 'pb_customer_id',
            'override_comment', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        # SQL for UPSERT (INSERT OR UPDATE)
        insert_sql = """
                    INSERT INTO dic_issuers (bloom_company_id, company_name, equity_ticker, company_is_private,
                                                  country_iso, parent_name, parent_equity_ticker, parent_is_private,
                                                  parent_country_of_risk, bloom_parent_id, bloom_class_level1,
                                                  bloom_class_level2, bloom_class_level3, bloom_class_level4,
                                                  ultimate_equity_ticker, equity_index, pb_customer_id,
                                                  override_comment, time_stamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                    ON CONFLICT(bloom_company_id) DO UPDATE SET
                             company_name = excluded.company_name,
                             equity_ticker = excluded.equity_ticker,
                             company_is_private = excluded.company_is_private,
                             country_iso = excluded.country_iso,
                             parent_name = excluded.parent_name,
                             parent_equity_ticker = excluded.parent_equity_ticker,
                             parent_is_private = excluded.parent_is_private,
                             parent_country_of_risk = excluded.parent_country_of_risk,
                             bloom_parent_id = excluded.bloom_parent_id,
                             bloom_class_level1 = excluded.bloom_class_level1,
                             bloom_class_level2 = excluded.bloom_class_level2,
                             bloom_class_level3 = excluded.bloom_class_level3,
                             bloom_class_level4 = excluded.bloom_class_level4,
                             ultimate_equity_ticker = excluded.ultimate_equity_ticker,
                             equity_index = excluded.equity_index,
                             pb_customer_id = excluded.pb_customer_id,
                             override_comment = excluded.override_comment,
                             time_stamp = excluded.time_stamp
                    WHERE excluded.time_stamp > dic_issuers.time_stamp
                    """
        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in df[required_columns].to_numpy()]
        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#------------------------------------------------------------------------

def import_to_dic_bonds(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'dic_bonds'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA synchronous = FULL")
        #cursor.execute("PRAGMA foreign_keys = ON") # have to disable - do not understand the source of conflict
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        # Ensure bloom_company_id is string type to match database
        df['bloom_company_id'] = df['bloom_company_id'].astype(str)

        required_columns = [
                   'isin', 'portfolio', 'class_internal', 'pricing_source', 'bloom_company_id', 'bond_name', 'redemption_type',
                   'bond_rank', 'currency', 'maturity', 'coupon_type', 'day_count', 'coupon_frequency', 'coupon',
                   'par_value', 'override_comment', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        #df['bloom_company_id'] = df['bloom_company_id'].astype(str) # hot fix for format mismatch
        # SQL for UPSERT (INSERT OR UPDATE)
        insert_sql = """
                        INSERT INTO dic_bonds ('isin', 'portfolio', 'class_internal', 'pricing_source', 'bloom_company_id', 'bond_name',
                                            'redemption_type', 'bond_rank', 'currency', 'maturity', 'coupon_type',
                                            'day_count', 'coupon_frequency', 'coupon', 'par_value', 
                                            'override_comment', 'time_stamp')
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(isin) DO 
                         UPDATE SET
                             portfolio = excluded.portfolio,
                             class_internal = excluded.class_internal,
                             pricing_source = excluded.pricing_source,
                             bloom_company_id = excluded.bloom_company_id,
                             bond_name = excluded.bond_name,
                             redemption_type = excluded.redemption_type,
                             bond_rank = excluded.bond_rank,
                             currency = excluded.currency,
                             maturity = excluded.maturity,
                             coupon_type = excluded.coupon_type,
                             day_count = excluded.day_count,
                             coupon_frequency = excluded.coupon_frequency,
                             coupon = excluded.coupon,
                             par_value = excluded.par_value,
                             override_comment = excluded.override_comment,
                             time_stamp = excluded.time_stamp
                         WHERE excluded.time_stamp > dic_bonds.time_stamp
                         """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in
                              df[required_columns].to_numpy()]
        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-------------------------------------------------------------------


def import_to_dic_bond_cf(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'dic_bond_cf'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'isin', 'cashflows_date', 'coupon', 'principal', 'data_source', 'override_comment', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        return    
  
    
    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return


    # Perform UPSERT operation
    try:
        # SQL for UPSERT (INSERT OR UPDATE)
        insert_sql = """
                    INSERT INTO dic_bond_cf (isin, cashflows_date, coupon, principal, data_source, 
                                             override_comment, time_stamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?) 
                    ON CONFLICT(isin, cashflows_date) DO UPDATE SET
                             coupon = excluded.coupon,
                             principal = excluded.principal,
                             data_source = excluded.data_source,
                             override_comment = excluded.override_comment,
                             time_stamp = excluded.time_stamp
                    WHERE excluded.time_stamp > dic_bond_cf.time_stamp
                    """
        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in df[required_columns].to_numpy()]
        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-----------------------------------------------------------------------------------


def import_to_risk_free_issuers(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'risk_free_issuers'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'bloom_company_id', 'currency', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error processing data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO risk_free_issuers (
                         bloom_company_id, currency, time_stamp
                     ) 
                     VALUES (?, ?, ?) 
                     ON CONFLICT(bloom_company_id) DO UPDATE SET
                         currency = excluded.currency,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > risk_free_issuers.time_stamp 
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row)
                         for row in df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#----------------------------------------------------------------------------------------------------


def import_to_bond_price(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'bond_price'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'RepDate', 'isin', 'pricing_source', 'ytm_bid',
            'price_bid', 'price_last', 'oas_spread', 'z_spread', 'factor_principal', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO bond_price (
                         RepDate, isin, pricing_source, ytm_bid, 
                         price_bid, price_last, oas_spread, z_spread, factor_principal, time_stamp
                     ) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT(RepDate, isin) DO UPDATE SET
                         pricing_source = excluded.pricing_source,
                         ytm_bid = excluded.ytm_bid,
                         price_bid = excluded.price_bid,
                         price_last = excluded.price_last,
                         oas_spread = excluded.oas_spread,
                         z_spread = excluded.z_spread,
                         factor_principal = excluded.factor_principal,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > bond_price.time_stamp 
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in
                          df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-----------------------------------------------------------------------------------


def import_to_rating_matrix(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'rating_matrix' with Moodys as primary key"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'rating_scale', 'SP', 'Moodys', 'Fitch', 'rating_category',
            'rating_category_sp', 'rating_category_moodys',
            'PD_TTC', 'PD_PIT', 'LGD', 'MDY_PD', 'SP_PD', 'PD',
            'cond_loss', 'expected_loss', 'unexpected_loss', 'valid_since', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation with Moodys as primary key
    try:
        insert_sql = """
                     INSERT INTO rating_matrix (
                         rating_scale, SP, Moodys, Fitch, rating_category,
                         rating_category_sp, rating_category_moodys,
                         PD_TTC, PD_PIT, LGD, MDY_PD, SP_PD, PD,
                         cond_loss, expected_loss, unexpected_loss, valid_since, time_stamp
                     ) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?) 
                     ON CONFLICT(Moodys, valid_since) DO UPDATE SET
                         rating_scale = excluded.rating_scale,
                         SP = excluded.SP,
                         Fitch = excluded.Fitch,
                         rating_category = excluded.rating_category,
                         rating_category_sp = excluded.rating_category_sp,
                         rating_category_moodys = excluded.rating_category_moodys,                         
                         PD_TTC = excluded.PD_TTC,
                         PD_PIT = excluded.PD_PIT,
                         LGD = excluded.LGD,
                         MDY_PD = excluded.MDY_PD,
                         SP_PD = excluded.SP_PD,
                         PD = excluded.PD,
                         cond_loss = excluded.cond_loss,
                         expected_loss = excluded.expected_loss,
                         unexpected_loss = excluded.unexpected_loss,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > rating_matrix.time_stamp 
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row) for row in
                          df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-----------------------------------------------------------------


def import_to_stress_scenarios_for_risk_free_rate(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'stress_scenarios_for_risk_free_rate'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'scenario_name', 'currency', 'tenor', 'rate_start_pp',
            'rate_end_pp', 'rate_change_pp', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO stress_scenarios_for_risk_free_rate (
                         scenario_name, currency, tenor, rate_start_pp,
                         rate_end_pp, rate_change_pp, time_stamp
                     ) 
                     VALUES (?, ?, ?, ?, ?, ?,?) 
                     ON CONFLICT(scenario_name, currency, tenor) DO UPDATE SET
                         rate_start_pp = excluded.rate_start_pp,
                         rate_end_pp = excluded.rate_end_pp,
                         rate_change_pp = excluded.rate_change_pp,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > stress_scenarios_for_risk_free_rate.time_stamp 
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row)
                         for row in df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#----------------------------------------------------------------------------------------------------


def import_to_stress_scenarios_for_credit_spread(excel_file_path, file_sheet_name, db_file_path):
    """Import Excel file to SQLite database table 'stress_scenarios_for_credit_spread'"""

    # Read Excel file into DataFrame
    try:
        df = pd.read_excel(excel_file_path, sheet_name=file_sheet_name)
        print(f"Successfully read Excel file with {len(df)} records.")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'scenario_name', 'rating_category_sp', 'range_start_days', 'range_end_days',
            'spread_start_bp', 'spread_end_bp','spread_change_bp', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in Excel file")

    except Exception as e:
        print(f"Error transforming data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO stress_scenarios_for_credit_spread (
                         scenario_name, rating_category_sp, range_start_days, 
                         range_end_days, spread_start_bp, spread_end_bp,
                         spread_change_bp, time_stamp
                     ) 
                     VALUES (?, ?, ?, ?, ?, ?,?,?) 
                     ON CONFLICT(scenario_name, rating_category_sp, range_start_days, range_end_days) DO UPDATE SET
                         spread_change_bp = excluded.spread_change_bp,
                         spread_start_bp = excluded.spread_start_bp,
                         spread_end_bp = excluded.spread_end_bp,                         
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > stress_scenarios_for_credit_spread.time_stamp 
                     """

        # Convert DataFrame to a list of tuples and handle NULL values
        data_to_insert = [tuple(str(x) if pd.notna(x) else None for x in row)
                         for row in df[required_columns].to_numpy()]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#-------------------------------------------------------------------


def import_bond_prices_df(df: pd.DataFrame, db_file_path: str):
    """Import DataFrame 'bond_prices' to SQLite table 'bond_price' with UPSERT"""

    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        conn.execute("PRAGMA synchronous = FULL")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'RepDate', 'isin', 'pricing_source', 'ytm_bid',
            'price_bid', 'price_last', 'oas_spread', 'z_spread',
            'factor_principal', 'time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in DataFrame")

    except Exception as e:
        print(f"Error validating data: {e}")
        conn.close()
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
                     INSERT INTO bond_price (
                         RepDate, isin, pricing_source, ytm_bid, 
                         price_bid, price_last, oas_spread, z_spread, factor_principal, time_stamp
                     ) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                     ON CONFLICT(RepDate, isin) DO UPDATE SET
                         pricing_source = excluded.pricing_source,
                         ytm_bid = excluded.ytm_bid,
                         price_bid = excluded.price_bid,
                         price_last = excluded.price_last,
                         oas_spread = excluded.oas_spread,
                         z_spread = excluded.z_spread,
                         factor_principal = excluded.factor_principal,
                         time_stamp = excluded.time_stamp
                     WHERE excluded.time_stamp > bond_price.time_stamp 
                     """

        # Convert DataFrame to list of tuples (NaN → None)
        data_to_insert = [
            tuple(x if pd.notna(x) else None for x in row)
            for row in df[required_columns].to_numpy()
        ]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#------------------------------------------------------------------
def import_yield_curves_df(df: pd.DataFrame, db_file_path: str):
    """Import DataFrame 'yield_curves' to SQLite table 'yield_curves' with UPSERT"""

    # Prepare the DataFrame for insertion
    try:
        required_columns = [
            'RepDate','currency','component_id','index_id','tenor',
            'rate_percent','time_stamp'
        ]

        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Required column '{col}' not found in DataFrame")

    except Exception as e:
        print(f"Error validating data: {e}")
        return
    
    # Connect to SQLite database
    try:
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = FULL")
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Perform UPSERT operation
    try:
        insert_sql = """
            INSERT INTO yield_curves (
                RepDate, currency, component_id, index_id, tenor, rate_percent, time_stamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?) 
            ON CONFLICT(RepDate, currency, tenor) DO UPDATE SET
                component_id = excluded.component_id,
                index_id = excluded.index_id,
                rate_percent = excluded.rate_percent,
                time_stamp = excluded.time_stamp
            WHERE excluded.time_stamp > yield_curves.time_stamp
        """

        # Convert DataFrame to list of tuples (NaN → None)
        data_to_insert = [
            tuple(x if pd.notna(x) else None for x in row)
            for row in df[required_columns].to_numpy()
        ]

        # Execute the UPSERT
        cursor.executemany(insert_sql, data_to_insert)
        print(f"Processed {cursor.rowcount} records (inserted or updated)")

        conn.commit()

    except sqlite3.IntegrityError as e:
        print(f"Integrity error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Error during database operations: {e}")
        conn.rollback()
    finally:
        conn.close()
#----------------------------------------------


