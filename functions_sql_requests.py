import sqlite3
import pandas as pd

def check_available_dates (tables_to_check, valuation_date, db_path):
    '''check if data is available for the given date and return a log of the results'''
    check_log = []

    try:
        with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=10) as connection:
            cursor = connection.cursor()
            for table in tables_to_check:
                message = f'*** {table} ***'
                check_log.append(message)

                cursor.execute(f'SELECT 1 FROM {table} WHERE RepDate = ?', (valuation_date,))
                if cursor.fetchone():
                    message = 'data exists'
                    check_log.append(message)
                else:
                    cursor.execute(f'''SELECT RepDate FROM {table}
                                                          WHERE RepDate <= ?
                                                          ORDER BY RepDate DESC LIMIT 1''',
                                   (valuation_date,))
                    available_date = cursor.fetchone()

                    if available_date:
                        message = ['no date match', f'latest available date is: {available_date[0]}']
                        check_log.extend(message)
                    else:
                        message = f'no historical data before {valuation_date}'
                        check_log.append(message)

    except sqlite3.Error as e:
        check_log.append(f'Database error: {str(e)}')
    except Exception as e:
        check_log.append('error')
    return check_log
#----------------------------------------------------------------------------------------------------------------------


def check_dic_bonds_data(valuation_date, db_path):
    '''check if bond static data is available for the given date and return a log of the results'''
    check_log = []

    try:
        with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=10) as connection:
            cursor = connection.cursor()

            # check for missing isins in dic_bonds
            check_log.append('*** bonds dictionary ***')
            cursor.execute('''
                           SELECT DISTINCT isin 
                           FROM positions
                           WHERE isin NOT IN (SELECT isin FROM dic_bonds)
                             AND RepDate = (SELECT RepDate FROM positions
                                    WHERE RepDate <= ?
                                    ORDER BY RepDate DESC LIMIT 1)
                           ''', (valuation_date,))
            result = cursor.fetchall()
            if result:
                for record in result:
                    check_log.append(f'missing isin: {record[0]}')
            else:
                check_log.append('all isins are present in dic_bonds')

    except sqlite3.Error as e:
        check_log.append(f'Database error: {str(e)}')
    except Exception as e:
        message = 'error'
        check_log.append(message)
    return check_log
#----------------------------------------------------------------------------------------------------------------------

def check_dic_bond_cf_data(valuation_date, db_path):
    '''check if bond cash flow data is available for bonds in portfolio at a given date and return a log of the results '''
    check_log = []

    try:
        with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=10) as connection:
            cursor = connection.cursor()

            # check for missing isins in dic_bond_cf
            check_log.append('*** bonds cash-flow dictionary ***')
            cursor.execute('''
                            SELECT DISTINCT isin
                            FROM positions
                            WHERE isin NOT IN (SELECT isin FROM dic_bond_cf)
                                AND RepDate = (SELECT RepDate FROM positions
                                    WHERE RepDate <= ?
                                    ORDER BY RepDate DESC LIMIT 1) 
                            AND isin NOT IN (SELECT isin FROM dic_bonds
                                             WHERE class_internal = 'bad debt')                                    
                            ''', (valuation_date,))
            result = cursor.fetchall()
            if result:
                for record in result:
                    check_log.append(f'missing isin: {record[0]}')
            else:
                check_log.append('all isins are present in dic_bond_cf')

    except sqlite3.Error as e:
        check_log.append(f'Database error: {str(e)}')
    except Exception as e:
        message = 'error'
        check_log.append(message)
    return check_log
#----------------------------------------------------------------------------------------------------------------------


def check_bond_par_value(valuation_date, db_path):
    """ compares par_value in 'dic_bonds' with on shown in position report
    valuation_date = '2025-07-09'
    db_path = 'arms_database.db'
    """
    table_name = 'positions'
    output_file_name = 'check_bond_par_value'
    output_file_path = rf"P:\Application\Risk Mgmt\MRM\ARMS\temporary\{output_file_name} {valuation_date}.xlsx"
    check_log = []

    try:
        with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=10) as connection:
            connection = sqlite3.connect(db_path)
            cursor = connection.cursor()
            QUERY = f"""SELECT pos.isin, pos.issuer, SUM(pos.quantity) as quantity, SUM(pos.total_notional) as notional, 
                               SUM(pos.invested_amount) as invested_amount, dic.par_value as par_value_arms 
                FROM positions AS pos
                LEFT JOIN (SELECT isin, par_value, class_internal 
                           FROM dic_bonds ) AS dic
                           ON pos.isin = dic.isin                 
                WHERE pos.RepDate = '{valuation_date}' AND dic.class_internal <> 'bad debt'  
                GROUP BY pos.isin"""
            request_data = cursor.execute(QUERY).fetchall()
            column_names = [description[0] for description in cursor.description]
            request_data = pd.DataFrame (request_data, columns=column_names)
            
        # NOMINAL in new portfolio = par value per bond (e.g. 100 or 1000), not total notional
        request_data['par_value_dwh'] = request_data['notional'] / request_data['quantity']
        request_data['diff'] = request_data['par_value_arms'] - request_data['par_value_dwh']        
        request_data['investment_per_bond'] = request_data['invested_amount'] / request_data['quantity']
        filtered_data = request_data[request_data['diff'] != 0]

        #with pd.ExcelWriter (output_file_path, engine = 'openpyxl') as writer:
            #filtered_data.to_excel(writer, sheet_name='Output', index=False)
        #print (f"file {output_file_name} was saved")

        return filtered_data

    except sqlite3.Error as e:
        check_log.append(f'Database error: {str(e)}')
    except Exception as e:
        message = 'error'
        check_log.append(message)
    return check_log

    
#----------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    db_path = 'arms_database.db'
    valuation_date = '2025-07-10'

    tables_to_check = [
        'positions',
        'dbOAS_Global',
        'dbOAS_EM',
        'bond_price',
        'cbar_fx_rates',
        'bond_price']
    check_log = check_available_dates(tables_to_check, valuation_date, db_path)
    check_log = pd.DataFrame({'message':check_log})
    print(check_log)

    check_log = check_dic_bonds_data(valuation_date, db_path)
    check_log = pd.DataFrame({'message':check_log})
    print(check_log)

    check_log = check_dic_bond_cf_data(valuation_date, db_path)
    check_log = pd.DataFrame({'message':check_log})
    print(check_log)

    