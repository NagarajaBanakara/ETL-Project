import logging
import mysql.connector
import pyodbc
import pandas as pd
from datetime import datetime

# Configure Logging
logging.basicConfig(filename='etl_process.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Database Configurations
hana_conn_string = 'DRIVER={HDBODBC};SERVERNODE=your_hana_server:port;UID=user;PWD=password'

mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'etl'
}

# ETL tracking dictionary
etl_log = {
    'start_time': datetime.now(),
    'records_inserted': 0,
    'records_deleted': 0,
    'error_logs': []
}

def extract_data():
    try:
        hana_conn = pyodbc.connect(hana_conn_string)
        query = """
            SELECT 
                snapshot_date,
                position_id,
                first_name,
                last_name,
                effective_start_date,
                effective_status,
                emp_id,
                email,
                employment_status,
                date_of_joining
            FROM 
                Position_Snapshot_Data
            WHERE 
                snapshot_date >= DATEADD(month, -6, CURRENT_DATE)
                AND position_id IS NOT NULL
        """
        df = pd.read_sql(query, hana_conn)
        hana_conn.close()
        logging.info("Data extraction successful.")
        return df
    except Exception as e:
        logging.error("Extraction failed: " + str(e))
        etl_log['error_logs'].append(str(e))
        raise

def transform_data(df):
    try:
        df['emp_name'] = df['first_name'] + ' ' + df['last_name']
        current_date = datetime.now()

        df['years_with_company'] = df['date_of_joining'].apply(
            lambda x: current_date.year - x.year - ((current_date.month, current_date.day) < (x.month, x.day))
        )

        df.drop(['first_name', 'last_name'], axis=1, inplace=True)

        logging.info("Data transformation successful.")
        return df
    except Exception as e:
        logging.error("Transformation failed: " + str(e))
        etl_log['error_logs'].append(str(e))
        raise

def validate_data(df):
    try:
        critical_fields = ['emp_id', 'emp_name', 'position_id', 'date_of_joining']
        if df[critical_fields].isnull().any().any():
            raise ValueError("Null found in critical fields.")

        if not df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$').all():
            raise ValueError("Invalid email format.")

        if (df['years_with_company'] < 0).any():
            raise ValueError("Invalid years_with_company calculation.")

        logging.info("Data validation successful.")
    except Exception as e:
        logging.error("Validation failed: " + str(e))
        etl_log['error_logs'].append(str(e))
        raise

def load_data(df):
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO processed_position_data 
            (snapshot_date, position_id, emp_id, emp_name, email, employment_status, 
             effective_start_date, effective_status, date_of_joining, years_with_company)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        cursor.executemany(insert_query, df.to_records(index=False))
        conn.commit()

        etl_log['records_inserted'] = len(df)
        logging.info(f"Inserted {len(df)} records into MySQL.")

        conn.close()
    except Exception as e:
        logging.error("Loading failed: " + str(e))
        etl_log['error_logs'].append(str(e))
        raise

def write_audit_log():
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        duration = (datetime.now() - etl_log['start_time']).seconds
        status = "FAILED" if etl_log['error_logs'] else "SUCCESS"

        cursor.execute("""
            INSERT INTO etl_audit_log
            (start_time, end_time, duration_sec, records_inserted, records_deleted, status, error_logs)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            etl_log['start_time'],
            datetime.now(),
            duration,
            etl_log['records_inserted'],
            etl_log['records_deleted'],
            status,
            '\n'.join(etl_log['error_logs']) if etl_log['error_logs'] else None
        ))

        conn.commit()
        conn.close()

    except Exception as e:
        logging.error("Audit logging failed: " + str(e))

def main_etl():
    try:
        df = extract_data()
        df = transform_data(df)
        validate_data(df)
        load_data(df)
    except:
        pass
    finally:
        write_audit_log()
        logging.info("ETL Process Completed")

if __name__ == "__main__":
    main_etl()
