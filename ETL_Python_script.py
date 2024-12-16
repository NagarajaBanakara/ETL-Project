import logging
import smtplib
import requests
from datetime import datetime
import pyodbc
import pandas as pd

# Configure Logging
logging.basicConfig(filename='etl_process.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# ETL Dictionary
etl_log = {
    'start_time': datetime.now(),
    'end_time': None,
    'records_inserted': 0,
    'records_deleted': 0,
    'error_logs': []
}

def send_email(subject, body, to_addresses):
    # Email sending logic
    pass


def extract_data():
    try:
        # Connect to HANA
        hana_conn = pyodbc.connect('DRIVER={HDBODBC};SERVERNODE=your_hana_server:port;UID=user;PWD=password')
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
        logging.error(f"Extraction failed: {e}")
        etl_log['error_logs'].append(str(e))
        raise

def transform_data(df):
    try:
        # Concatenate first and last names
        df['emp_name'] = df['first_name'] + ' ' + df['last_name']
        
        # Calculate years_with_company
        current_date = pd.to_datetime(datetime.now())
        df['years_with_company'] = df['date_of_joining'].apply(lambda x: current_date.year - x.year - ((current_date.month, current_date.day) < (x.month, x.day)))
        
        # Drop unnecessary columns
        df = df.drop(['first_name', 'last_name'], axis=1)
        
        logging.info("Data transformation successful.")
        return df
    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        etl_log['error_logs'].append(str(e))
        raise

def validate_data(df):
    try:
        # Check for nulls in critical fields
        critical_fields = ['emp_id', 'emp_name', 'position_id', 'date_of_joining']
        if df[critical_fields].isnull().any().any():
            raise ValueError("Null values found in critical fields.")
        
        # Validate email format
        if not df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$').all():
            raise ValueError("Invalid email formats detected.")
        
        # Validate years_with_company
        if (df['years_with_company'] < 0).any():
            raise ValueError("Negative values found in years_with_company.")
        
        logging.info("Data validation successful.")
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        etl_log['error_logs'].append(str(e))
        raise

def load_data(df):
    try:
        # Connect to SQL Server
        sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_sql_server;DATABASE=your_db;UID=user;PWD=password')
        cursor = sql_conn.cursor()
        
        # Insert Data
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO Processed_Position_Data 
                (snapshot_date, position_id, emp_id, emp_name, email, employment_status, effective_start_date, effective_status, date_of_joining, years_with_company)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, 
            row['snapshot_date'], row['position_id'], row['emp_id'], row['emp_name'], row['email'],
            row['employment_status'], row['effective_start_date'], row['effective_status'], 
            row['date_of_joining'], row['years_with_company'])
        
        sql_conn.commit()
        records_inserted = len(df)
        etl_log['records_inserted'] += records_inserted
        logging.info(f"Data loading successful. Records inserted: {records_inserted}")
        sql_conn.close()
    except Exception as e:
        logging.error(f"Loading failed: {e}")
        etl_log['error_logs'].append(str(e))
        raise

def main_etl():
    try:
        logging.info("ETL process started.")
        df = extract_data()
        df = transform_data(df)
        validate_data(df)
        load_data(df)
    except Exception as e:
        logging.error("ETL process encountered errors.")
    finally:
        etl_log['end_time'] = datetime.now()
        # Send Notifications
        if etl_log['error_logs']:
            subject = "ETL Process Failed"
            body = "\n".join(etl_log['error_logs'])
            send_email(subject, body, ['stakeholder1@yourcompany.com'])
            
        else:
            subject = "ETL Process Completed Successfully"
            body = f"Records Inserted: {etl_log['records_inserted']}\nRecords Deleted: {etl_log['records_deleted']}"
            send_email(subject, body, ['stakeholder1@yourcompany.com'])
        logging.info("ETL process completed.")

if __name__ == "__main__":
    main_etl()
