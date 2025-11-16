import datetime
import pandas as pd
import mysql.connector
import traceback

# =========================================
#  MySQL Database Connection
# =========================================
def get_mysql_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="etl"
    )


start_time = datetime.datetime.now()
records_inserted = 0
records_deleted = 0
status = "SUCCESS"
error_message = ""

conn = None
cursor = None

try:
    # Establish DB connection
    conn = get_mysql_connection()
    cursor = conn.cursor()

    # =========================================
    # STEP 1: CREATE MOCK SOURCE DATA
    # =========================================
    source_data = [
        {
            "snapshot_date": datetime.datetime.now(),
            "position_id": "POS001",
            "emp_id": "EMP1001",
            "first_name": "Niya",
            "last_name": "Sharma",
            "email": "niya.sharma@example.com",
            "employment_status": "Active",
            "effective_start_date": datetime.datetime.now(),
            "effective_status": "Active",
            "date_of_joining": datetime.date(2020, 5, 15)
        },
        {
            "snapshot_date": datetime.datetime.now(),
            "position_id": "POS002",
            "emp_id": "EMP1002",
            "first_name": "Raj",
            "last_name": "Kumar",
            "email": "raj.kumar@example.com",
            "employment_status": "Active",
            "effective_start_date": datetime.datetime.now(),
            "effective_status": "Active",
            "date_of_joining": datetime.date(2019, 3, 20)
        }
    ]

    df = pd.DataFrame(source_data)

    # =========================================
    # STEP 2: TRANSFORMATION
    # =========================================
    df["emp_name"] = df["first_name"] + " " + df["last_name"]
    current_date = datetime.datetime.now()

    df["years_with_company"] = df["date_of_joining"].apply(
        lambda doj: current_date.year - doj.year -
        ((current_date.month, current_date.day) < (doj.month, doj.day))
    )

    df = df.drop(["first_name", "last_name"], axis=1)

    # =========================================
    # STEP 3: INSERT DATA INTO TARGET TABLE
    # =========================================

    insert_query = """
        INSERT INTO etl.processed_position_data (
            snapshot_date, position_id, emp_id, emp_name, email, employment_status,
            effective_start_date, effective_status, date_of_joining, years_with_company
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["snapshot_date"],
            row["position_id"],
            row["emp_id"],
            row["emp_name"],
            row["email"],
            row["employment_status"],
            row["effective_start_date"],
            row["effective_status"],
            row["date_of_joining"],
            row["years_with_company"]
        ))

    conn.commit()
    records_inserted = len(df)

except Exception as e:
    status = "FAILED"
    error_message = traceback.format_exc()

finally:
    end_time = datetime.datetime.now()
    duration_sec = int((end_time - start_time).total_seconds())

    # Only insert audit if DB insert attempt was made
    if conn is not None and cursor is not None:
        audit_query = """
            INSERT INTO etl.etl_audit_log (
                start_time, end_time, duration_sec, records_inserted, records_deleted, status, error_logs
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(audit_query, (
            start_time,
            end_time,
            duration_sec,
            records_inserted,
            records_deleted,
            status,
            error_message
        ))
        conn.commit()
        cursor.close()
        conn.close()

print("\n===== ETL EXECUTION RESULT =====")
print(f"Status: {status}")
print(f"Rows Inserted: {records_inserted}")
print("--------------------------------")

if status == "FAILED":
    print("ERROR DETAILS:")
    print(error_message)
