# ETL Flask Application with MySQL

## Overview
This project demonstrates how to build a complete **ETL (Extract, Transform, Load)** workflow using **Python, Flask, and MySQL**.  
It also includes a **web-based UI** to insert employee data into a staging table, which is later processed and transformed into a final table using a standalone ETL script.

---

##System Architecture

Flask Web UI → employee_staging → ETL Script → processed_position_data


---

##Folder Structure


ETL-Project/
│
├── app.py # Flask UI application
├── etl_script.py # ETL Python script
│
├── templates/
│ ├── home.html
│ ├── insert.html
│ ├── success.html
│ └── staging_data.html
│
└── static/ # optional for CSS/JS/images


---

## 🗄 Database Schema

### 1. **Staging Table**
```sql
CREATE TABLE employee_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    emp_id VARCHAR(20),
    emp_name VARCHAR(50),
    emp_dept VARCHAR(50),
    emp_salary DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

2. Processed Table
CREATE TABLE processed_position_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    emp_id VARCHAR(20),
    emp_name VARCHAR(50),
    emp_dept VARCHAR(50),
    emp_salary DECIMAL(10,2),
    processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

Setup Instructions
Prerequisites
| Dependency | Version |
| ---------- | ------- |
| Python     | 3.7+    |
| MySQL      | 5.7+    |
| pip        | Latest  |

1. Install Python Dependencies
pip install flask mysql-connector-python

2. Update Database Credentials
Inside app.py and etl_script.py, modify:
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'yourpassword',
    'database': 'etl'
}

Running the Application
Start Flask UI
python app.py

Open browser:
http://127.0.0.1:5000/

ETL Workflow Summary
| Step      | Description                                       |
| --------- | ------------------------------------------------- |
| Extract   | Fetch raw data from `employee_staging`            |
| Transform | Data cleaning (trim, validate, cast salary)       |
| Load      | Insert into `processed_position_data`             |
| Audit     | Print summary (status, rows inserted, time taken) |

Logging & Error Handling
Uses try/except with rollback
rovides detailed console logs
Tracks total processed rows & execution duration

Future Enhancements
| Feature   | Description                 |
| --------- | --------------------------- |
| Scheduler | Use CRON / Apache Airflow   |
| Docker    | Containerize for deployment |
| API Layer | Convert to REST via FastAPI |
| Security  | Add login + form validation |
| Reporting | Dashboard using Chart.js    |

Author
Nagaraja Banakara
Data Engineer & ETL Developer

License
This project is open-source under the MIT License.