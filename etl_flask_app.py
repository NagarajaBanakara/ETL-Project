from flask import Flask, render_template, request, redirect, send_file, flash
import mysql.connector
import datetime
import pandas as pd
import traceback
import os

app = Flask(__name__)
app.secret_key = "secretkey123"

# =========================
# DB CONNECTION
# =========================
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="etl"
    )

# =========================
# HOME PAGE
# =========================
@app.route("/")
def home():
    return render_template("home.html")

# =========================
# ADD EMPLOYEE
# =========================
@app.route("/add", methods=["GET", "POST"])
def add_employee():
    if request.method == "POST":
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            query = """
                INSERT INTO processed_position_data (
                    snapshot_date, position_id, emp_id, emp_name, email,
                    employment_status, effective_start_date, effective_status,
                    date_of_joining, years_with_company
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """

            data = (
                datetime.datetime.now(),
                request.form["position_id"],
                request.form["emp_id"],
                request.form["emp_name"],
                request.form["email"],
                request.form["employment_status"],
                request.form["effective_start_date"],
                request.form["effective_status"],
                request.form["date_of_joining"],
                request.form["years_with_company"]
            )

            cursor.execute(query, data)
            conn.commit()
            cursor.close()
            conn.close()

            flash("Employee Added Successfully!", "success")
            return redirect("/employees")

        except Exception as e:
            flash(str(e), "danger")

    return render_template("add_employee.html")


# =========================
# VIEW EMPLOYEES
# =========================
@app.route("/employees")
def view_employees():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM processed_position_data")
    employees = cursor.fetchall()
    cursor.close()
    conn.close()
    return render_template("view_employees.html", employees=employees)


# =========================
# EDIT EMPLOYEE
# =========================
@app.route("/edit/<emp_id>", methods=["GET", "POST"])
def edit_employee(emp_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if request.method == "POST":
        update_query = """
            UPDATE processed_position_data
            SET position_id=%s, emp_name=%s, email=%s, employment_status=%s,
                effective_start_date=%s, effective_status=%s, date_of_joining=%s,
                years_with_company=%s
            WHERE emp_id=%s
        """

        cursor.execute(update_query, (
            request.form["position_id"],
            request.form["emp_name"],
            request.form["email"],
            request.form["employment_status"],
            request.form["effective_start_date"],
            request.form["effective_status"],
            request.form["date_of_joining"],
            request.form["years_with_company"],
            emp_id
        ))

        conn.commit()
        cursor.close()
        conn.close()

        flash("Employee Updated Successfully!", "success")
        return redirect("/employees")

    cursor.execute("SELECT * FROM processed_position_data WHERE emp_id=%s", (emp_id,))
    emp = cursor.fetchone()
    cursor.close()
    conn.close()

    return render_template("edit_employee.html", emp=emp)


# =========================
# DELETE EMPLOYEE
# =========================
@app.route("/delete/<emp_id>")
def delete_employee(emp_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM processed_position_data WHERE emp_id=%s", (emp_id,))
    conn.commit()
    cursor.close()
    conn.close()
    flash("Employee Deleted!", "warning")
    return redirect("/employees")


# =========================
# EXPORT TO CSV
# =========================
@app.route("/export")
def export_csv():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM processed_position_data", conn)
    conn.close()

    file_name = "employees_export.csv"
    df.to_csv(file_name, index=False)
    return send_file(file_name, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)
