from flask import Flask, render_template, request, redirect, url_for, flash
import mysql.connector
import datetime

app = Flask(__name__)
app.secret_key = "secret123"  # needed for flash messages

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="etl"
    )

@app.route("/")
def form():
    return render_template("form.html")

@app.route("/submit", methods=["POST"])
def submit():
    try:
        # Read form input
        position_id = request.form["position_id"]
        emp_id = request.form["emp_id"]
        emp_name = request.form["emp_name"]
        email = request.form["email"]
        employment_status = request.form["employment_status"]
        effective_start_date = request.form["effective_start_date"]
        effective_status = request.form["effective_status"]
        date_of_joining = request.form["date_of_joining"]

        # Compute experience
        doj = datetime.datetime.strptime(date_of_joining, "%Y-%m-%d")
        current_date = datetime.datetime.now()
        years_with_company = (
            current_date.year - doj.year -
            ((current_date.month, current_date.day) < (doj.month, doj.day))
        )

        conn = get_connection()
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO etl.processed_position_data (
                snapshot_date, position_id, emp_id, emp_name, email,
                employment_status, effective_start_date, effective_status,
                date_of_joining, years_with_company
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(insert_query, (
            datetime.datetime.now(), position_id, emp_id, emp_name, email,
            employment_status, effective_start_date, effective_status,
            date_of_joining, years_with_company
        ))

        conn.commit()
        cursor.close()
        conn.close()

        flash("Employee record inserted successfully!", "success")
        return redirect(url_for("form"))

    except Exception as e:
        flash(f"Error: {str(e)}", "danger")
        return redirect(url_for("form"))

if __name__ == "__main__":
    app.run(debug=True)
