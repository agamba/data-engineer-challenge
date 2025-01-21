import os
from flask import Flask, jsonify, render_template

app = Flask(__name__, template_folder='templates')

@app.route("/")
def index():
    return render_template('index.html')


@app.route("/departments")
def get_departments():
    response = {
        "message": "Departments endpoint",
        "error": ""
    }
    return jsonify(response)


@app.route("/jobs")
def get_jobs():
    response = {
        "message": "Jobs endpoint",
        "error": ""
    }
    return jsonify(response)

@app.route("/hired_employees")
def get_hired_employees():
    response = {
        "message": "Hired Employees endpoint",
        "error": ""
    }
    return jsonify(response)


@app.route("/backups/restore")
def restore_backups():
    response = {
        "message": "Restore Backups endpoint",
        "error": ""
    }
    return jsonify(response)

@app.route("/backups/create")
def create_backups():
    response = {
        "message": "Create Backups endpoint",
        "error": ""
    }
    return jsonify(response)

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=8080)





