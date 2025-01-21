import os
from flask import Flask, jsonify, render_template, request
import json

# import custom functions
from models import *
from csv_to_db import *
from util import *


app = Flask(__name__, template_folder='templates')

@app.route("/")
def index():
    return render_template('index.html')


@app.route("/departments", methods=['POST'])
def get_departments():
    data = request.get_json()
    # print("data: ", data)
    file_name = data.get('file_name')
    chunk_size = data.get('chunk_size')
    table_name = data.get('table_name')
    # print(file_name, chunk_size, table_name)

    # assume drop table before import
    # Department.__table__.drop(engine)

    result_log = process_valid_invalid_results(file_name, chunk_size, table_name)
    response = {
        "message": "Departments endpoint",
        "error": "",
        "result_log": result_log
    }
    return jsonify(response), 201


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





