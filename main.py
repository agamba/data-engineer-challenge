import os
from flask import Flask, jsonify, render_template, request
import json
import traceback

# import custom functions
from models import *
from csv_to_db import *


app = Flask(__name__, template_folder='templates')

@app.route("/")
def index():
    return render_template('index.html')


@app.route("/departments", methods=['POST'])
def get_departments():

    data = request.get_json()
    file_name_q = data.get('file_name')
    chunk_size = data.get('chunk_size')
    table_name = data.get('table_name')
    # print(file_name_q, chunk_size, table_name)

    result_logs, file_path = process_valid_invalid_results(file_name_q, chunk_size, table_name)
    response = {
        "parameters": data,
        "message": "Departments endpoint",
        "result_logs": result_logs,
        "logs_file_path": file_path
    }
    return jsonify(response), 201


@app.route("/jobs", methods=['POST'])
def get_jobs():
    data = request.get_json()
    # print("data: ", data)
    file_name = data.get('file_name')
    chunk_size = data.get('chunk_size')
    table_name = data.get('table_name')
    # print(file_name, chunk_size, table_name)

    result_logs, file_path = process_valid_invalid_results(file_name, chunk_size, table_name)
    response = {
        "parameters": data,
        "message": "Jobes endpoint",
        "error": "",
        "result_logs": result_logs,
        "logs_file_path": file_path
    }
    return jsonify(response), 201

@app.route("/hired_employees", methods=['POST'])
def get_hired_employees():
    data = request.get_json()
    # print("data: ", data)
    file_name = data.get('file_name')
    chunk_size = data.get('chunk_size')
    table_name = data.get('table_name')
    # print(file_name, chunk_size, table_name)

    result_logs, file_path = process_valid_invalid_results(file_name, chunk_size, table_name)
    response = {
        "parameters": data,
        "message": "Hired Employees endpoint",
        "error": "",
        "result_logs": result_logs,
        "logs_file_path": file_path
    }
    return jsonify(response), 201


@app.route("/backups/restore", methods=['POST'])
def restore_backups():
    response = {
        "message": "Restore Backups endpoint",
        "error": ""
    }
    return jsonify(response)

@app.route("/backups/create", methods=['POST'])
def create_backups():
    response = {
        "message": "Create Backups endpoint",
        "error": ""
    }
    return jsonify(response)

if __name__ == '__main__':
    #app.run(debug=True)
    app.run(host='0.0.0.0', port=8080)





