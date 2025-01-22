import os
from flask import Flask, jsonify, render_template, request
import json
import traceback

# import custom functions
from models import *
from csv_to_db import *


app = Flask(__name__, template_folder='templates')

# Adjust code to recieved data also from web page demo
# e.g. curl -X POST -F "file=@data/departments.csv" -F "table_name=departments" -F "chunk_size=1000" http://127.0.0.1:8080/

# move to a simple version with a single route for all import functions

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/import", methods=['GET', 'POST'])
def get_import():
    if request.method == 'POST':
        # process request 
        if 'file' not in request.files:
            return jsonify({"error": "No file part in the request"}), 400

        file = request.files['file']
        table_name  = request.form.get('table_name')  # Get table name        
        chunk_size = request.form.get('chunk_size')  # Get chunk size

        # validate data
        if file.filename == '' or not table_name or not chunk_size: 
            # return redirect(request.url)
            return "Invalid data.\n\n", 400

        # TODO: save submitted contents to a file

        # get log results and file path to json log
        logs_result, file_path = process_valid_invalid_results(file_name, chunk_size, table_name)
        response = {
                "parameters": "",
                "message": "",
                "logs_result": logs_result,
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

# NOTE: use only one end point for create and restore backups: simpler approch
@app.route("/backups", methods=['POST'])
def manage_backups():
    response = {
        "message": "Create/Restore Backups endpoint",
        "error": ""
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)





