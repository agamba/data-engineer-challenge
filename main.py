import os
from flask import Flask, jsonify, render_template, request
import json
import traceback

# import custom functions
from models import *
from csv_to_db import *


app = Flask(__name__, template_folder='templates')

# Set the upload folder path (adjust as needed)
UPLOAD_FOLDER = 'uploads'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Ensure the upload folder exists
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/dashboard")
def dashboard():
    return render_template('dashboard.html')

def index():
    return render_template('index.html')

# Adjust code to recieved data also from web page demo
# e.g. curl -X POST -F "file=@data/departments.csv" -F "table_name=departments" -F "chunk_size=1000" http://127.0.0.1:8080/import
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

        print()
        print("table_name: ", table_name)
        print("chunk_size: ", chunk_size)
        print("file: ", file.filename)

        # TODO: save submitted contents to a file
        if file:
            filename = file.filename.replace(" ", "_")
            filename_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filename_path)
            print(f'File uploaded successfully at: {filename_path}')

            # return "File uploaded successfully.", 201

            try:
                chunk_size = int(chunk_size)  # Convert chunk_size to integer
                if chunk_size > 1000:
                    return "Chunk size must less or equal to 1000."
            except ValueError:
                return "Invalid chunk size. Please enter a number."

            print(f"PARAMS:table_name: {table_name}, Chunk size: {chunk_size}")

        # Process data and get logs
        logs_result, file_path = process_valid_invalid_results(filename_path, chunk_size, table_name)
        response = {
                "table_name": table_name,
                "chunk_size": chunk_size,
                "filename_path":filename_path,
                "message": "Data processed successfully.",
                "logs_result": logs_result,
                "logs_file_path": file_path
        }
        print(response)
        return "data processed successfully\n\n", 201
        # return jsonify(response), 201
    else:
        # render the import page
        return render_template('import.html')


@app.route("/backups", methods=['GET', 'POST'])
def manage_backups():
    if request.method == 'POST':
        # TODO: Implement the logic to create/restore backups

        
        backup_file_name = "backup_file_name"

        # result = create_backup(Department)
        # result = restore_backup(backup_file_name, Department)
        # print(result)
        return "Backup created/restored successfully\n\n", 201

        response = {
            "message": "Create/Restore Backups endpoint",
            "error": ""
        }
        return jsonify(response)
    else:
        # get a list of existing backups
        backups = ["backup1", "backup2", "backup3", "backup4"]

        return render_template('backups.html' , backups=backups)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)



