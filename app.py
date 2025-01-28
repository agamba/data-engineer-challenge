import os
from flask import Flask, jsonify, render_template, request, send_from_directory, abort
import json
import traceback
import mimetypes

from config import RESULT_FOLDER, UPLOAD_FOLDER
from models import initialize_db
from csv_to_db import process_valid_invalid_results, get_table_counts, get_import_logs
from backups import create_backup, restore_backup, get_backup_files

from req001 import process_requirement1
from req002 import process_requirement2

app = Flask(__name__, template_folder='templates')

# Initialize the database
initialize_db()

# TODO: securuty considerations/options (not implemented yet)
# use simple API key 
# use environment variable for API key
# use authentication mechansim in cloud enironment

# HOME
@app.route("/")
def index():
    return render_template('index.html')


# DASHBOARD
@app.route("/dashboard")
def dashboard():
    # Generate reports for each requirement and pass to the template

    results1 = process_requirement1(year=2021)
    results2 = process_requirement2(year=2021)
    
    # TODO: Further consideration needed for rendering result data in the template

    # Format results for display in template
    req1_dict = {
        "report_name": results1["report_name"],
        "html": results1["html"],
        "csv": results1["csv"],
        "image1": results1["images"].split(",")[0],
        "image2": results1["images"].split(",")[1],
    }

    req2_dict = {
        "report_name": results2["report_name"],
        "html": results2["html"],
        "csv": results2["csv"],
        "image1": results2["images"], # Assuming there is only one image for req2
    }
    table_counts = get_table_counts()

    return render_template('dashboard.html', table_counts=table_counts, results1=req1_dict, results2=req2_dict)


# IMPORT CSV DATA
# Adjusted to recieved data both from  web page demo and CURL
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

        print("#######################")
        print("Processing Import Request:")
        print("table_name: ", table_name)
        print("chunk_size: ", chunk_size)
        print("file: ", file.filename)

        # save submitted contents to a file
        if file:
            filename = file.filename.replace(" ", "_")
            imported_file = os.path.join(UPLOAD_FOLDER, filename)
            file.save(imported_file)
            print(f'File uploaded successfully at: {imported_file}')

            try:
                chunk_size = int(chunk_size)  # Convert chunk_size to integer
                if chunk_size > 1000:
                    return "Chunk size must less or equal to 1000."
            except ValueError:
                return "Invalid chunk size. Please enter a number."

            print(f"PARAMS:table_name: {table_name}, Chunk size: {chunk_size}")

        # Process data and get logs
        logs_file_path = process_valid_invalid_results(imported_file, chunk_size, table_name)
        response = {
                "table_name": table_name,
                "chunk_size": chunk_size,
                "imported_file":imported_file,
                "message": "Data processed successfully.",
                "logs_file_path": logs_file_path
        }
        print(response)
        return f"\n\nData processed successfully, check logs for details: \n{logs_file_path}.\n\n", 201
        # return jsonify(response), 201
    else:
        # get import transactions logs (last 100)
        number_of_logs=100
        import_logs_html = get_import_logs(number_of_logs=number_of_logs)
        # render the import page
        return render_template('import.html', import_logs_html=import_logs_html, number_of_logs=number_of_logs)

# BACKUPS
@app.route("/backups")
def backup_page():
    # load and dispaly existing backups
    backups = get_backup_files()
    return render_template('backups.html' , backups=backups)

@app.route("/backups-restore", methods=['GET', 'POST'])
def backup_restore():
    restore_file_name = request.form.get('restore_file_name')
    if request.method == 'POST':
        restore_file_name = request.form.get('restore_file_name')
        print(f"restore_file_name: {restore_file_name}")
        
        if restore_file_name == "":
            return "Please select a backup file to restore."
        
        # TODO: determine the table name from the backup file name best approach, 
        # extract from file name, e.g "hired_employees___1104e5c8-b133-4594-b231-31ab84a75fa8.avro"
        try:
            table_name = restore_file_name.split("___")[0]
        except:
            return "Invalid backup file name.", 400

        is_vallid, result = restore_backup(table_name, restore_file_name)
        if not is_vallid:
            return f"Error restoring backup: {result}"
        
        return f"Backup restored! File name: {restore_file_name} - table name: {table_name}\n\n{result}", 201

@app.route("/backups-create", methods=['GET', 'POST'])
def backup_create():
    if request.method == 'POST':
        table_name = request.form.get('table_name')
        
        # TODO: Check best error code for this case
        if table_name == "":
            return "Must provide table_name to create backup.", 200
        
        # Perform simple validation
        valid_tables = ["hired_employees", "departments", "jobs"]
        if table_name not in valid_tables:
            return "Invalid table name.", 400

        is_valid, result = create_backup(table_name)
        if not is_valid:
            return f"Error creating backup: {result}", 201
        
        print(f"result: {result}")
        return f"Backup created for Table name: {table_name}\n\n{result}", 201


@app.route('/serve/<path:filename>')
def serve_file(filename):
    """
    Serves a file from the specified directory.

    Args:
        filename: The path to the file within the FILE_DIRECTORY.  This should *not* include FILE_DIRECTORY itself.

    Returns:
        Flask Response: The file, or an error response if the file is not found or inaccessible.
    """
    filepath = os.path.join(RESULT_FOLDER, filename)

    # Security check: Ensure the file is within the designated directory
    if not filepath.startswith(RESULT_FOLDER):
        abort(403, description="Forbidden: Attempted access outside allowed directory.")  # Forbidden

    if not os.path.exists(filepath):
        abort(404, description=f"File not found: {filename}") # File not found

    # Determine the content type of the file
    content_type = mimetypes.guess_type(filepath)[0]
    if content_type is None:
      content_type = "application/octet-stream"  # Default content type if unknown

    try:
        return send_from_directory(RESULT_FOLDER, filename, mimetype=content_type)
    except Exception as e:
        abort(500, description=f"Internal Server Error: {e}") # Internal server error

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)

