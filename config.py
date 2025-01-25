# config.py
import os
from dotenv import load_dotenv

SHOW_CONSOLE_LOGS_IMPORT = True
SHOW_CONSOLE_LOGS_API = True
SHOW_CONSOLE_LOGS_REPORTS = True

# TODO: Make this dynamic from DB models
columns_names_by_table = {
    "departments": ['id', 'department'],
    "jobs": ['id', 'job'],
    "hired_employees": ['id', 'name', 'datetime', 'department_id', 'job_id']
}

# Load environment variables from .env file
load_dotenv()
# TODO: Prepare for loading env vars in cloud environment

# Get DB credential variables
host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")
port = os.getenv("MYSQL_PORT")

DATABASE_URI = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
API_URL = 'http://127.0.0.1:8080'

"""define directories"""
# used in import pipline
UPLOAD_FOLDER = 'UPLOADS'
LOGS_FOLDER  = 'LOGS'
# used in reports and backup pipline
RESULT_FOLDER = 'RESULTS'
BACKUPS_FOLDER = 'BACKUPS'

# create directores if not exist
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
if not os.path.exists(RESULT_FOLDER):
    os.makedirs(RESULT_FOLDER)
if not os.path.exists(LOGS_FOLDER):
    os.makedirs(LOGS_FOLDER)
if not os.path.exists(BACKUPS_FOLDER):
    os.makedirs(BACKUPS_FOLDER)
