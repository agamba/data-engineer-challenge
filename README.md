# Data Engineer Challenge

- Migrate data from CSV files to a new database
- csv files: hired_employees, jobs, departments

## Challenge 1

1. Move data from files in CSV format to the new database.
2. Create a Rest API service to receive new data. This service must have:
   \
    2.1. Each new transaction must fit the data dictionary rules.
   \  
    2.2. Be able to insert batch transactions (1 up to 1000 rows) with one request.
   \
    2.3. Receive the data for each table in the same service.
   \
    2.4. Keep in mind the data rules for each table.
   \
3. Create a feature to backup for each table and save it in the file system in AVRO format.
4. Create a feature to restore a certain table with its backup.

## Challenge 2

- "Number of employees hired for each job and department in 2021 divided by quarter. The
  table must be ordered alphabetically by department and job."

- Add dashboard to visualize data
- Make abailable stakeholders' metrics/reports
- Use existing libs or create my own for visualization?

# Project structure (Plan)

├── models.py # Database models (SQLAlchemy)
\
├── csv_to_db.py # CSV and validation functionality
\
├── main.py # create API
\
├── requirements.txt # Python dependencies
\
├── .gitignore # Files to ignore in git
\
├── README.md # Project documentation
\
├── data # Directory for sample data
\
├── uploads # Directory for data uploaded by users
\
├── logs # Directory for json logs
\

# Development plan - Todo App Code

\
[X] Set up github repo
\
[X] Load data from csv files in batches
\
[X] test DB connection (try local mysql to start)
\
[X] Create data models, using sqlalchemy
\
[X] Validate data using pandas: missing and incorrect values, date format
\
[X] Verify mysql tables are dynamically created from models
\
[X] Create draft API with Flask
\
[X] Integrate csv to db import function into API routes
\
[X] Implement Backup Create and Restore functionality
\
[ ] Add Backup Create and Restore to UI
\
[ ] Implement Reports for requirements
\
[ ] Implement Reports using ploty and chart.js
\
[ ] Isolate db credentials (.env or other method? clound based?)
\
[X] Evaluate best practice for logging failed transactions
\
[X] Log transactions in json file in logs folder
\
[ ] Implement securuty, e.g. required API_KEY

# Considerations

- Consider performance implications in validation
- focus on app logic localy first
- Implement configuration for db type (e.g. mysql, postgress, other)? out of scope for now

# Validation cases:

- number of columns
- null and empty values
- is a valid ISO date
- department_id exists in departments table
- job_id exists in jobs table
