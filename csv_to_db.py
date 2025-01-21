"""
This module is responsible for importing and validating data from a CSV file into the database.
"""
from datetime import timezone
import datetime 
import traceback
import pandas as pd
import numpy as np

# import db models
from models import engine, Session, Department, Job, HiredEmployee, DepartmentFailed, JobFailed, HiredEmployeeFailed

# import utility functions

"""Define csv to db functions"""
#################################

def load_csv_data(file_name, chunk_size, table_name):
    columns_names = columns_names_by_table[table_name]
    # optional, Specify columns data types: e.g. dtype={'id': 'int64', }
    df_chunks = pd.read_csv(file_name, chunksize=chunk_size, names=columns_names)
    print("Padas chunks csv", df_chunks)
    return df_chunks

def process_df_chunks(df_chunks, table_name):
    # TODO: loop over chunks and validate and process data in each chunk
    results = []
    for df in df_chunks:
        # 1. TODO: validate data in each chunk
        # consider separating valid and invalid data in df and process them separately
        # validate_data(df, table_name)

        # 2. TODO: insert valid data in sql 
        # insert_data(df, table_name)
        # consider using pandas to_sql() or other SQLAlchemy methods
        # e.g. chunk.to_sql(name='your_table', con=engine, if_exists='append', index=False)

        # 3. TODO: handle errors

        print("---")
        print(df)
    
    return results


#################################
# load db session
session = Session()

# Define the column names for each table
# assuming that csv does not have header
columns_names_by_table = {
    "departments": ['id', 'department'],
    "jobs": ['id', 'job'],
    "hired_employees": ['id', 'name', 'datetime', 'department_id', 'job_id']
}

# Define api variables
################################
table_name = "hired_employees"
file_name = './data/hired_employees.csv'
# file_name = './data/hired_employees_error.csv'

# table_name = "departments"
# file_name = './data/departments.csv'
# file_name = './data/departments_part.csv'

# table_name = "jobs"
# file_name = './data/jobs.csv'

chunk_size = 4

################################
# 1. get data batches
df_batches = load_csv_data(file_name, chunk_size, table_name)

# 2. Validate and process df batches
result = process_df_chunks(df_batches, table_name)

# validate_and_process_data_batches(session, batches, table_name=table_name, file_name=file_name)

session.close()
engine.dispose()
