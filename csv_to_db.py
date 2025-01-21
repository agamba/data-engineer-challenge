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
    print("Padas parsers object: ", df_chunks)
    return df_chunks

def separate_valid_invalid_data(df_chunks, table_name):
    """
    This function is responsible for separating valid and invalid data in each chunk
    Parameters:
        df_chunks: pandas dataframe chunks
        table_name: name of the table to be processed
    Returns:
        valid_data: valid data in each chunk
        invalid_data: invalid data in each chunk
    """

    results = [] # list to store results of each chunk
    # loop over chunks and validate and process data in each chunk
    
    try:
        for df in df_chunks:
            # 1. TODO: validate data in each chunk
            # consider separating valid and invalid data in df and process them separately
            # validate_data(df, table_name)

            # before start make a full copy of the df. to be used for logging purposes
            df_copy = df.copy()
            
            """Define validation rules for each table"""

            if(table_name=="hired_employees"):
                valid_data, invalid_data = validate_hired_employees(df)
                # print("valid_data", valid_data)
                # print("invalid_data", invalid_data)
                print("LEN valid_data", len(valid_data))
                print("LEN invalid_data", len(invalid_data))
                
            elif(table_name=="departments"):
                valid_data, invalid_data = validate_departments(df)
                
            elif(table_name=="jobs"):
                valid_data, invalid_data = validate_jobs(df)
                
            else:
                print("table name not found")

            # collect all valid and invalid data in each chunk
            results.append([valid_data, invalid_data])

            
        # TODO: consider creating a dictionary to store valid and invalid stats ?
        return valid_data, invalid_data
    
    except Exception as e:
            error_message = f"\nValidating batch. error: {e}"
            error_message += f"\nTraceback:\n{traceback.format_exc()}"
            print(error_message)
            return None
    
def validate_hired_employees(df):
    """
    Validate the data in the hired_employees table.
    :param df: DataFrame containing the data to be validated.
    :return: DataFrame containing the valid data.
    """
    print("\t validate_hired_employees()")
    # copy datetime column to new column for logging purposes and validating duplicates
    df['datetime_str'] = df['datetime'].copy()

    """ convert columns to appropriate data types so all invalid data is NaN"""
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')  # Nullable integer type
    df['department_id'] = pd.to_numeric(df['department_id'], errors='coerce').astype('Int64')
    df['job_id'] = pd.to_numeric(df['job_id'], errors='coerce').astype('Int64')
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')  # convert datetime to pd format

    # Create a new df with all rows with NaN values
    invalid_data = df[df.isnull().any(axis=1)]

    # Remove rows with NaNs in all required fields so we have only the valid data
    df.dropna(subset=['id', 'name', 'datetime', 'department_id', 'job_id'], inplace=True)
    
    # return a tuple with valid data and invalid data
    results = (df, invalid_data)
    return results

def validate_departments(df):
    print("\t validate_departments()")
    
    # convert id to integers in imported data
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64') 

    # collect all rows with NaN values in a new df
    invalid_data = df[df.isnull().any(axis=1)]

    # Remove rows with NaNs in required fields
    df.dropna(subset=['id', 'department'], inplace=True)

    results = (df, invalid_data)
    return results


def validate_jobs(df):
    print("\t validate_jobs()")
    
    # convert id to integers in imported data
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64') 

    # collect all rows with NaN values in a new df
    invalid_data = df[df.isnull().any(axis=1)]

    # Remove rows with NaNs in required fields
    df.dropna(subset=['id', 'job'], inplace=True)

    results = (df, invalid_data)
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
# table_name = "hired_employees"
# file_name = './data/hired_employees.csv'
# file_name = './data/hired_employees_error.csv'

# table_name = "departments"
# file_name = './data/departments.csv'
# file_name = './data/departments_error.csv'

table_name = "jobs"
# file_name = './data/jobs.csv'
file_name = './data/jobs_error.csv'

chunk_size = 1000

################################
# 1. get data batches
df_batches = load_csv_data(file_name, chunk_size, table_name)

# 2. separate valid and invalid data for each batch
result = separate_valid_invalid_data(df_batches, table_name)
print("\t RESULTS: separate_valid_invalid_data")
print("VALID DATA")
print(result[0])

print("INVALID DATA")
print(result[1])


# validate_and_process_data_batches(session, batches, table_name=table_name, file_name=file_name)

session.close()
engine.dispose()
