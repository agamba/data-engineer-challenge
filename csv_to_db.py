# csv_to_db.py
"""Define csv to db functions for importing, validating and inserting data into the database from a CSV file."""
from datetime import datetime
import traceback
import pandas as pd
import numpy as np
import json
from sqlalchemy import insert
import uuid

from config import LOGS_FOLDER, SHOW_CONSOLE_LOGS_IMPORT, columns_names_by_table
from models import engine, Session, Department, Job, HiredEmployee, Transaction, exc

def get_table_counts():
    query = """SELECT 
    (SELECT COUNT(*) FROM hired_employees) AS 'Total Hired Employees',
    (SELECT COUNT(*) FROM departments) AS 'Total Departments',
    (SELECT COUNT(*) FROM jobs) AS 'Total Jobs';"""
    result = pd.read_sql_query(query, engine)
    html = result.to_html(index=False)
    return html

def load_csv_data(file_name, chunk_size, table_name):
    columns_names = columns_names_by_table[table_name]
    # optional, Specify columns data types: e.g. dtype={'id': 'int64', }
    df_chunks = pd.read_csv(file_name, chunksize=chunk_size, names=columns_names)
    # print("Padas parsers object: ", df_chunks)
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

            # before start make a full copy of the df. to be used for logging purposes
            df_copy = df.copy()
            
            """Define validation rules for each table"""
            if(table_name=="hired_employees"):
                valid_data, invalid_data = validate_hired_employees(df)               
            elif(table_name=="departments"):
                valid_data, invalid_data = validate_departments(df)
            elif(table_name=="jobs"):
                valid_data, invalid_data = validate_jobs(df)
            else:
                print("table name not found")

            # collect all valid and invalid data in each chunk
            # TODO: consider collecting also original df to report exact values in logging
            results.append([valid_data, invalid_data, df_copy])
        
        return results
    
    except Exception as e:
            error_message = f"\nValidating batch. error: {e}"
            # error_message += f"\nTraceback:\n{traceback.format_exc()}"
            print(error_message)
            return None
    
def validate_hired_employees(df):
    """
    Validate the data in the hired_employees table.
    :param df: DataFrame containing the data to be validated.
    :return: tuple of 2 DataFrames. valid data and invalid data
    """
    if SHOW_CONSOLE_LOGS_IMPORT:
        print("\n\n\t validate_hired_employees()")
    # copy datetime column to new column for logging purposes and validating datetime conversions
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
    """
    Validate the departments data.
    :param df: DataFrame containing the data to be validated.
    :return: tuple of 2 DataFrames. valid data and invalid data
    """
    if SHOW_CONSOLE_LOGS_IMPORT:
        print("\n\n\t validate_departments()")
    
    # convert id to integers in imported data
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64') 

    # collect all rows with NaN values in a new df
    invalid_data = df[df.isnull().any(axis=1)]

    # Remove rows with NaNs in required fields
    df.dropna(subset=['id', 'department'], inplace=True)

    results = (df, invalid_data)
    return results

def validate_jobs(df):
    """
    Validate the jobs data.
    :param df: DataFrame containing the data to be validated.
    :return: tuple of 2 DataFrames. valid data and invalid data
    """
    if SHOW_CONSOLE_LOGS_IMPORT:
        print("\n\n\t validate_jobs()")
    
    # convert id to integers in imported data
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64') 

    # collect all rows with NaN values in a new df
    invalid_data = df[df.isnull().any(axis=1)]

    # Remove rows with NaNs in required fields
    df.dropna(subset=['id', 'job'], inplace=True)

    results = (df, invalid_data)
    return results

def create_data_object(table_name, row):
    """
    Create a data object based on the table name and row data.
    Args:
        table_name (str): The name of the table.
        row (pandas.Series): The row data.
    Returns:
        object: The data object using SQLAlchemy models.
    """
    try:
        if(table_name == "departments"):
            obj = {
                "id": row["id"],
                "department": row["department"],
            }
            return Department(**obj)

        elif(table_name == "jobs"):
            obj = {
                "id": row["id"],
                "job": row["job"],
            }
            return Job(**obj)
            
        elif(table_name == "hired_employees"):
            
            datetime_str = ""
            if("datetime_str" in row):
                datetime_str = row["datetime_str"]
            
            obj = {
                    "id": row["id"],
                    "name": row["name"],
                    "datetime": row["datetime"],
                    "datetime_str": datetime_str,
                    "department_id": row["department_id"],
                    "job_id": row["job_id"],
            }            
            return HiredEmployee(**obj)

        else:
            if SHOW_CONSOLE_LOGS_IMPORT:
                print(f"Table name {table_name} not recognized")
            return None
    
    except Exception as e:
        error_message = f"\nError creating data object for table: {table_name}. error: {e}"
        # error_message += f"\nTraceback:\n{traceback.format_exc()}" # only for dev mode
        print(error_message)
        return None

def insert_data_to_db(batches, table_name):
    """
    Inserts data into the database.
    Args:
        batches (list): A list of batches, where each batch is a tuple containing valid and invalid data.
        table_name (str): The name of the table to insert data into.
    Returns:
        json_log_file (str): path to JSON file containing the log of the insertion process.
    """
    if SHOW_CONSOLE_LOGS_IMPORT:
            print("\n\n\t insert_data_to_db()")
    try:
        # create db session
        session = Session()

        # TODO: need to determine better logic for saving logs, 1 log file per request or 1 per batch 
        # for the moment 1 log file per request and batches containing any duplicate id will be rejected and not logged 
        logs = []

        # loop over array of results (valild[0], invalid[1])
        for i, batch in enumerate(batches):
            # Uncoment ONLY for development mode 
            # if SHOW_CONSOLE_LOGS_IMPORT:
            #     print("==========")
            #     print("batch: ", i+1)
            #     print("VALID DATA")
            #     print(batch[0])
            #     print("INVALID DATA")
            #     print(batch[1])
            #     print("==========")

            # loop Valid data
            for index, row in batch[0].iterrows():
                data_object = create_data_object(table_name=table_name, row=row)
                session.add(data_object)
            
            # save all invalid data into json log file
            # Replace NaN values with empty strings in invalid df
            invallid_df = batch[1].copy()
            # add spacial case for "datetime" columns

            if(table_name == "hired_employees"):
                # drop datetime column
                invallid_df = invallid_df.drop('datetime', axis=1)  # axis=1 specifies column
                invallid_df = invallid_df.rename(columns={'datetime_str': 'datetime'}) 

            # replace NaN values with 0
            invallid_df.fillna(0, inplace=True)

            batch_number = i+1

            # catch exceptions when committing each batch
            try:
                # pass
                # comit each batch to db
                session.commit()
                result_log = {
                    "table_name": table_name,
                    "batch_number": batch_number,
                    "total_valid_records": len(batch[0]),
                    "total_invalid_records": len(batch[1]),
                    "invalid_data": invallid_df.to_dict(orient="records"),
                    "status": "success",
                    "message": "Valid data in batch inserted into database.",
                    "timestamp": datetime.now().timestamp()
                    
                }
                logs.append(result_log)

            except exc.IntegrityError as e:
                session.rollback()
                error_message = f"IntegrityError processing batch."
                error_message += "Detailed error: " + str(e)
                # error_message += f"\nTraceback:\n{traceback.format_exc()}" # for dev only

                # TODO: further evaluation is needed to determine the type of logging in this scenario
                result_log_rejected = {
                    "table_name": table_name,
                    "batch_number": batch_number,
                    "total_valid_records": len(batch[0]),
                    "total_invalid_records": len(batch[1]),
                    "invalid_data": invallid_df.to_dict(orient="records"),
                    "status": "rejected",
                    "message": "Data in batch violates existing data integrity constraints",
                    "timestamp": datetime.now().timestamp(),
                    "error_message": str(e._message()), 
                    "error_params": str(e.params)
                }
                logs.append(result_log_rejected)

        # save logs to json file for each request
        json_log_file  = dump_json_to_file(logs, table_name)

        # Add log metadata to database
        transaction_data = {
            'table_name': table_name,
            'datetime': datetime.now(),
            'json_log_file': json_log_file
        }
  
        # Add transaction log event into database
        stmt = insert(Transaction).values(**transaction_data)
        session.execute(stmt)
        session.commit()

        # close db session
        session.close()
        
        if SHOW_CONSOLE_LOGS_IMPORT:
            print(f"Logs saved successfuly at path: {json_log_file}")

        return json_log_file
    
    except Exception as e:
        session.rollback()
        error_message = f"\n\nException Error processing batch.\n\nerror: {e}"
        # error_message += f"\nTraceback:\n{traceback.format_exc()}" 
        print(error_message)
        return "no_log_file_created"

def process_valid_invalid_results(file_name, chunk_size, table_name):
    """
    This function processes the data in chunks and separates valid and invalid data for each batch.
    
    Parameters:
    - file_name: str: The name of the file to be processed.
    - chunk_size: int: The size of each chunk to be processed.
    - table_name: str: The name of the table to be processed.
    """
    if SHOW_CONSOLE_LOGS_IMPORT:
        print("\n\n\t process_valid_invalid_results")
        print("Chunk size: ", chunk_size)
        print("Table name: ", table_name)
        print("File name: ", file_name)
    
    # 1. get data batches
    df_batches = load_csv_data(file_name, chunk_size, table_name)
    
    # 2. separate valid and invalid data for each batch
    valid_invalid_array = separate_valid_invalid_data(df_batches, table_name)

    num_of_batches = len(valid_invalid_array)
    
    if SHOW_CONSOLE_LOGS_IMPORT:
        print("Number of batchs: ", num_of_batches)
    
    if(num_of_batches == 0):
        print(f"No data to process. {file_name}, {chunk_size}, {table_name}")
        # TODO: return error message
        return None

    # 3. insert valid data into db and generate json log file 
    import_log_json_file = insert_data_to_db(valid_invalid_array, table_name)

    if SHOW_CONSOLE_LOGS_IMPORT:
        print("\n\nimport_log_json_file: ", import_log_json_file)
    
    return import_log_json_file


def get_datetime_string():
    """Generates a string representing the current time """
    now = datetime.now()
    return now.strftime('%Y-%m-%d_%H_%M_%S')

def dump_json_to_file(data, table_name):
  """Dumps JSON data to a file with error handling and statistics about import.

  Args:
    data: The Python dictionary or list to be dumped as JSON.
    file_path: The path to the file where the JSON data will be written.

  Returns:
    True if the JSON data was successfully dumped, False otherwise.
  """
  try:
    file_path = f"{LOGS_FOLDER}/{table_name}___{uuid.uuid4()}.json"
    # print("file_path: ", file_path)

    with open(file_path, 'w') as f:
      json.dump(data, f, indent=4)
    return file_path
  except IOError as e:
    print(f"An error occurred while writing to the file: {e}")
    return False
  except json.JSONEncoder as e:
    print(f"An error occurred while encoding JSON: {e}")
    return False
