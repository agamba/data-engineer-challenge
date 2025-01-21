"""
This module is responsible for importing and validating data from a CSV file into the database.
"""
from datetime import timezone
import datetime 
import traceback
import pandas as pd
import numpy as np

# import db models
from models import engine, Session, Department, Job, HiredEmployee

# import utility functions
# TODO: need to move some vars to Flask app
from util import file_name, chunk_size, table_name, columns_names_by_table

"""Define csv to db functions"""
#################################

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
    """
    Validate the departments data.
    :param df: DataFrame containing the departments data.
    :return: DataFrame containing the valid and invalid data.
    """
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
    """
    Validate the jobs data.
    :param df: DataFrame containing the jobs data.
    :return: DataFrame containing the valid and invalid data.
    """
    print("\t validate_jobs()")
    
    # convert id to integers in imported data
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64') 

    # collect all rows with NaN values in a new df
    invalid_data = df[df.isnull().any(axis=1)]

    # Remove rows with NaNs in required fields
    df.dropna(subset=['id', 'job'], inplace=True)

    results = (df, invalid_data)
    return results

def create_data_object(table_name, row, is_valid_data):
    """
    Create a data object based on the table name and row data.
    Create different data objects for valid and invalid data.
    Args:
        table_name (str): The name of the table.
        row (pandas.Series): The row data.
        is_valid_data (bool): The validity of the data.
    Returns:
        object: The data object using SQLAlchemy models.
    """
    try:

        if(table_name == "departments"):
            obj = {
                "id": row["id"],
                "department": row["department"],
            }
            if(is_valid_data):
                return Department(**obj)
            else:
                return DepartmentFailed(**obj)

        elif(table_name == "jobs"):
            obj = {
                "id": row["id"],
                "job": row["job"],
            }
            if(is_valid_data):
                return Job(**obj)
            else:
                return JobFailed(**obj)
        elif(table_name == "hired_employees"):
            
            datetime_str = ""
            if("datetime_str" in row):
                datetime_str = row["datetime_str"]
            
            obj = {
                    "employee_id": row["id"],
                    "name": row["name"],
                    "datetime": row["datetime"],
                    "datetime_str": datetime_str,
                    "department_id": row["department_id"],
                    "job_id": row["job_id"],
            }
            if(is_valid_data):
                return HiredEmployee(**obj)
            else:
                # remove datetime_str because it's not logged into HiredEmployeeFailed
                del obj["datetime_str"]
                return HiredEmployeeFailed(**obj)
        else:
            print(f"Table name {table_name} not recognized")
            return None

    
    except Exception as e:
        error_message = f"\nError creating data object for table: {table_name}. error: {e}"
        error_message += f"\nTraceback:\n{traceback.format_exc()}"  # Add traceback information
        print(error_message)
        return None
    
def insert_data_to_db(batches, table_name):
    """
    """
    print("\t insert_data_to_db()")
    # load db session
    session = Session()
    print("batches type: ", type(batches))
    print("batches len: ", len(batches))
    print("==========")

    # loop over array of results (valild[0], invalid[1])
    # loop over batches
    for i, batch in enumerate(batches):
        print("==========")
        print("batch: ", i+1)
        print("VALID DATA")
        print(batch[0])
        print("INVALID DATA")
        print(batch[1])
        print("==========")

        # loop Valid data
        for index, row in batch[0].iterrows():
            data_object = create_data_object(table_name=table_name, row=row, is_valid_data=True)
            session.add(data_object)
        
        # save all invalid data into json log file
        # Replace NaN values with empty strings in invalid df
        copy_invallid = batch[1].copy()
        copy_invallid.fillna(0, inplace=True)
        error_log = {
            "file_name": file_name,
            "table_name": table_name,
            "batch_number": i+1,
            "total_valid_records": len(batch[0]),
            "total_invalid_records": len(batch[1]),
            "invalid_data": copy_invallid.to_dict(orient="records")
        }

        print(error_log)


    
    session.commit()
        

    results = ""
    
    return results
    # close db session and engine
    session.close()
    # engine.dispose() # consider keeping connection open?

def process_valid_invalid_results(file_name, chunk_size, table_name):
    """
    """
    print("###################################")
    print("Chunk size: ", chunk_size)
    print("Table name: ", table_name)
    print("File name: ", file_name)
    
    # 1. get data batches
    df_batches = load_csv_data(file_name, chunk_size, table_name)
    print("###################################")
    print("Pandas parser object: ", df_batches)
    
    # 2. separate valid and invalid data for each batch
    valid_invalid_array = separate_valid_invalid_data(df_batches, table_name)

    print("###################################")
    print("CHECK CONSISTENCY OF DATA")
    # print(valid_invalid_array)
    print("type valid_invalid_array: ", type(valid_invalid_array))
    num_of_batches = len(valid_invalid_array)
    print("Number of batchs: ", num_of_batches)
    
    if(num_of_batches == 0):
        print("No data to process")
        return False

    # 3. insert valid and invalid data into db
    results_db = insert_data_to_db(valid_invalid_array, table_name)

    return True

result = process_valid_invalid_results(file_name, chunk_size, table_name)