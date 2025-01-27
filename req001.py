# req001.py
""" Challenge 2, Requirement 1 """

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import uuid

from config import RESULT_FOLDER, columns_names_by_table
# load models and engine from models.py
from models import engine, Job, Department, HiredEmployee

def load_data():
    # load data from database
    sql_query = f"SELECT * FROM hired_employees"
    df = pd.read_sql(sql_query, engine)
    df.rename(columns={'id': 'employee_id'}, inplace=True)   # rename id to avoid conflicts

    sql_query = f"SELECT * FROM departments"
    departments_df = pd.read_sql(sql_query, engine)
    departments_df.rename(columns={'id': 'department_id'}, inplace=True)  # rename id to enable inner join 

    sql_query = f"SELECT * FROM jobs"
    jobs_df = pd.read_sql(sql_query, engine)
    jobs_df.rename(columns={'id': 'job_id'}, inplace=True)  # rename id to enable inner join 

    # Join departments and jobs to validate foreign keys
    df = pd.merge(df, departments_df, on='department_id', how='left')
    df = pd.merge(df, jobs_df, on='job_id', how='left')

    return df, departments_df, jobs_df

# test loading data
df, departments_df, jobs_df = load_data()
print(df.head())
print(departments_df.head())
print(jobs_df.head())
print()

