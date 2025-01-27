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


def hires_quarter(df, year=2021):
    """
    Calculates the number of employees hired per department and job for each quarter in a year.
    Ensures all four quarters are present in the output, even if no hires occurred.

    Args:
        df (pd.DataFrame): DataFrame with 'datetime', 'department_id', 'job_id', and 'employee_id' columns.
        year (int): Year to filter the data. Default is 2021.

    Returns:
        pd.DataFrame: Number of hires by department, job, and quarter, 
                      sorted ascending by department_id and job_id.  All four quarters
                      are included as columns.
    """ 
    # Add quarter column to df
    df['quarter'] = df['datetime'].dt.quarter

    # Create filter and filter data for year
    filt = (df['datetime'].dt.year == year)
    df_filtered_by_year = df[filt]

    # Group by (on filter slice) department, job, and quarter and count unique employee IDs:
    hires_group_dep_job_quarter  = df_filtered_by_year.groupby(['department_id', 'job_id', 'quarter'])['employee_id'].nunique().reset_index()

    # Pivot the table to have quarters as columns:
    hires_df = hires_group_dep_job_quarter.pivot_table(index=['department_id', 'job_id'], columns='quarter', values='employee_id', fill_value=0).reset_index()

    ################ fix
    # Ensure all four quarters are present:
    for quarter in range(1, 5):  # Check for quarters 1 through 4
        if quarter not in hires_df.columns:
            hires_df[quarter] = 0  # Add missing quarter and fill with 0

    # Reorder columns to ensure Q1, Q2, Q3, Q4 order (if needed):
    quarter_cols = [col for col in hires_df.columns if isinstance(col, (int, np.integer)) and 1 <= col <=4]
    other_cols = [col for col in hires_df.columns if col not in quarter_cols]
    hires_df = hires_df[other_cols + sorted(quarter_cols)]
    ################

    # Sort alphabetically:
    # TODO: check required order
    hires_df = hires_df.sort_values(['department_id', 'job_id'])

    return hires_df


# test loading data
df, departments_df, jobs_df = load_data()
print(df.head())
print(departments_df.head())
print(jobs_df.head())
print()

print("#" * 20)

# test hires_quarter

hires_quarter_df = hires_quarter(df)
print(hires_quarter_df.head())
print()