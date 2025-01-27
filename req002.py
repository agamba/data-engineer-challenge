# req001.py
""" Challenge 2, Requirement 1 """

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import uuid
from sqlalchemy import insert
from datetime import datetime, timezone

from config import RESULT_FOLDER
# load models and engine from models.py
from models import engine, Session, Report

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

def high_performing_departments(hired_employees_df, departments_df, year=2021):
    """
    Identifies departments that hired more employees than the 2021 average.

    Args:
        hired_employees_df: DataFrame representing the 'hired_employees' table.
        departments_df: DataFrame representing the 'departments' table.

    Returns:
        DataFrame with 'id', 'department', and 'hired' columns for qualifying departments,
        sorted by 'hired' in descending order, or None if an error occurs.
    """
    try:
        # Filter hires to 2021
        hired_employees_2021 = hired_employees_df[
            hired_employees_df['datetime'].dt.year == year
        ]

        # Calculate hires per department in 2021
        hires_per_department = hired_employees_2021.groupby('department_id').size().reset_index(name='hired')

        # Calculate the mean of hires across all departments in 2021
        mean_hires_2021 = hires_per_department['hired'].mean()

        # Filter departments that hired above the mean
        high_performing = hires_per_department[hires_per_department['hired'] > mean_hires_2021]

        # Merge with departments table to get department names
        result = pd.merge(high_performing, departments_df, left_on='department_id', right_on='id', how='left')

        # Select and rename columns
        result = result[['id', 'department', 'hired']].rename(columns={'id': 'id'})

        # Sort by 'hired' in descending order
        result = result.sort_values('hired', ascending=False)

        return result

    except Exception as e:  # Handle potential errors (e.g., missing data, incorrect types)
        error_message = f"\nEerror: {e}"
        # error_message += f"\nTraceback:\n{traceback.format_exc()}"
        print(error_message)
        return None

def generate_visualizations(df, uuid_sess):
    """
    Plots the number of hires per department using seaborn.

    Args:
        data: Pandas DataFrame with 'department' and 'hired' columns.
    """
    try:
        print("Generating plot images ...")
        images = []
        plt.figure(figsize=(10, 6))
        # sns.barplot(x='hired', y='department', data=df, palette="viridis")
        # fix
        sns.barplot(x='hired', y='department', data=df, hue='department', dodge=False, palette="viridis", legend=False)
        plt.title('Top Hires per Department')
        plt.xlabel('Number of Hires')
        plt.ylabel('Department')
        file_name = f'{RESULT_FOLDER}/{uuid_sess}___req_02_hires_dep_top.png'
        plt.savefig(file_name, bbox_inches='tight', dpi=150)
        # plt.show()
        images.append(file_name)
        print("Plot images geenrated !")

        return images

    except Exception as e:
        print(f"An error occurred in generate_visualizations(): {e}")        

def process_requirement2(year=2021):
    # create unique id for session
    uuid_sess = "" + str(uuid.uuid4())

    # load data from database
    sql_query = f"SELECT * FROM hired_employees"
    hired_employees_df = pd.read_sql(sql_query, engine)

    sql_query = f"SELECT * FROM departments"
    departments_df = pd.read_sql(sql_query, engine)

    result_df = high_performing_departments(hired_employees_df, departments_df, year)

    if result_df is not None:

        # generate plot images
        images = generate_visualizations(result_df, uuid_sess)

        # save results to csv
        report_csv_file = f'{RESULT_FOLDER}/{uuid_sess}__req_02_hires_dep_top.csv'
        result_df.to_csv(report_csv_file, index=False)

        # save also an html version of the dataframe for the report
        result_html = result_df.to_html(index=False, classes='table table-striped table-bordered table-hover')
        report_html_file = f'{RESULT_FOLDER}/{uuid_sess}__req_02_hires_dep_top.html'
        with open(report_html_file, 'w') as f:
            f.write(result_html)

        result_dic = {
            "report_name": "req_02_hires_dep_top",
            "datetime": datetime.now(),
            "html": report_html_file,
            "csv": report_csv_file,
            "images": ",".join(images) # return images as s string separated by commas
        }
        # log created results to database
        stmt = insert(Report).values(**result_dic)

        # Execute the insert statement within a unique session
        # TODO: evaluate better session management
        with Session() as session:
            session.execute(stmt)
            session.commit()
            session.close()

        return result_dic
    else:
        return None 
    
# result = process_requirement()
# print(result)