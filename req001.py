# req001.py
""" Challenge 2, Requirement 1 """

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import uuid
from sqlalchemy import insert
from datetime import datetime, timezone

from config import RESULT_FOLDER, columns_names_by_table
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


def hires_quarter(df, departments_df, jobs_df, year=2021):
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

    ################ fixed
    # Ensure all four quarters are present:
    for quarter in range(1, 5):  # Check for quarters 1 through 4
        if quarter not in hires_df.columns:
            hires_df[quarter] = 0  # Add missing quarter and fill with 0

    # Reorder columns to ensure Q1, Q2, Q3, Q4 order (if needed):
    quarter_cols = [col for col in hires_df.columns if isinstance(col, (int, np.integer)) and 1 <= col <=4]
    other_cols = [col for col in hires_df.columns if col not in quarter_cols]
    hires_df = hires_df[other_cols + sorted(quarter_cols)]
    ################

    # TODO: doube check required order
    hires_df = hires_df.sort_values(['department_id', 'job_id'])

    # Adjust data frame to requirement 
    
    # rename quarter columns to string values
    hires_df.rename(columns={1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}, inplace=True)

    # join with departments and jobs to get the names
    hires_df_dept_jobs = hires_df.merge(departments_df, on='department_id', how='left').merge(jobs_df, on='job_id', how='left') 

    # Set a new column order 
    new_order = ['department', 'job', 'department_id', 'job_id', 'Q1', 'Q2', 'Q3', 'Q4']

    # Reorder the DataFrame
    hires_df_dept_jobs = hires_df_dept_jobs[new_order]

    # drop department_id and job_id columns
    hires_df_dept_jobs.drop(['department_id', 'job_id'], axis=1, inplace=True)

    return hires_df_dept_jobs


def generate_visualizations(df, uuid_sess):
    """ 
    Geneate a plot for the report using seaborn
    can be computationally expensive for large datasets. 
    TODO: consider other options. e.g, chart.js for web reports
    Args:
        df (pd.DataFrame): DataFrame with the data to plot
    Returns:
    """
    # Initialize lists to store generated plot images
    images = []

    # Melt the DataFrame to make it suitable for seaborn
    df_melted = df.melt(id_vars=['department', 'job'], var_name='quarter', value_name='hires')

    # Convert quarter to string (for categorical plotting)
    df_melted['quarter'] = df_melted['quarter'].astype(str)

    print("Generating plot images ...")
    # Option 1: 
    # Heatmap (Good for overview)
    ##################
    # generte image full path to image file name
    image_name1 = f'{RESULT_FOLDER}/{uuid_sess}___req_01_hires_dep_job_quarter_heatmap.png'

    plt.figure(figsize=(15, 8))
    sns.heatmap(df.set_index(['department', 'job']), annot=True, cmap="YlGnBu", fmt=".0f", cbar_kws={'label': 'Number of Hires'})
    plt.title('Hires by Department, Job, and Quarter')
    # fixing saved image cropped
    plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels if needed
    plt.tight_layout()  # Adjust subplot parameters for padding
    plt.savefig(image_name1, bbox_inches='tight', dpi=300) 
    # plt.show() # Display the plot
    images.append(image_name1) # add image to list

    # Option 2
    # Grouped bar chart (comparing quarters and departments)
    ##################
    
    image_name2 = f'{RESULT_FOLDER}/{uuid_sess}___req_01_hires_dep_quarter_barchar.png'
    plt.figure(figsize=(12, 10))
    sns.barplot(x='department', y='hires', hue='quarter', data=df_melted)
    plt.title('Hires by Department and Quarter')
    # fixing saved image cropped
    plt.xticks(rotation=45, ha='right') # rotate x-axis labels if needed
    plt.tight_layout()  # Adjust subplot parameters for padding
    plt.xlabel('Department')
    plt.ylabel('Number of Hires')
    plt.savefig(image_name2, bbox_inches='tight', dpi=300)
    # plt.show()
    images.append(image_name2) # add image to list

    print("Plot images geenrated !")

    return images

def process_requirement(year=2021):
    """ 
    Process the requirement 1
    Arguements:
        year (int): year to process
    Returns:
        result_dic (dic): Dictionary with the results
    """

    # create unique id for session
    uuid_sess = "" + str(uuid.uuid4())

    # loading data
    df, departments_df, jobs_df = load_data()
    # print(df.head())
    # print(departments_df.head())
    # print(jobs_df.head())
    # print()
    # print("#" * 20)

    # test hires_quarter
    hires_df_dept_jobs = hires_quarter(df, departments_df, jobs_df, year)
    print(hires_df_dept_jobs.head())
    

    if hires_df_dept_jobs is not None:
        # generate plot images
        images = generate_visualizations(hires_df_dept_jobs, uuid_sess)
        
        # save results to csv
        report_csv_file = f'{RESULT_FOLDER}/{uuid_sess}___req_01_hires_dep_job_quarter.csv'
        hires_df_dept_jobs.to_csv(report_csv_file, index=False)

        # save also an html version of the dataframe for the report
        result_html = hires_df_dept_jobs.to_html(index=False, classes='table table-striped table-bordered table-hover')
        report_html_file = f'{RESULT_FOLDER}/{uuid_sess}___req_01_hires_dep_job_quarter.html'
        with open(report_html_file, 'w') as f:
            f.write(result_html)

        result_dic = {
            "report_name": "req_01_hires_dep_job_quarter",
            "datetime": datetime.now(),
            "html": report_html_file,
            "csv": report_csv_file,
            "images": ",".join(images) # return images as s string separated by commas
        }

        # log created results to database
        stmt = insert(Report).values(**result_dic)

        # Execute the insert statement within a unique session
        with Session() as session:
            session.execute(stmt)
            session.commit()
            session.close()

        return result_dic
    else:
        return None

# results = process_requirement(year=2021)
# print(results)