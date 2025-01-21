""" Util functions and constants """

# Define api variables
################################
table_name = "hired_employees"
file_name = './data/hired_employees.csv'
file_name = './data/hired_employees_error.csv'

# table_name = "departments"
# file_name = './data/departments.csv'
# file_name = './data/departments_error.csv'

# table_name = "jobs"
# file_name = './data/jobs.csv'
# file_name = './data/jobs_error.csv'

chunk_size = 1000

# Define the column names for each table
# assuming that csv does not have header
columns_names_by_table = {
    "departments": ['id', 'department'],
    "jobs": ['id', 'job'],
    "hired_employees": ['id', 'name', 'datetime', 'department_id', 'job_id']
}

