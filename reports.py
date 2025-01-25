# In case of error with recent version of pandas and numpy 
# pip uninstall numpy
# pip uninstall pandas
# pip install pandas==1.3.5
# pip install numpy==1.26.4

import pandas as pd
import traceback
import seaborn as sns
import matplotlib.pyplot as plt
import os
import uuid
from models import engine

RESULT_FOLDER = 'RESULTS'
if not os.path.exists(RESULT_FOLDER):
    os.makedirs(RESULT_FOLDER)

"""
Challenge 2
Requiment 1
Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
"""
# TODO: 
# Create a function to generate dataframe with the number of employees hired for each job and department in 2021 divided by quarter.
# Create a function to generate a visualization of the dataframe.
# export result data in csv and or html for dashboard


"""
Challenge 2
Requiment 2
List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).
"""
# TODO: 
# Create a function to generate dataframe with high performing departments for 2021
# Create a function to generate a visualization of the dataframe.
# export result data in csv and or html for dashboard

