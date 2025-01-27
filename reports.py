# reports.py

# for clarity, separated functions for each requirement
from req001 import process_requirement1
from req002 import process_requirement2

# TODO: remove this file and call this directory into Flask app
# create a new session
"""
Challenge 2
Requiment 1
"""
results = process_requirement1(year=2021)
print(results)

"""
Challenge 2
Requiment 2
"""
results = process_requirement2()
print(results)
