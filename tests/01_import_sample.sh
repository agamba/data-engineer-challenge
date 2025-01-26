# IMPORT GIVEN DATA
curl -X POST -F "file=@data/departments.csv" \
-F "table_name=departments" \
-F "chunk_size=1000" http://127.0.0.1:8080/import

curl -X POST -F "file=@data/jobs.csv" \
-F "table_name=jobs" \
-F "chunk_size=1000" http://127.0.0.1:8080/import

curl -X POST -F "file=@data/hired_employees.csv" \
-F "table_name=hired_employees" \
-F "chunk_size=1000" http://127.0.0.1:8080/import



# TODO: AUTOMATION WITH MULTILE TYPES OF DATA


# IMPORT ERROR DATA

# IMPORT MIXED VALID/ERROR DATA

# IMPORT AI GEN DATA
