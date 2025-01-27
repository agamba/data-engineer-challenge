# IMPORT DATA
curl -X POST -F "file=@data/departments.csv" \
-F "table_name=departments" \
-F "chunk_size=1000" http://127.0.0.1:8080/import

curl -X POST -F "file=@data/jobs.csv" \
-F "table_name=jobs" \
-F "chunk_size=1000" http://127.0.0.1:8080/import

curl -X POST -F "file=@data/hired_employees.csv" \
-F "table_name=hired_employees" \
-F "chunk_size=1000" http://127.0.0.1:8080/import

# RESTORE BACKUCP
curl -X POST \
-F "restore_file_name=departments___d51850d6-0974-4563-80d9-4f437fecd8b6.avro" \
http://127.0.0.1:8080/backups-restore


# CREATE BACKUCP
curl -X POST \
-F "table_name=departments" \
http://127.0.0.1:8080/backups-create
