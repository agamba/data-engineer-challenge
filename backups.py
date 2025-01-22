""" create and restore backups of the database in AVRO format """
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.orm import sessionmaker
from fastavro import writer, reader, parse_schema
import os
import uuid

# re use sqlAlchemy engine from models.py
from models import engine

Session = sessionmaker(bind=engine)
metadata = MetaData()

BACKUPS_FOLDER = 'backups'


# Ensure the backups folder exists
if not os.path.exists(BACKUPS_FOLDER):
    os.makedirs(BACKUPS_FOLDER)


def create_backup(table_name: str):
    """
    Creates a backup of a specific table by exporting it to an Avro file.

    :param table_name: The name of the table to back up.
    :param backup_file: Path to the Avro file to store the backup.
    """
    try:
        random_uuid = uuid.uuid4()
        backup_file=f"{BACKUPS_FOLDER}/{table_name}_{random_uuid}.avro"
        
        # Reflect the table
        metadata.reflect(bind=engine)
        table = Table(table_name, metadata, autoload_with=engine)

        # Query all rows from the table
        session = Session()
        rows = session.execute(select(table)).fetchall()
        session.close()

        # Define the Avro schema based on the table columns
        schema = {
            "type": "record",
            "name": table_name,
            "fields": [{"name": col.name, "type": "string"} for col in table.columns],
        }

        # Write the data to an Avro file
        with open(backup_file, "wb") as f:
            writer(f, parse_schema(schema), [dict(row._mapping) for row in rows])

        print(f"Backup of table '{table_name}' created at: {backup_file}")
    except Exception as e:
        print(f"Error creating backup: {e}")


create_backup(table_name="jobs")

