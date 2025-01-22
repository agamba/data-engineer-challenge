""" create and restore backups of the database in AVRO format """
import traceback
import os
import uuid
import fastavro
from sqlalchemy import create_engine, MetaData, select, Column, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.sqltypes import Integer, String, DateTime, Boolean, Float, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

# re use sqlAlchemy engine from models.py
from models import engine, Job, Department, HiredEmployee

Session = sessionmaker(bind=engine)
metadata = MetaData()

BACKUPS_FOLDER = 'backups'


# Ensure the backups folder exists
if not os.path.exists(BACKUPS_FOLDER):
    os.makedirs(BACKUPS_FOLDER)
def get_avro_type(sql_type):
    if isinstance(sql_type, Numeric):  # Check for Numeric type first
        return {"type": "bytes", "logicalType": "decimal", "precision": sql_type.precision, "scale": sql_type.scale}
    type_map = {
        Integer: "int",
        String: "string",
        DateTime: {"type": "long", "logicalType": "timestamp-millis"},
        Boolean: "boolean",
        Float: "float",
    }
    return type_map.get(type(sql_type), "string") 

def create_backup(model_class):
    """Creates an Avro backup of a SQLAlchemy model's table."""
    try:
        table_name = model_class.__tablename__
        backup_file = f"{BACKUPS_FOLDER}/{table_name}_{uuid.uuid4()}.avro"

        session = Session()
        rows = session.query(model_class).all()
        session.close()

        schema = {
            "type": "record",
            "name": table_name,
            "fields": [{"name": col.name, "type": get_avro_type(col.type)} for col in model_class.__table__.columns],
        }

        # Convert datetime objects to milliseconds since epoch for Avro compatibility
        avro_rows = []
        for row in rows:
            row_dict = row.__dict__.copy() # Create a shallow copy of the row's attributes, to avoid inadvertently modifying the actual ORM-mapped row object
            if 'datetime' in row_dict and row_dict['datetime']: # Check if datetime field exists and is not null. Note, in some cases, it can be None even if it's not null in the DB
                row_dict['datetime'] = int(row_dict['datetime'].timestamp() * 1000)
            avro_rows.append(row_dict)

        with open(backup_file, "wb") as f:
            fastavro.writer(f, fastavro.parse_schema(schema), avro_rows)


        print(f"Backup of table '{table_name}' created at: {backup_file}")

        return backup_file

    except Exception as e:
        print(f"Error creating backup: {e}")
        
def restore_backup(backup_file, model_class):
    """Restores a table from an Avro backup file."""
    try:
        table_name = model_class.__tablename__

        with open(f"{BACKUPS_FOLDER}/{backup_file}", "rb") as f:
            avro_reader = fastavro.reader(f)
            data = list(avro_reader)  # Read all records into a list

        session = Session()
        metadata = MetaData()
        metadata.reflect(engine, only=[table_name])  # Use reflect
        table = metadata.tables[table_name] 

        # Insert data, handling DateTime conversion
        for record in data:
            if 'datetime' in record:
                try: #try parsing assuming no timezone info
                   record['datetime'] = datetime.fromtimestamp(record['datetime']/1000)
                except: #try parsing with timezone info
                    record['datetime'] = datetime.fromtimestamp(record['datetime'] / 1000, tz=timezone.utc)
            
            session.execute(table.insert().values(record))

        session.commit()
        session.close()

        print(f"Backup '{backup_file}' restored to table '{table_name}'")

    except Exception as e:
        print(f"Error restoring backup: {e}")



# Create backups for each table
# create_backup(Department)
# create_backup(Job)
# create_backup(HiredEmployee)


backup_filepath = "departments_2c19cbac-6315-41c0-9682-26a94684dabf.avro"
restore_backup(backup_filepath, Department)

backup_filepath = "hired_employees_f957ff44-8f1a-44e4-acf7-8e7a31dd5721.avro"
restore_backup(backup_filepath, HiredEmployee)

backup_filepath = "jobs_d86deeca-fe53-4397-bafc-11b230d7e1d4.avro"
restore_backup(backup_filepath, Job)