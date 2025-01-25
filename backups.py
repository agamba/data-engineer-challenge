# backup.py
""" Functmions for create and restore backups in AVRO format """
import traceback
import os
import uuid
import fastavro
from sqlalchemy import insert, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.sqltypes import Integer, String, DateTime, Boolean, Float, Numeric
from datetime import datetime, timezone

from config import BACKUPS_FOLDER
# re use sqlAlchemy engine from models.py
from models import engine, Job, Department, HiredEmployee, BackupFile

Session = sessionmaker(bind=engine)
metadata = MetaData()


# table names to model class mappings
# TODO: need to optimize this to be dynamic
TABLES = {
    'jobs': Job,
    'departments': Department,
    'hired_employees': HiredEmployee,
}

def get_avro_type(sql_type):
    """
    Map SQLAlchemy types to Avro types.
    :param sql_type: SQLAlchemy type
    :return: Avro type
    """
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

def create_backup(table_name):
    """
    Creates an Avro backup of a sql table.
    :param table_name: Name of the table to back up
    Returns:
        bool: True if the backup was restored successfully, False otherwise.
        dict: A dictionary containing the action, status, and any error messages.
    """
    try:
        model_class = TABLES[table_name]
        backup_file = f"{table_name}___{uuid.uuid4()}.avro"
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

        with open(f"{BACKUPS_FOLDER}/{backup_file}", "wb") as f:
            fastavro.writer(f, fastavro.parse_schema(schema), avro_rows)

        print(f"Backup of table '{table_name}' created at: {backup_file}")
        backup_data = {
            'table_name': table_name,
            'datetime': datetime.now(),
            'avro_file': backup_file
        }
        
        # log created backup action to database
        stmt = insert(BackupFile).values(**backup_data)

        # Execute the insert statement within a session
        with Session() as session:
            session.execute(stmt)
            session.commit()
            session.close()
            
        return True, {
            "action": "create_backup",
            "status": "success",
            "file_name": backup_file,
        }
    except SQLAlchemyError as e:
        # print(f"Error during commit: {e}")
        session.rollback()
        return False, {
            "action": "create_backup",
            "status": "error",
            "error": str(e)
        }
    except Exception as e:
        session.rollback()
        return False, {
            "action": "create_backup",
            "status": "error",
            "error": str(e)
        }
        
def restore_backup(table_name, backup_file):
    """
    Restores a sql table from an Avro backup file.

    Args:
        table_name (str): The name of the table to restore.
        backup_file (str): The name of the Avro backup file.
    Returns:
        bool: True if the backup was restored successfully, False otherwise.
        dict: A dictionary containing the action, status, and any error messages.
    """
    try:
        with open(f"{BACKUPS_FOLDER}/{backup_file}", "rb") as f:
            avro_reader = fastavro.reader(f)
            data = list(avro_reader)  # Read all records into a list

        session = Session()
        metadata = MetaData()
        metadata.reflect(engine, only=[table_name])  # Use reflect
        table = metadata.tables[table_name] 

        # Insert data into the table
        for record in data:
            # Convert datetime format for compatibility
            if 'datetime' in record and isinstance(record['datetime'], int): # Check data type and if it exists
                try:  # Try parsing without timezone info
                    record['datetime'] = datetime.fromtimestamp(record['datetime'] / 1000)
                except:  # Try parsing with timezone info
                    record['datetime'] = datetime.fromtimestamp(record['datetime'] / 1000, tz=timezone.utc)
            elif 'datetime' in record and isinstance(record['datetime'], datetime):
                pass #already a datetime - don't modify
            session.execute(table.insert().values(record))
        session.commit()
        session.close()

        print(f"Backup file: '{backup_file}' restored to table '{table_name}'")

        return True, {
            "action": "restore_backup",
            "status": "success",
            "backup_file": backup_file,
            "table_name": table_name
        }

    except SQLAlchemyError as e:
        session.rollback()
        return False, {
            "action": "restore_backup",
            "status": "SQLAlchemyError",
            "error": str(e)
        }
    except Exception as e:
        session.rollback()
        print(f"Error restoring backup: {e}")
        return False, {
            "action": "restore_backup",
            "status": "error",
            "error": str(e)
        }
