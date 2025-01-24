""" create and restore backups of the database in AVRO format """
import traceback
import os
import uuid
import fastavro
from sqlalchemy import insert, MetaData, Column, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.sqltypes import Integer, String, DateTime, Boolean, Float, Numeric
from datetime import datetime, timezone


# re use sqlAlchemy engine from models.py
from models import engine, Job, Department, HiredEmployee, BackupFile

Session = sessionmaker(bind=engine)
metadata = MetaData()

BACKUPS_FOLDER = 'backups'

# table names to model class mappings
TABLES = {
    'jobs': Job,
    'departments': Department,
    'hired_employees': HiredEmployee,
}

# Ensure the backups folder exists
if not os.path.exists(BACKUPS_FOLDER):
    os.makedirs(BACKUPS_FOLDER)

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
    Creates an Avro backup of a SQLAlchemy model's table.
    :param table_name: Name of the table to back up
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
        return False, {
            "action": "create_backup",
            "status": "error",
            "error": str(e)
        }
    except Exception as e:
        return False, {
            "action": "create_backup",
            "status": "error",
            "error": str(e)
        }
        
def restore_backup(table_name, backup_file):
    """
    Restores a table from an Avro backup file.

    Args:
        table_name (str): The name of the table to restore.
        backup_file (str): The name of the Avro backup file.
    """
    try:
        # table_name = model_class.__tablename__

        with open(f"{BACKUPS_FOLDER}/{backup_file}", "rb") as f:
            avro_reader = fastavro.reader(f)
            data = list(avro_reader)  # Read all records into a list

        session = Session()
        metadata = MetaData()
        metadata.reflect(engine, only=[table_name])  # Use reflect
        table = metadata.tables[table_name] 

        # Insert data into the table
        for record in data:
            if 'datetime' in record and isinstance(record['datetime'], int): # Check data type and if it exists
                try:  # Try parsing without timezone info
                    record['datetime'] = datetime.fromtimestamp(record['datetime'] / 1000)
                except:  # Try parsing with timezone info
                    record['datetime'] = datetime.fromtimestamp(record['datetime'] / 1000, tz=timezone.utc)


            elif 'datetime' in record and isinstance(record['datetime'], datetime):
                pass #already a datetime - don't modify
            
            # print(f"{table.insert().values(record)}")
            print(record)

            session.execute(table.insert().values(record))

        session.commit()
        session.close()

        print(f"Backup file: '{backup_file}' restored to table '{table_name}'")

        # TODO: Add log in the database
        return True, {
            "action": "restore_backup",
            "status": "success",
            "backup_file": backup_file,
            "table_name": table_name
        }

    except SQLAlchemyError as e:
        return False, {
            "action": "restore_backup",
            "status": "SQLAlchemyError",
            "error": str(e)
        }
    except Exception as e:
        print(f"Error restoring backup: {e}")
        return False, {
            "action": "restore_backup",
            "status": "error",
            "error": str(e)
        }
