import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, text, exc
from sqlalchemy.orm import sessionmaker, declarative_base

# Load environment variables from .env file
load_dotenv()

columns_names_by_table = {
    "departments": ['id', 'department'],
    "jobs": ['id', 'job'],
    "hired_employees": ['id', 'name', 'datetime', 'department_id', 'job_id']
}

"""Create a database engine using environment variables."""
host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")
port = os.getenv("MYSQL_PORT")

# Database connection
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# create base class
Base = declarative_base()
    
# base tables
#####################
class Department(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True, autoincrement=False)
    department = Column(String(255))

class Job(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True, autoincrement=False)
    job = Column(String(255))

class HiredEmployee(Base):
    __tablename__ = 'hired_employees'
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(255))  
    datetime = Column(DateTime)
    datetime_str = Column(String(255))  # added to store the raw datetime string
    department_id = Column(Integer)
    job_id = Column(Integer)

class BackupFile(Base):
    __tablename__ = 'backups_files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(255))
    datetime = Column(DateTime)
    avro_file = Column(String(255))

# Create tables if do not exist
Base.metadata.create_all(engine)

# bind session to engine
Session = sessionmaker(bind=engine)

def delete_all_tables():
    Base.metadata.drop_all(engine)

def delete_table(table_name):
    table = Base.metadata.tables[table_name]
    table.drop(engine)

def get_backup_files():
    session = Session()
    backup_files = session.query(BackupFile).all()
    backups = []
    for backup in backup_files:
        record = {
            "table_name": backup.table_name,
            "datetime": backup.datetime,
            "avro_file": backup.avro_file

        }
        backups.append(record)
    session.close()
    return backups

