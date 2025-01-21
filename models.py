import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, text
from sqlalchemy.orm import sessionmaker, declarative_base

# Load environment variables from .env file
load_dotenv()

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
    transaction_id = Column(String(255))

class HiredEmployee(Base):
    __tablename__ = 'hired_employees'
    id = Column(Integer, primary_key=True, autoincrement=True)
    employee_id = Column(Integer)
    name = Column(String(255))  
    datetime = Column(DateTime)
    datetime_str = Column(String(255))  # added to store the raw datetime string
    department_id = Column(Integer)
    job_id = Column(Integer)

# logging tables used for storing invalid data 
# having a separate table for each base table seems more efficient for later analisys
# TODO: consier if we need to log the original csv line of data that failed 
#####################

class BackupFile(Base):
    __tablename__ = 'backups_files'
    id = Column(Integer, primary_key=True, autoincrement=False)
    table_name = Column(String(255))
    datetime = Column(DateTime)
    avro_file = Column(String(255))

class DepartmentFailed(Base):
    __tablename__ = 'departments_failed'
    row_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String(255))
    department = Column(String(255))
    errors = Column(String(255))

class JobFailed(Base):
    __tablename__ = 'jobs_failed'
    row_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String(255))
    job = Column(String(255))
    errors = Column(String(255))

class HiredEmployeeFailed(Base):
    __tablename__ = 'hired_employees_failed'
    # id = Column(Integer, primary_key=True, autoincrement=False)
    row_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String(255))
    name = Column(String(255))
    datetime = Column(String(255))
    department_id = Column(String(255))
    job_id = Column(String(255))
    errors = Column(String(255))

# Create tables if do not exist
Base.metadata.create_all(engine)

def drop_all_tables():
    Base.metadata.drop_all(engine)

# bind session to engine
Session = sessionmaker(bind=engine)
