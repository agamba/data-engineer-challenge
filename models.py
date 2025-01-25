# models.py
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, text, exc
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URI

# create base class
Base = declarative_base()

# Create a database engine and session
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
# Evaluate when is better to start a new session
# session = Session()

# Initialize databas
def initialize_db():
    Base.metadata.create_all(engine)

# Define db tables
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

def delete_all_tables():
    Base.metadata.drop_all(engine)

def delete_table(table_name):
    table = Base.metadata.tables[table_name]
    table.drop(engine)

# TODO: Move this function to approppriate pipeline
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
