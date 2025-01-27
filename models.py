# models.py
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, text, exc
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URI

# create base class
Base = declarative_base()


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

class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(255))
    datetime = Column(DateTime)
    json_log_file = Column(String(255))

class Report(Base):
    __tablename__ = 'reports'
    id = Column(Integer, primary_key=True, autoincrement=True)
    report_name = Column(String(255))
    datetime = Column(DateTime)
    html = Column(String(255))
    csv = Column(String(255))
    images = Column(String(255))

def delete_all_tables():
    Base.metadata.drop_all(engine)

def delete_table(table_name):
    table = Base.metadata.tables[table_name]
    table.drop(engine)

# Create a database engine and session
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

# # Initialize databas
def initialize_db():
    Base.metadata.create_all(engine)