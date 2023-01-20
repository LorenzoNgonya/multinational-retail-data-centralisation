import yaml
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DatabaseConnector():
    def read_db_creds(self):
        with open("db_creds.yaml", "r") as creds:
            data = yaml.safe_load(creds)
        
        return data

    def init_db_engine(self):
        data = self.read_db_creds()
        engine = create_engine(f"{data['DATABASE_TYPE']}+{data['DBAPI']}://{data['RDS_USER']}:{data['RDS_PASSWORD']}@{data['RDS_HOST']}:{data['RDS_PORT']}/{data['RDS_DATABASE']}")
      
        return engine

    
    def  list_db_tables (self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        for table in inspector.get_table_names():
            print(table)