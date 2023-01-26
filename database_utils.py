import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from data_cleaning import DataClean
from data_extraction import DataExtractor
import pandas as pd



class DatabaseConnector():
    def read_db_creds(self):
        with open("db_creds.yaml", "r") as creds:
            data = yaml.safe_load(creds)
        
        return data

    def init_db_engine(self):
        data = self.read_db_creds()
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['RDS_HOST']
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE = data['RDS_DATABASE']
        PORT = data['RDS_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    
    def  list_db_tables (self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
         
     
    def upload_to_db(self, data_frame, table_name):
     
     
     DATABASE_TYPE = 'postgresql'  
     DBAPI = 'psycopg2'
     HOST = 'localhost'
     PASSWORD = 'Lorenzo97='
     USER = 'postgres'
     DATABASE = 'Sales_Data'
     PORT = 5432
     engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
     return data_frame.to_sql(table_name, engine, if_exists='replace')


