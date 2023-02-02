import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import psycopg2





class DatabaseConnector:
    def read_db_creds(self,creds):
        creds = 'db_creds.yaml'
        with open(creds, "r") as file:
            data = yaml.safe_load(file)
        print (data)
        return data

    def init_db_engine(self, creds):
        data = self.read_db_creds(creds) 
        
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = data['RDS_HOST']
        USER = data['RDS_USER']
        PASSWORD = data['RDS_PASSWORD']
        DATABASE = data['RDS_DATABASE']
        PORT = data['RDS_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    
    def  list_db_tables (self,engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        print (table_names)
        return table_names
      
     
    def upload_to_db (self, data_frame):
     DATABASE_TYPE = 'postgresql'  
     DBAPI = 'psycopg2'
     HOST = 'localhost'
     PASSWORD = 'Lorenzo97='
     USER = 'postgres'
     DATABASE = 'Sales_Data'
     PORT = 5432
     engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
     data_frame.to_sql('dim_users', engine, if_exists='replace')
     
#conn_instance = DatabaseConnector()
#extraction_instance = data_extraction.DataExtractor()
#clean_instance = data_cleaning.DataClean()
