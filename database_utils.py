import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DatabaseConnector:
    def __init__(self):
        pass
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            data = yaml.safe_load(f)
            #print(data)    
        return data

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f"{'postgresql'}+{creds['DBAPI']}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine 

    def list_db_tables(self):
        eng = self.init_db_engine()
        eng.connect()
        inspector = inspect(eng)
        return inspector.get_table_names()     

    def local_connection(self):
        newconn_str = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user='postgres',
            password='Lorenzo97=',
            host='localhost',
            port='5432',
            database='Sales_Data'
        )
        engine = create_engine(newconn_str)
        return engine


    def upload_to_db(self,table_name, df, connection_type = 'local'):
        if connection_type == 'local':
            engine = self.local_connection()
        else:
            engine = self.init_db_engine()

        df.to_sql(table_name, engine, if_exists = 'append')
        


if __name__ == "__main__":
    db = DatabaseConnector()
    db.read_db_creds()
    db.init_db_engine()
    tables = db.list_db_tables()
    print(tables)
   