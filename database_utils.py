import yaml
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
#Step 4
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


if __name__ == "__main__":
    db = DatabaseConnector()
    db.read_db_creds()
    db.init_db_engine()
    tables = db.list_db_tables()
    print(tables)