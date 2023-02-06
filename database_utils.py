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
            print(data)    
        return data

db = DatabaseConnector()
db.read_db_creds
    