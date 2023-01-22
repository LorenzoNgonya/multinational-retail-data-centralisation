import yaml
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor():
    def read_rds_table (self,dbcon = DatabaseConnector()):
        eng = dbcon.init_db_engine()
        user_tb = dbcon.list_db_tables()
        users = pd.read_sql_table(user_tb[1],eng)
        print (users)

 
