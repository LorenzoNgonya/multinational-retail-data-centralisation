import yaml
from sqlalchemy import create_engine
from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor():
    def read_rds_table (self,dbcon = DatabaseConnector()):
        engine = dbcon.init_db_engine()
        tables = dbcon.list_db_tables()
        user_table_df= pd.read_sql_table(tables[1],engine)
        return user_table_df

 
