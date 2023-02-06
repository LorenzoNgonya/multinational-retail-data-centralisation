from database_utils import DatabaseConnector
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
class DataExtractor:
    def read_rds_table(self, db_con = DatabaseConnector()):
     engine1 = db_con.init_db_engine()
     user_tb = db_con.list_db_tables()
     user = pd.read_sql_table(user_tb[1],engine1)
     print(user.head)
     return user

if __name__ == "__main__":
    ex = DataExtractor()
    ex.read_rds_table()
