from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor():
    def read_rds_table(self, database, table='legacy_users'):
        eng = dbcon.init_db_engine()
    user_tb = dbcon.list_db_tables()
    users = pd.read_sql_table(user_tb[1],eng)
    return users