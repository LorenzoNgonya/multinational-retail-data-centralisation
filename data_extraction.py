from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor:
    def read_rds_table(self, db_con = DatabaseConnector()):
     engine1 = db_con.init_db_engine()
     user_tb = db_con.list_db_tables()
     user = pd.read_sql_table(user_tb[1],engine1)
     print(user.head)
     return user

    def retrieve_pdf_data(self, address='card_details.pdf'):
        dfs = tabula.read_pdf(address, pages='all')
        return dfs


if __name__ == "__main__":
    ex = DataExtractor()
    ex.retrieve_pdf_data()
