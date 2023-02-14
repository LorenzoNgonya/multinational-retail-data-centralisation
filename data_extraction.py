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

    def retrieve_pdf_data(self,link):
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        dfs = tabula.read_pdf(link, pages='all', multiple_tables=True)
        print(dfs)
        #return dfs


if __name__ == "__main__":
    ex = DataExtractor()
    ex.retrieve_pdf_data(link=any)
