import pandas as pd

class DataExtractor():
    def extract_rds_table(self, table_name):
         import database_utils
         engine = database_utils.DatabaseConnector.init_db_engine
         df = pd.read_sql_table(engine, table_name)
         return df

    #def retrieve_pdf_data(self, link):

         #Read remote pdf into list of DataFrame
         pdf_data = tabula.read_pdf(link, pages='all')#returns a list of data frames
        
         return pdf_data
        
