import pandas as pd
from database_utils import DatabaseConnector
class DataExtractor():
    def extract_rds_table(self, table_name):
         creds = DatabaseConnector.read_db_creds(creds) 
         DatabaseConnector.read_db_creds(self, creds)
         engine = DatabaseConnector.init_db_engine(self, creds)
         #df = pd.read_sql_table(table_name,con = engine)
         print (engine)
         #return df

    #def retrieve_pdf_data(self, link):

         #Read remote pdf into list of DataFrame
         #pdf_data = tabula.read_pdf(link, pages='all')#returns a list of data frames
        
         #return pdf_data
        
extraction_instance = DataExtractor()
extraction_instance.extract_rds_table(table_name='L')