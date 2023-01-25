import yaml

from database_utils import DatabaseConnector
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
import tabula

class DataExtractor():
    def extract_rds_table(self, table_name):
         engine = DatabaseConnector().init_db_engine()
         table = pd.read_sql_table(table_name, engine, index_col='index')
         df = pd.DataFrame(table)
         return df

    #def retrieve_pdf_data(self, link):

         # Read remote pdf into list of DataFrame
         #pdf_data = tabula.read_pdf(link, pages='all')#returns a list of data frames
        
         #return pdf_data
        