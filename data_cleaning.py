import pandas as pd
from data_extraction import DataExtractor
import tabula
import yaml
import sqlalchemy

class DataClean():
    def clean_user_data(self, df):
         df = DataExtractor().extract_rds_table('legacy_users')
         df['date_of_birth'] = pd.to_datetime(df['date_of_birth'],  infer_datetime_format=True, errors='coerce')
         df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
         #print(df['date_of_birth'])
         df.dropna(subset=['date_of_birth','join_date'], inplace=True)

         return df


        
   
