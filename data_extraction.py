from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import json
import boto3

class DataExtractor:
    def read_rds_table(self, db_con = DatabaseConnector()):
     engine1 = db_con.init_db_engine()
     user_tb = db_con.list_db_tables()
     user = pd.read_sql_table(user_tb[2],engine1)
     print(user.head)
     return user

    def retrieve_pdf_data(self, address='card_details.pdf'):
        dfs = tabula.read_pdf(address, pages='all')
        return dfs

    def list_number_of_stores(self,):
        dictionary = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores',headers=dictionary)
        store_number = stores.json()
        return store_number


    def retrieve_stores_data(self, endpoint='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'):
        dict_list = []
        dictionary = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        number_store = self.list_number_of_stores()
        for num in range (451):
            if num%10 == 0:
                print(num,"/",number_store)
            dict = requests.get(f'{endpoint}/{num}',headers=dictionary)
            content = dict.json()
            dict_list.append(content)
        
        dict = pd.DataFrame.from_dict(dict_list)
        return dict

    def extract_from_s3(self):
        s3_client = boto3.client('s3')
        bucket = 'data-handling-public'
        object = 'products.csv'
        file = 'products.csv'
        s3_client.download_file(bucket,object,file)
        table = pd.read_csv('./products.csv')
        return table

    def extract_from_s3_json(self):
        #https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json
        s3_client = boto3.client('s3')
        bucket = 'data-handling-public'
        object = 'date_details.json'
        file = 'date_details.json'
        s3_client.download_file(bucket,object,file)
        table = pd.read_json('./date_details.json')
        return table



if __name__ == "__main__":
    ex = DataExtractor()
    ex.extract_from_s3_json()
