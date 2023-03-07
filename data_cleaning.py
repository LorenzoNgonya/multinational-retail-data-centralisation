import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

class DataCleaning():
    def __init__(self) -> None:
        pass

    def clean_user_data(self):
        table = DataExtractor.read_rds_table('legacy_users')
        print(table.tail(10))
        nan_values = table[table.isnull().any(axis=1)]
        nan_num = len(nan_values)
        print(f"There are Nan {nan_num} rows")
        if nan_num != 0: 
            print("Here are the Nan rows")
            print(nan_values)

        null_values = table[table['first_name'].str.contains("NULL")]
        null_num = len(null_values)
        print(f"There are NULL {null_num} rows")
        if null_num != 0: 
            print("Here are the NULL rows:")
            print(null_values)    
        table = table.drop(null_values.index)    

        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'], errors='coerce')
        table = table.dropna(subset=['date_of_birth']) 
        table['join_date'] = pd.to_datetime(table['join_date'], errors='coerce')
        table = table.dropna(subset=['join_date'])
        # Check datetime formate
        pd.to_datetime(table['join_date'], format='%Y-%M-%D', errors='raise')

        print(table['country'].unique())
        print(table)
        
        

    def clean_card_data(self):
        file = DataExtractor().retrieve_pdf_data()
        table = pd.DataFrame()
        for item in file:
            table = pd.concat([table, item],ignore_index=True)
        print (table)
        table.dropna(axis=0, how='all', inplace=True)
        table.dropna(axis=1, how='all', inplace=True)
        table = table.drop_duplicates(keep='first')
        #duplicated_rows = table.duplicated().sum()
        #if duplicated_rows == 0:
            #print(f'{duplicated_rows} duplicate rows found')


    def clean_store_data(self):
        store_data_instance = DataExtractor()
        table = store_data_instance.retrieve_stores_data()
        print (table)
        # Clean up staff numbers, longitude and latitide columns
        table[['staff_numbers', 'longitude', 'latitude']] = table[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: round(pd.to_numeric(x, errors = 'coerce'), 1))
        table.dropna(subset = ['staff_numbers'], inplace=True)
        table = table.astype({'staff_numbers': 'int64'})

        # Dropping nan values, redundant index column, lat column which contains little valid data and resetting index
        table.drop(['index', 'lat'], axis = 1, inplace=True)
        table.dropna(inplace = True, subset = ['store_code', 'store_type'])
        table.drop_duplicates(inplace = True)
        table.reset_index(drop = True, inplace=True)
        
        db_conn = DatabaseConnector()
        db_conn.upload_to_db('dim_store_details', table)

if __name__ == "__main__":
    clean = DataCleaning()
    clean.clean_store_data()
    
    
    