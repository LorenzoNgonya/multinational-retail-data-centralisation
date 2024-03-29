import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import numpy as np
import re 
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

    def clean_card_table(self):
        table = pd.read_csv("card_details.csv")

        # Check card providers are valid and corresponding number of digits in the card number is correct
        providers = {'VISA 13 digit': [13], 'VISA 16 digit': [16], 'VISA 19 digit': [19], 'JCB 15 digit': [15], 'JCB 16 digit': [16], 'Discover': [16], 'Mastercard': [16], 'Maestro': range(12, 21), 'American Express': [15], 'Diners Club / Carte Blanche': [14]}
        table['card_provider'] = table['card_provider'].apply(lambda x: x if x in providers else np.nan)
        table.dropna(inplace=True, subset = ['card_provider']) 
        # removes '?' from card_number
        table['card_number'] = table['card_number'].apply(lambda x: str(x) )
        table['card_number'] = table['card_number'].apply(lambda x: x.strip('?') if '?' in x else x)
        #converts the date strings to ISO date format
        table['date_payment_confirmed'] = pd.to_datetime(table['date_payment_confirmed'])
        table['date_payment_confirmed'].dt.strftime('%Y-%m-%d')
        table['card_number'] = table['card_number'].str.strip('?')
        table['expiry_date'] = pd.to_datetime(table['expiry_date'],format = '%m/%y')

        card_number_validated = table['card_number'].str.fullmatch("[0-9]+")
        table = table[card_number_validated]
        table['card_number'] = table['card_number'].astype('int')

        table.dropna(inplace=True, subset=['card_number'])
        table.dropna(axis=0, how='all', inplace=True)
        table.dropna(axis=1, how='all', inplace=True)

        table = table.drop_duplicates(keep='first')
        duplicated_rows = table.duplicated().sum()
        if duplicated_rows == 0:
            print(f'{duplicated_rows} duplicate rows found')
         
        return table

    def clean_table(self):
        table_instance = DataExtractor()
        table = table_instance.retrieve_stores_data()
        print (table)

        #Clean column with address
        table.address = table.address.apply(lambda x: np.nan if len(str(x).split())==1 else x)

       # Clean column with opening date
        table.opening_date = table.opening_date.apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce')).dt.date

        # Clean up continent column
        continents = ['Europe', 'America']
        table.continent = table.continent.apply(lambda x: x if x in continents else('Europe' if 'Europe' in str(x) else('America' if 'America' in str(x) else np.nan)))

        # Clean up country code column
        country_codes = ['GB', 'US', 'DE']
        table.country_code = table.country_code.apply(lambda x: x if x in country_codes else np.nan)

        # Clean up store type column
        store_types = ['Local', 'Super Store', 'Mall Kiosk', 'Outlet', 'Web Portal']
        table.store_type = table.store_type.apply(lambda x: x if x in store_types else np.nan)

        # Clean column with locality    
        table.locality.replace('[\d]', np.nan, regex=True, inplace=True)

        # Check store_code against specific format
        table.store_code = table.store_code.apply(lambda x: x if re.match('^[A-Z]{2,3}-[A-Z0-9]{8}$', str(x)) else np.nan)
      
        # Clean up staff numbers, longitude and latitide columns
        table['staff_numbers'] = table['staff_numbers'].str.replace('[a-zA-Z]', '')
        table[['staff_numbers', 'longitude', 'latitude']] = table[['staff_numbers', 'longitude', 'latitude']].apply(lambda x: round(pd.to_numeric(x, errors = 'coerce'), 1))
        table.dropna(subset = ['staff_numbers'], inplace=True)
        table = table.astype({'staff_numbers': 'int64'})
        table['longitude'] = table['longitude'].astype('float')
        table['latitude'] = table['latitude'].astype('float')

        # Dropping nan values, redundant index column, lat column which contains little valid data and resetting index
        table.drop(['lat'], axis = 1, inplace=True)
        table.dropna(inplace = True, subset = ['store_code', 'store_type'])
        table.reset_index(drop = True, inplace = True)
        table.drop_duplicates(inplace = True)
        table.reset_index(drop = True, inplace=True)
        return table

    def convert_product_weights(self):
        
        products = DataExtractor().extract_from_s3()
        products.weight =  products.weight.apply(lambda x: str(x)[: (str(x).index('g')+1)] if 'g' in str(x) else x)

        products.weight =  products.weight.apply(lambda x:
        pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'kg' else
        (0.001*pd.to_numeric(str(x).strip('g').split(' ')[0]) * pd.to_numeric(str(x).strip('g').split(' ')[-1]) if ('x' in str(x) and str(x)[-1] == 'g') else 
        (0.001*pd.to_numeric(str(x)[:-1], errors = 'coerce') if str(x)[-1] == 'g' else
        (0.001*pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'ml' else
        (0.0284*pd.to_numeric(str(x)[:-2], errors = 'coerce') if str(x)[-2:] == 'oz' else 
        np.nan)))))

        products.weight =  products.weight.apply(lambda x: round(x, 2))

        return products
        
    def clean_products_data(self, df):
         weight_table = df.copy()

        # Verify category column against valid categories
         categories = ['homeware', 'toys-and-games', 'food-and-drink', 'pets', 'sports-and-leisure', 'health-and-beauty', 'diy']
         weight_table.category =  weight_table.category.apply(lambda x: x if x in categories else np.nan)

        # Verify removed column against valid availability categories
         availability = ['Still_avaliable', 'Removed']
         weight_table.removed = weight_table.removed.apply(lambda x: x if x in availability else np.nan).replace('Still_avaliable', 'Still_available')

        # Convert date added column entries to datetime date type
         weight_table.date_added = weight_table.date_added.apply(lambda x: pd.to_datetime(x, format = '%Y-%m-%d' , errors = 'coerce'))

        # Conveert price column to numeric data
         weight_table.product_price = weight_table.product_price.apply(lambda x: round(pd.to_numeric(str(x).strip('£'), errors='coerce'), 2))

        # Verify that uuid column entires follow a specific format
         weight_table.uuid = weight_table.uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Verify that EAN column entries follow a specific format
         weight_table.rename(columns = {'EAN': 'ean'}, inplace=True)
         weight_table.ean = weight_table.ean.apply(lambda x: str(x) if re.match('^[0-9]{12,13}$', str(x)) else np.nan)
         
        # Verify that product_code column entries follow a specific format
         weight_table.product_code = weight_table.product_code.apply(lambda x: x if re.match('^[a-zA-Z0-9]{2}-[0-9]{6,7}[a-zA-Z]$', str(x)) else np.nan)

        # Delete unnecessary columns, nan entries and reset index (no duplicates confirmed)
         weight_table.drop('Unnamed: 0', axis=1, inplace=True)
         weight_table.dropna(inplace = True, subset=['product_code'])
         weight_table.drop_duplicates(inplace=True)
         weight_table.reset_index(drop=True, inplace=True)

         return weight_table

    def clean_orders_data(self):
        table = DataExtractor.read_rds_table('orders_table')
        #print(table.columns)
        table = table.drop(['first_name', 'last_name', '1', 'level_0', 'index'], axis=1)
        table = table.to_string('clean_orders_data.txt')
        print (table.info)

    def clean_date_details(self):

         date_table = DataExtractor.extract_from_s3_json(self)

         # allows null entries to recognised by pandas or numpy
         date_table.replace('NULL', np.nan, inplace=True)

         # Convert timestamp column to datatime.time format
         date_table['timestamp'] = date_table['timestamp'].apply(lambda x: pd.to_datetime(x, format= '%H:%M:%S', errors = 'coerce')).dt.time

         # Clean up months column by removing invalid entries
         months = [*range(1,13)]
         date_table.month = date_table.month.apply(lambda x: x if pd.to_numeric(x, errors='coerce') in months else np.nan)

        # Clean up days column by removing invalid entries
         days = [*range(1,32)]
         date_table.day = date_table.day.apply(lambda x: x if pd.to_numeric(x, errors='coerce') in days else np.nan)

        # Clean up years column by removing invalid entries
         years = [*range(1980,2023)]
         date_table.year = date_table.year.apply(lambda x: x if pd.to_numeric(x, errors='coerce') in years else np.nan)

        # Clean up user uuid column by removing entries of invalid length
         date_table.date_uuid = date_table.date_uuid.apply(lambda x: x if re.match('^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', str(x)) else np.nan)

        # Clean up periods column by removing invalid entries
         periods = ['Morning', 'Midday', 'Evening', 'Late_Hours']
         date_table.time_period = date_table.time_period.apply(lambda x: x if x in periods else np.nan)

         # Remove nan values, duplicates and reset index
         date_table.dropna(inplace = True, subset=['date_uuid']) # currently keep all rows to avoid over-cleaning
         date_table.drop_duplicates(inplace=True)
         date_table.reset_index(drop=True, inplace=True)

         return date_table

         

if __name__ == "__main__":
     
    clean = DataCleaning()
    table = clean.clean_card_table()
    #table.to_string('card_details.txt')
    db_conn = DatabaseConnector()
    db_conn.upload_to_db('dim_card_details', table)
    #weight_db=clean.clean_products_data(table)
    #weight_db.to_string('clean_table.txt')