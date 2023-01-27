import pandas as pd


class DataClean():
    def clean_user_data(df):
        import data_extraction
        df = data_extraction.DataExtractor
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'],  infer_datetime_format=True, errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
        print(df['date_of_birth'])
        df.dropna(subset=['date_of_birth','join_date'], inplace=True)

        return df


        
   
