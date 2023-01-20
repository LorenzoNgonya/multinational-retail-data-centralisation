import yaml

class DatabaseConnector():
    def read_db_creds():
        with open('db_creds.yaml', 'r') as stream:
            data_loaded = yaml.safe_load(stream)
            print(type(data_loaded))