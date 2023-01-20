import yaml

class DatabaseConnector():
    def read_db_creds():
        with open('../db_creds.yaml', 'r') as f:
            data = yaml.safe_load(f)
            print(data)