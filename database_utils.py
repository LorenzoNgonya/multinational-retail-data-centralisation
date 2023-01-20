import yaml

class DatabaseConnector():
    def read_db_creds(self):
        with open('cleardb_creds.yaml', 'r') as creds:
            data = yaml.safe_load(creds)
            
        return (data) 
        