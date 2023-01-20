import yaml

class DatabaseConnector():
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as creds:
            data = yaml.load(creds)
            
        return (data) 
        