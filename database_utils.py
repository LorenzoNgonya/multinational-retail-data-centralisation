import yaml

class DatabaseConnector():
    def read_db_creds(self):
        with open('db_creds.yaml') as creds:
            data = yaml.safe_load(creds)
            
        print (data)


        