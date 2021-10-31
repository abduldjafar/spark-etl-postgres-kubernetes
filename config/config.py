from os import environ
import json

class Config(object):
    def __init__(self):
        self.postgres_password = environ["POSTGRES_PASSWORD"] if "POSTGRES_PASSWORD" in environ else ""
        self.postgres_host = environ["POSTGRES_HOST"] if "POSTGRES_HOST" in environ else ""
        self.postgres_port = environ["POSTGRES_PORT"] if "POSTGRES_PORT" in environ else ""
        self.postgres_user = environ["POSTGRES_USER"] if "POSTGRES_USER" in environ else ""
        self.data_sources = environ["DATA_SOURCES"] if "DATA_SOURCES" in environ else "data/transaction.csv"
        self.database = environ["DATABASE_DESTINATION"] if "DATABASE_DESTINATION" in environ else ""
        self.table = environ["TABLE_DESTINATION"] if "TABLE_DESTINATION" in environ else ""
    
    def get_postgres_password(self):
        return self.postgres_password
    
    def get_postgres_host(self):
        return self.postgres_host
    
    def get_postgres_port(self):
        return self.postgres_port
    
    def get_postgres_user(self):
        return self.postgres_user
    
    def get_data_sources(self):
        return self.data_sources
    
    def get_database(self):
        return self.database
    
    def get_table(self):
        return self.table
    
    def get_config_from_json(self,file_name):
        with open(file_name) as json_file:
            data = json.load(json_file)
        return data