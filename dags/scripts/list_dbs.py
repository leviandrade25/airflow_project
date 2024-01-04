from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json

# read json with credentials
with open('dags/credentials/credentials.json','r') as file_vault:
    credentials = json.load(file_vault)
    file_vault.close()


# set the credentials
user = credentials['user_name']
password = credentials['password']
ip_connection = credentials['ip_connection']
port = credentials['port']
db = credentials['db']


# Function to list existing dbs
def list_dbs_name():
    file_path = 'data_out/db_names.json'
    dict_to_json = {}
    iterator = 1
    engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')
    with engine.connect() as conn:
        query = text("SELECT datname FROM pg_database")
        result = conn.execute(query)
        db_names = [row['datname'] for row in result]
        for name in db_names:
            if not name.startswith("template"):
                dict_to_json[f'db_{iterator}'] = name
                iterator += 1

        # Writing the result in a json file
        with open(file_path, 'w') as f:
            json.dump(dict_to_json, f)
    

