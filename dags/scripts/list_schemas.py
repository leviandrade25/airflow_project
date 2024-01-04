from sqlalchemy import create_engine
from sqlalchemy.sql import text
import json


file_path = 'data_out/list_schemas.json'

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



# Function to list existing schemas
def list_schemas():
    list_schemas = []
    dict_schemas = {}

    # Reading json with db names
    with open('data_out/db_names.json','r') as db_names:
        db_list_names_json = json.load(db_names)
        db_list_names = db_list_names_json.values()
       
    # Connecting to each db and taking existing schemas
    for db in db_list_names:
        print(f"CONNETING TO db: {db}")
        engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')

        with engine.connect() as conn:
            # Consulting Schema list query
            query = text("SELECT schema_name FROM information_schema.schemata")

            # Execute query
            result = conn.execute(query)

            # Saving the schema name information
        for row in result:
            if row['schema_name'] not in ("public","information_schema","pg_catalog","pg_toast"):
                list_schemas.append(row['schema_name'])
                print(f"Encontrato schema: {row['schema_name']}")
                
        dict_schemas[db] = [c for c in list_schemas]
        list_schemas = []

    # Writing the result to a json file
    with open(file_path, 'w') as f:
            json.dump(dict_schemas, f)
