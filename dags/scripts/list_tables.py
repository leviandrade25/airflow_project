from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
import json


file_path = 'data_out/list_tables.json'

with open('dags/credentials/credentials.json','r') as file_vault:
    credentials = json.load(file_vault)
    file_vault.close()

user = credentials['user_name']
password = credentials['password']
ip_connection = credentials['ip_connection']
port = credentials['port']
db = credentials['db']

def list_tables():
    dict_json = {}
    list_dict = []
    with open('data_out/db_names.json','r') as db_names:
        db_list_names_json = json.load(db_names)
        db_list_names = db_list_names_json.values()


    for db in db_list_names:

        engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')

        with open('data_out/list_schemas.json','r') as schema_names:
            db_schema_names_json = json.load(schema_names)
            db_schema_names = db_schema_names_json[db]
   

        insp = reflection.Inspector.from_engine(engine)

        print(f"db :{db}".center(150,"*"))
        for schema in db_schema_names:
            list_of_tables = []
            print(f"schema : {schema}")

            # Nome do schema desejado
            nome_do_schema = schema

            # Listar tabelas no schema especificado
            tabelas = insp.get_table_names(schema=nome_do_schema)

            for tabela in tabelas:
                print(f" Localizada tabela {tabela} no schema {schema} no bd {db}")
                list_of_tables.append(tabela)

            list_dict.append([f"{db}",f"{schema}",[c for c in list_of_tables]])
            print(f"Adicionadas Tables do schema : {schema}")
            print(list_dict)

       
    print(list_dict)        
    for chave_principal, chave_aninhada, valor_aninhado in list_dict:
        if chave_principal not in dict_json:
            dict_json[chave_principal] = {}
        dict_json[chave_principal][chave_aninhada] = valor_aninhado

    
    with open(file_path, 'w') as f:
            json.dump(dict_json, f)