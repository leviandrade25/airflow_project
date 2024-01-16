from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import *


user = "postgres"
password = "3523"
ip_connection = "192.168.0.142"
port = "5432"


main_db = "postgres"
main_schema = "postgres_1"
cloned_db = "new_db"
cloned_schema = "new_db_1"
table_name = "user"

# Configuração da conexão com o banco de dados
database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
cloned_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{cloned_db}'
master_engine = create_engine(database_uri)
cloned_engine = create_engine(cloned_uri)
# metadata_target = MetaData(schema=cloned_schema)
# metadata_origem = MetaData(bind=master_engine)


inspector = inspect(master_engine)
inspector_clone = inspect(cloned_engine)

main_table = 'user'
cloned_tables = inspector_clone.get_table_names(schema=cloned_schema)


columns_target = []
columns_target = inspector_clone.get_columns(main_table,schema=cloned_schema)
columns_target_name = [c['name'] for c in columns_target]
columns_info = inspector.get_columns(main_table, schema=main_schema)

for col in columns_info:
    if col['name'] not in columns_target_name:
        if main_table in cloned_tables:
            main_query = f""" ALTER TABLE {cloned_schema}.{main_table} ADD COLUMN {col['name']} {col['type']} """

            if col['nullable'] is not None:
                main_query += f""" NOT NULL DEFAULT {col['default']} """

            if col['comment'] is not None:
                main_query += f""" {col['comment']} """

            cloned_engine.execute(main_query)
            print(f"Completed adding columns in {cloned_db}.{cloned_schema}.{main_table}")
            
        else:
            print(f" Table {main_table} not found in {cloned_db}.{cloned_schema}")
            exit()
        
