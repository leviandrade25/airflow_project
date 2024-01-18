from sqlalchemy import create_engine, MetaData, Table, Column, DDL, event
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import declarative_base


user = "postgres"
password = "3523"
ip_connection = "192.168.0.142"
port = "5432"


main_db = "postgres"
main_schema = "postgres_1"
cloned_db = "new_db"
cloned_schema = "new_db_1"
table_name = "user"

Base = declarative_base()

try:
    # Conecction to main db
    database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
    master_engine = create_engine(database_uri)
    inspector = inspect(master_engine)
except Exception as e:
    print(e)

try:
    # Table to be compared
    main_table = 'user'
    columns_source = inspector.get_columns(main_table, schema=main_schema)
except Exception as e:
    print(e)


try:
    # Conecction to cloned db
    cloned_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{cloned_db}'
    cloned_engine = create_engine(cloned_uri)
    inspector_clone = inspect(cloned_engine)
except Exception as e:
    print(e)

try:
    # Getting table's name
    cloned_tables = inspector_clone.get_table_names(schema=cloned_schema)
    #Columns name
    columns_target_name = [c['name'] for c in inspector_clone.get_columns(main_table,schema=cloned_schema)]
except Exception as e:
    print(e)

if main_table in cloned_tables:
    for col in columns_source:
        if col['name'] not in columns_target_name:
            main_query = f""" ALTER TABLE {cloned_schema}.{main_table} ADD COLUMN {col['name']} {col['type']} """
            print(f"----- comment {col['comment']}")
            print(f"----- nullable {col['nullable']}")
            print(f"----- type {col['type']}")

            if col['nullable'] is False:
                main_query += f""" NOT NULL DEFAULT {col['default']} """

            if col['comment'] is not None:
                main_query += f""" {col['comment']} """

            event.listen(Base.metadata, 'before_create', DDL(main_query).execute_if(dialect='postgresql'))
            Base.metadata.create_all(cloned_engine)
            print(f"Completed adding columns in {cloned_db}.{cloned_schema}.{main_table}")
            
else:
    print(f" Table {main_table} not found in {cloned_db}.{cloned_schema}")
    exit()
        
