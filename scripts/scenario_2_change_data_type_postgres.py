from sqlalchemy import create_engine, MetaData, Table, Column, DDL, event
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import declarative_base
import re




def remove_parenteses(texto):
    return re.sub(r'\(.*\)', '', texto)

Base = declarative_base()


user = "postgres"
password = "3523"
ip_connection = "192.168.0.142"
port = "5432"


main_db = "postgres"
main_schema = "postgres_1"
cloned_db = "new_db"
cloned_schema = "new_db_1"

try:
# Conecction to main db
    database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
    master_engine = create_engine(database_uri)
    inspector = inspect(master_engine)
except Exception as e:
    print(e)
    exit()

try:
    # Table to be compared
    main_table_name = 'user'
    # Main Columns type informations
    columns_source_type = {c['name']: c['type'] for c in inspector.get_columns(main_table_name, schema=main_schema)}
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
    # Cloned Columns type informations
    columns_target_type = {c['name']: c['type'] for c in inspector_clone.get_columns(main_table_name, schema=cloned_schema)}
except Exception as e:
    print(e)

if main_table_name in cloned_tables:
    print(f"table {main_table_name} found in {cloned_tables}")

    for column_target in columns_target_type.keys():
        if (remove_parenteses(str(columns_target_type[column_target])) != remove_parenteses(str(columns_source_type[column_target]))) is True:
         

            alter_column_query = DDL(f"""ALTER TABLE {cloned_schema}.{main_table_name} 
                                        ALTER COLUMN {column_target} 
                                        TYPE {columns_source_type[column_target]} 
                                        USING ({column_target}::{columns_source_type[column_target]})""")
                
                
            
            event.listen(Base.metadata, 'before_create', alter_column_query.execute_if(dialect='postgresql'))
            Base.metadata.create_all(cloned_engine)
            print(f"Acomplished ALTER TABLE in {column_target} to type {columns_source_type[column_target]}")
        else:
            print(f" Same type for {column_target}")

    
else:
    print(f"Table {main_table_name} Not Found in {cloned_db}.{cloned_schema}")
    exit()












