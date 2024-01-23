from sqlalchemy import create_engine, MetaData, Table, MetaData, DDL, event
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects import mssql
from sqlalchemy.orm import declarative_base
import logging




def remove_collate(column_type):
    return str(column_type).split(" ")[0]


user = "Levi.deAndrade"
password = "#l3v^!d$ApP%^20*&23_"
main_db = "APP_MASTER_CHECKER_DEV"
server_name = "azsqlserver-601a-sqlprod.database.windows.net"
driver_odbc = "ODBC+Driver+17+for+SQL+Server"


main_schema = "dbo"
cloned_db = "APP_MASTER_CHECKER_DEV"
cloned_schema = "c1"
table_name = "STANDARD"

Base = declarative_base()

try:
    # Conecction to main db
    database_uri = f"mssql+pyodbc://{user}:{password}@{server_name}/{main_db}?driver={driver_odbc}"
    master_engine = create_engine(database_uri)
    metadata.reflect(bind=master_engine)
    inspector = inspect(master_engine)
except Exception as e:
    logging.info(e)

try:
    # Table to be compared
    main_table = table_name
    columns_source = inspector.get_columns(main_table, schema=main_schema)
    columns_source_name = [c['name'] for c in columns_source]
    table = Table(main_table, metadata, autoload=True, autoload_with=master_engine)
except Exception as e:
    logging.info(e)


try:
    # Conecction to cloned db
    cloned_uri = f"mssql+pyodbc://{user}:{password}@{server_name}/{cloned_db}?driver={driver_odbc}"
    cloned_engine = create_engine(cloned_uri)
    inspector_clone = inspect(cloned_engine)
except Exception as e:
    logging.info(e)

try:
    # Getting table's name
    cloned_tables = inspector_clone.get_table_names(schema=cloned_schema)
    #Columns name
    columns_target_name = [c['name'].lower()  for c in inspector_clone.get_columns(main_table,schema=cloned_schema)]
except Exception as e:
    logging.info(e)

if main_table in cloned_tables:
    for col in columns_source:
        if col['name'].lower() not in columns_target_name:
            main_query = f""" ALTER TABLE {cloned_schema}.{main_table} ADD {col['name']} {remove_collate(col['type'])} """

            if col['nullable'] is False:
                main_query += f""" NOT NULL DEFAULT {col['default']} """


            event.listen(Base.metadata, 'before_create', DDL(main_query).execute_if(dialect=mssql.dialect()))
            print("")
            logging.info(f"Completed adding column {col['name']} in {cloned_db}.{cloned_schema}.{main_table}")
    Base.metadata.create_all(cloned_engine)
            
else:
    logging.info(f" Table {main_table} not found in {cloned_db}.{cloned_schema}")
    exit()
        
