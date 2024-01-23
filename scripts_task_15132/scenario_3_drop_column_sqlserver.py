from sqlalchemy import create_engine, DDL, event
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.dialects import mssql
from sqlalchemy.orm import declarative_base
import logging
from function_module._functions import  get_constraint_name, get_constraint_name


Base = declarative_base()


user = "Levi.deAndrade"
password = "#l3v^!d$ApP%^20*&23_"
server_name = "azsqlserver-601a-sqlprod.database.windows.net"
main_db = "APP_MASTER_CHECKER_DEV"
driver_odbc = "ODBC+Driver+17+for+SQL+Server"


main_schema = "dbo"
cloned_db = "APP_MASTER_CHECKER_DEV"
cloned_schema = "c1"
table_name = "STANDARD"

try:
# Conecction to main db
    database_uri = f"mssql+pyodbc://{user}:{password}@{server_name}/{main_db}?driver={driver_odbc}"
    master_engine = create_engine(database_uri)
    inspector = inspect(master_engine)
except Exception as e:
    logging.info(e)
    exit()

try:
    # Table to be compared
    main_table_name = table_name
    # Main Columns type informations
    columns_source = {c['name'].lower(): c['name'] for c in inspector.get_columns(main_table_name, schema=main_schema)}
except Exception as e:
    logging.info(e)
    exit()


try:
    # Conecction to cloned db
    cloned_uri = f"mssql+pyodbc://{user}:{password}@{server_name}/{cloned_db}?driver={driver_odbc}"
    cloned_engine = create_engine(cloned_uri)
    inspector_clone = inspect(cloned_engine)
except Exception as e:
    logging.info(e)
    exit()

try:
    # Getting table's name
    cloned_tables = inspector_clone.get_table_names(schema=cloned_schema)
    # Cloned Columns type informations
    columns_target = {c['name'].lower(): c['name'] for c in inspector_clone.get_columns(main_table_name, schema=cloned_schema)}
except Exception as e:
    logging.info(e)
    exit()

if main_table_name in cloned_tables:
    for column_target in columns_target.keys():
        drop_constraint = ""
        if column_target not in columns_source.keys():
            try:
                query_return = get_constraint_name(column_target, cloned_schema, cloned_uri)
                if query_return is not None:
                    drop_constraint = DDL(f"""BEGIN TRY
                                                    ALTER TABLE {cloned_schema}.{main_table_name} DROP CONSTRAINT {query_return}
                                                END TRY BEGIN CATCH END CATCH""")
                    
                    print(f" !CONSTRAINT {query_return} DROPPED FOR THIS OPERATION")
                    logging.info(f" !CONSTRAINT {query_return} DROPPED FOR THIS OPERATION")
                    event.listen(Base.metadata, 'before_create', drop_constraint.execute_if(dialect=mssql.dialect()))

            except Exception as e:
               
                logging.info(e)
                logging.info("Constraint Default not Found")


            drop_column_query = DDL(f"""ALTER TABLE {cloned_schema}.{main_table_name} 
                                        DROP COLUMN {column_target}""")
                
            
                
            event.listen(Base.metadata, 'before_create', drop_column_query.execute_if(dialect=mssql.dialect()))
            logging.info(f"Acomplished DROP COLUMN {column_target} in {cloned_db}.{cloned_schema}.{main_table_name}")
    Base.metadata.create_all(cloned_engine)
else:
    logging.info(f"Table {main_table_name} Not Found in {cloned_db}.{cloned_schema}")
    exit()












