import logging
from sqlalchemy.dialects import mssql
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.postgresql import *
from sqlalchemy import create_engine, DDL, event
from function_module._functions import connect_and_inspector, remove_parenteses, get_constraint_name, remove_collate, get_constraint_name



Base = declarative_base()


user = "Levi.deAndrade"
password = "#l3v^!d$ApP%^20*&23_"
server_name = "azsqlserver-601a-sqlprod.database.windows.net"
main_db = "APP_MASTER_CHECKER_DEV"
driver_odbc = "ODBC+Driver+17+for+SQL+Server"


main_schema = "dbo"
cloned_db = "APP_MASTER_CHECKER_DEV"
cloned_schema = "c1"
main_table = "STANDARD"

try:
# Conecction to main db
    database_uri = f"mssql+pyodbc://{user}:{password}@{server_name}/{main_db}?driver={driver_odbc}"
    master_engine = create_engine(database_uri)
    inspector = inspect(master_engine)



    # Conecction to main db and inspector creation
    _,master_engine, table_source , columns_source_name, coluns_source_name = connect_and_inspector(
        user     , password   , server_name,
        main_db  , main_table , main_schema,
        driver_odbc)   
    columns_source_type = {c['name'].lower(): c['type'] for c in inspector.get_columns(main_table, schema=main_schema)}

except Exception as e:
    logging.info(e)
    exit()


try:
    # Conecction to cloned db
    cloned_uri, cloned_engine, cloned_tables, columns_target_name , coluns_target_name = connect_and_inspector(
        user     , password       ,server_name   ,
        cloned_db, main_table     ,cloned_schema ,
        driver_odbc)
    
    inspector_clone = inspect(cloned_engine)
    columns_target_type = {c['name'].lower(): c['type'] for c in inspector_clone.get_columns(main_table, schema=cloned_schema)}
except Exception as e:
    logging.info(e)
    exit()


if main_table in cloned_tables:
    for column_target in columns_target_type.keys():
        drop_constraint = ""
        if column_target in columns_source_type.keys():
            if (remove_parenteses(str(columns_target_type[column_target])) != remove_parenteses(str(columns_source_type[column_target]))) is True:
                try:                               
                    query_return = get_constraint_name(column_target, cloned_schema, cloned_uri)

                    if query_return is not None:
                        drop_constraint = DDL(f"""BEGIN TRY
                                                    ALTER TABLE {cloned_schema}.{main_table} DROP CONSTRAINT {query_return}
                                                END TRY BEGIN CATCH END CATCH""")
                        
                        print(f" !CONSTRAINT {query_return} DROPPED FOR THIS OPERATION")
                        logging.info(f" !CONSTRAINT {query_return} DROPPED FOR THIS OPERATION")
                        event.listen(Base.metadata, 'before_create', drop_constraint.execute_if(dialect=mssql.dialect()))
                   
                except Exception as e:
                    
                    logging.info("Constraint not Found")
                    logging.info(e)      

                new_type = remove_collate(columns_source_type[column_target])
                alter_column_query = DDL(f"""ALTER TABLE {cloned_schema}.{main_table} 
                                            ALTER COLUMN {column_target.upper()} 
                                            {new_type} 
                                            """)
                

                event.listen(Base.metadata, 'before_create', alter_column_query.execute_if(dialect=mssql.dialect()))
                print("")
                logging.info(f"Acomplished ALTER TABLE in {column_target.upper()} to type {new_type}")
                print(f"Acomplished ALTER TABLE in {column_target.upper()} to type {new_type}")
            else:
                print(f" {column_target.upper()} OK FOR  types")
                logging.info(f" {column_target.lower()} OK FOR  types")
    Base.metadata.create_all(cloned_engine) 
               
else:
    logging.info(f"Table {main_table} Not Found in {cloned_db}.{cloned_schema}")
    print(f"Table {main_table} Not Found in {cloned_db}.{cloned_schema}")
    exit()  








