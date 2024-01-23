import logging
from sqlalchemy.dialects import mssql
from sqlalchemy import MetaData, DDL, event
from sqlalchemy.orm import declarative_base
from function_module._functions import connect_and_inspector, altertable_query

   
metadata = MetaData()


user = "Levi.deAndrade"
password = "#l3v^!d$ApP%^20*&23_"
main_db = "APP_MASTER_CHECKER_DEV"
server_name = "azsqlserver-601a-sqlprod.database.windows.net"
driver_odbc = "ODBC+Driver+17+for+SQL+Server"


main_schema = "dbo"
cloned_db = "APP_MASTER_CHECKER_DEV"
cloned_schema = "c1"
main_table = "STANDARD"

Base = declarative_base()

try:
    # Conecction to main db and inspector creation
    _,master_engine, table_source , columns_source_name, coluns_source_name = connect_and_inspector(
        user     , password   , server_name,
        main_db  , main_table , main_schema,
        driver_odbc)
    
except Exception as e:
    logging.info(e)

try:

    # Conecction to cloned db and inspector creation
    _,cloned_engine, cloned_tables, columns_target_name , coluns_target_name = connect_and_inspector(
        user     , password       ,server_name   ,
        cloned_db, main_table     ,cloned_schema ,
        driver_odbc)
except Exception as e:
    logging.info(e)


if main_table in cloned_tables:

    for col in columns_source_name:
        if col['name'].lower() not in coluns_target_name:
            main_query = altertable_query(cloned_schema, main_table, 
                                        col['name'],     col['type'], 
                                        col['nullable'], col['default'] )
            
            event.listen(Base.metadata, 'before_create', DDL(main_query).execute_if(dialect=mssql.dialect()))
            print("")
            logging.info(f"Completed adding column {col['name']} in {cloned_db}.{cloned_schema}.{main_table}")
            print(f"Completed adding column {col['name']} in {cloned_db}.{cloned_schema}.{main_table}")
    Base.metadata.create_all(cloned_engine) 

else:
    logging.info(f" Table {main_table} not found in {cloned_db}.{cloned_schema}")
    exit()