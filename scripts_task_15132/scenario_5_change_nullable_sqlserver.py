from sqlalchemy import create_engine, DDL, event
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import declarative_base
import re
import logging
import pandas as pd


def execute_query_and_store_result(connection_string, query):
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        result = pd.read_sql(query, connection)

    return result.iloc[0, 0]





def remove_collate(column_type):
    return str(column_type).split(" ")[0]



def remove_parenteses(texto):
    return re.sub(r'\(.*\)', '', texto)

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
    columns_source_type = {c['name']: {"type":c['type'],"nullable": c['nullable']} for c in inspector.get_columns(main_table_name, schema=main_schema)}
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
    columns_target_type = {c['name']: {"type":c['type'],"nullable": c['nullable']} for c in inspector_clone.get_columns(main_table_name, schema=cloned_schema)}
except Exception as e:
    logging.info(e)
    exit()


if main_table_name in cloned_tables:
    for column_target in columns_target_type.keys():
        drop_constraint = ""
        print(column_target)
        if column_target in columns_source_type.keys():
            
            if (remove_parenteses(str(columns_target_type[column_target]['type'])) == remove_parenteses(str(columns_source_type[column_target]['type']))) is True:
                if columns_target_type[column_target]['nullable'] != columns_source_type[column_target]['nullable']:
                    nullable = "Null" if str(columns_source_type[column_target]['nullable']) == "True" else "Not Null"
                    try:
                        query = f"""SELECT 
                                    dc.name
                                FROM 
                                    sys.columns c
                                        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                                        INNER JOIN sys.tables t ON c.object_id = t.object_id
                                        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                                        LEFT JOIN sys.default_constraints dc ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                                        LEFT JOIN sys.foreign_key_columns fkc ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
                                        LEFT JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
                                        LEFT JOIN sys.tables ref_t ON fk.referenced_object_id = ref_t.object_id
                                        LEFT JOIN sys.columns ref_c ON fkc.referenced_object_id = ref_c.object_id AND fkc.referenced_column_id = ref_c.column_id
                                WHERE
                                   c.name = '{column_target}' 
                                AND 
                                    s.name = '{cloned_schema}' """
                        query_return = execute_query_and_store_result(cloned_uri,query)
                        drop_constraint = DDL(f"""BEGIN TRY
                                                    ALTER TABLE {cloned_schema}.{main_table_name} DROP CONSTRAINT {query_return}
                                                END TRY BEGIN CATCH END CATCH""")
                        print(f" !CONSTRAINT {query_return} DROPPED FOR THIS OPERATION")
                        event.listen(Base.metadata, 'before_create', drop_constraint.execute_if(dialect=mssql.dialect()))
                    except Exception as e:
                        del drop_constraint
                        logging.info("Constraint Default not Found")
                        logging.info(e)      

                    alter_column_query = DDL(f"""ALTER TABLE {cloned_schema}.{main_table_name} 
                                                ALTER COLUMN {column_target} 
                                                {remove_collate(columns_source_type[column_target]['type'])} 
                                                {nullable}""")
                    
                    
                    event.listen(Base.metadata, 'before_create', alter_column_query.execute_if(dialect=mssql.dialect()))
                    print("")
                    print(f"Acomplished ADD {nullable} in {cloned_schema}.{main_table_name} in column {column_target} ")
            Base.metadata.create_all(cloned_engine)    
else:
    logging.info(f"Table {main_table_name} Not Found in {cloned_db}.{cloned_schema}")
    exit()  








