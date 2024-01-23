import re
import logging
import pandas as pd
from sqlalchemy import event, DDL
from sqlalchemy.dialects import mssql
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, MetaData



def remove_collate(column_type):
    return str(column_type).split(" ")[0]


def remove_parenteses(texto):
    return re.sub(r'\(.*\)', '', texto)




def connect_and_inspector(user_p,
                       password_p, 
                       server_name_p,
                       db_name_p, 
                       table_name_p ,
                       schema_name_p ,
                       driver_odbc = "ODBC+Driver+17+for+SQL+Server"):
    connection =  f"mssql+pyodbc://{user_p}:{password_p}@{server_name_p}/{db_name_p}?driver={driver_odbc}"
    engine = create_engine(connection)
    inspector = inspect(engine)
    tables_name = inspector.get_table_names(schema=schema_name_p)

    if table_name_p is not None and schema_name_p is not None:
        columns_to_return = inspector.get_columns(table_name_p, schema=schema_name_p)
        columns_name = [c['name'].lower() for c in columns_to_return]
        return connection, engine, tables_name, columns_to_return, columns_name
    else:
        return connection, engine, inspector
    

def altertable_query(schema_name_p, table_name_p, col_name_p, type_name_p, nullable_check, default_check):

    main_query = f""" ALTER TABLE {schema_name_p}.{table_name_p} ADD {col_name_p} {remove_collate(type_name_p)} """

    if nullable_check is False:
        main_query += f""" NOT NULL """
    else:
        main_query += f""" NULL """
    
    if default_check is None:
        return main_query
    else:
        main_query += F" DEFAULT {default_check}"
        return main_query

  

def get_constraint_name(column_name_p, schema_name_p, connection_string_P):
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
                                   c.name = '{column_name_p}' 
                                AND 
                                    s.name = '{schema_name_p}' """
    
    engine = create_engine(connection_string_P)

    with engine.connect() as connection:
        result = pd.read_sql(query, connection)

    return result.iloc[0,0]



def drop_constraint(schema_p, table_name_p, query_to_execute):
     
     return DDL(f"""BEGIN TRY
        ALTER TABLE {schema_p}.{table_name_p} DROP CONSTRAINT {query_to_execute}
        END TRY BEGIN CATCH END CATCH""")
