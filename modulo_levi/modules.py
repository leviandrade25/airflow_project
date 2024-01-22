from sqlalchemy import create_engine
import pandas as pd



def execute_query_and_store_result(connection_string, query):
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        result = pd.read_sql(query, connection)

    return result.iloc[0,0]