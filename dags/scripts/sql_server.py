from sqlalchemy import create_engine, select, text, Table, MetaData
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.postgresql import *
from sqlalchemy.orm import Session, load_only
import pyodbc


metadata = MetaData()

user = "levi"
password = "3523"
server_name = "DESKTOP-O17EGB4"
database_name = "master"

driver_odbc = "ODBC+Driver+17+for+SQL+Server"
connection_string = f"mssql+pyodbc://{user}:{password}@{server_name}/{database_name}?driver={driver_odbc}"

engine = create_engine(connection_string)

tabela1 = Table('MSreplication_options', metadata, autoload_with=engine)

stmt = select(tabela1).options(load_only(tabela1.transactional))


with Session(engine) as session:
    books = session.scalars(stmt).all()

    for book in books:
        print(book)