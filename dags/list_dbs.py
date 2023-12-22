from sqlalchemy import create_engine
from sqlalchemy.sql import text


# String de conexão: 'postgresql://username:password@host:port/database'
engine = create_engine('postgresql://postgres:3523@localhost:5432/postgres')


with engine.connect() as conn:
    # Consulta para listar os bancos de dados
    query = text("SELECT datname FROM pg_database")

    # Executar a consulta
    result = conn.execute(query)

    # Imprimir o nome de cada banco de dados
    for row in result:
        print(row['datname'])