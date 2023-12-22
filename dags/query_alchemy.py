from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select


def execute_query():       

    # String de conexão: 'postgresql://username:password@host:port/database'
    engine = create_engine('postgresql://postgres:3523@192.168.144.1:5432/postgres')

    # Conectar ao banco de dados
    connection = engine.connect()

    # Refletir a tabela existente
    metadata = MetaData(bind=engine)
    minha_tabela = Table('salary', metadata, autoload=True, autoload_with=engine)

    # Preparar e executar a query
    query = select([minha_tabela])
    result = connection.execute(query)

    # Iterar sobre os resultados
    for row in result:
        print(row)

    # Fechar a conexão
    connection.close()