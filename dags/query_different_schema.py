from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select

# String de conexão: 'postgresql://username:password@host:port/database'
engine = create_engine('postgresql://postgres:3523@localhost:5432/postgres')

# Conectar ao banco de dados
connection = engine.connect()

# Refletir a tabela existente no schema especificado
metadata = MetaData(bind=engine)
minha_tabela = Table('table_1_schema2_db1', metadata, 
                     autoload=True, 
                     autoload_with=engine, 
                     schema='schema_2_db_1')  # Especifique o schema aqui

# Preparar e executar a query
query = select([minha_tabela])
result = connection.execute(query)

# Iterar sobre os resultados
for row in result:
    print(row)

# Fechar a conexão
connection.close()
