from sqlalchemy import create_engine
from sqlalchemy.sql import text

# String de conexão: substitua com suas credenciais
engine = create_engine('postgresql://postgres:3523@localhost:5432/postgres')

# Conectar ao banco de dados
with engine.connect() as conn:
    # Consulta para listar os esquemas
    query = text("SELECT schema_name FROM information_schema.schemata")

    # Executar a consulta
    result = conn.execute(query)

    # Imprimir o nome de cada esquema
    for row in result:
        print(row['schema_name'])
