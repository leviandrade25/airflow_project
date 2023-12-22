from sqlalchemy import create_engine
from sqlalchemy.engine import reflection


# String de conexão: 'postgresql://username:password@host:port/database'
engine = create_engine('postgresql://postgres:3523@localhost:5432/postgres')

insp = reflection.Inspector.from_engine(engine)

# Nome do schema desejado
nome_do_schema = 'public'

# Listar tabelas no schema especificado
tabelas = insp.get_table_names(schema=nome_do_schema)
for tabela in tabelas:
    print(tabela)