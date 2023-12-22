from sqlalchemy import create_engine, MetaData, Table, Column

# Conexão com o banco de dados
engine = create_engine('postgresql://postgres:3523@localhost:5432/postgres')
metadata = MetaData()

# Refletir as tabelas
main_table = Table('usuario', metadata, autoload_with=engine)
secondary_table = Table('salary', metadata, autoload_with=engine)

# Função para ajustar a tabela secundária com base na tabela principal
def ajustar_tabelas(t_principal, t_secundaria):
    # Obter colunas das tabelas
    colunas_principal = {c.name: c for c in t_principal.columns}
    colunas_secundaria = {c.name: c for c in t_secundaria.columns}

    with engine.connect() as conn:
        trans = conn.begin()

        # Adicionar colunas faltantes da tabela principal à tabela secundária
        for col_name, col_obj in colunas_principal.items():
            if col_name not in colunas_secundaria:
                conn.execute(f'ALTER TABLE {t_secundaria.name} ADD COLUMN {col_name} {col_obj.type}')
                print(f'Coluna {col_name} adicionada à {t_secundaria.name}')

        # Alterar tipos de colunas na tabela secundária para corresponder à tabela principal
        for col_name, col_obj in colunas_principal.items():
            if col_name in colunas_secundaria and str(col_obj.type) != str(colunas_secundaria[col_name].type):
                conn.execute(f'ALTER TABLE {t_secundaria.name} ALTER COLUMN {col_name} TYPE {col_obj.type}')
                print(f'Tipo da coluna {col_name} alterado para {col_obj.type} em {t_secundaria.name}')

        trans.commit()

# Ajustar a tabela secundária com base na tabela principal
ajustar_tabelas(main_table, secondary_table)
