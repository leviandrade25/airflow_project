from sqlalchemy import create_engine, MetaData, Table

# Conexão com o banco de dados
engine = create_engine('postgresql://postgres:3523@localhost:5432/postgres')
metadata = MetaData()


#tabela principal que pode ser passada por argumento no airflow
main_table = "usuario"


# Refletir as tabelas
tabela1 = Table('salary', metadata, autoload_with=engine)
tabela2 = Table('usuario', metadata, autoload_with=engine)

# Função para comparar tabelas
def comparar_tabelas(t1, t2):
    diferenças = []

    # Comparar colunas
    colunas_t1 = set(c.name for c in t1.columns)
    colunas_t2 = set(c.name for c in t2.columns)

    diferenças.extend(f'Coluna apenas em {t1.name}: {col}' for col in colunas_t1 - colunas_t2)
    diferenças.extend(f'Coluna apenas em {t2.name}: {col}' for col in colunas_t2 - colunas_t1)

    # Comparar tipos de colunas
    for coluna in colunas_t1.intersection(colunas_t2):
        if str(t1.c[coluna].type) != str(t2.c[coluna].type):
            diferenças.append(f'Tipo diferente para coluna "{coluna}": '
                              f'{t1.name}({t1.c[coluna].type}), '
                              f'{t2.name}({t2.c[coluna].type})')

    return diferenças

# Comparar as tabelas
diferencas = comparar_tabelas(tabela1, tabela2)
for diferenca in diferencas:
    print(diferenca)
