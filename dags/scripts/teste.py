# Lista fornecida
lista = [['postgres', 'postgres_1', ['user']], ['new_db', 'new_db_2', ['user']], ['new_db', 'new_db_1', ['user', 'games']]]

# Inicializando o dicionário
dicionario = {}

# Preenchendo o dicionário
for chave_principal, chave_aninhada, valor_aninhado in lista:
    if chave_principal not in dicionario:
        dicionario[chave_principal] = {}
    dicionario[chave_principal][chave_aninhada] = valor_aninhado

print(dicionario)
