from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.schema import CreateTable
from sqlalchemy.inspection import inspect
import json

def create_absent_tables():
    # Carregar credenciais
    with open('dags/credentials/credentials.json', 'r') as file:
        credentials = json.load(file)

    user = credentials['user_name']
    password = credentials['password']
    ip_connection = credentials['ip_connection']
    port = credentials['port']
    db_name = credentials['db']

    # Carregar informações do esquema principal
    with open('data_out/main_db_schema.json', 'r') as main_info_file:
        main_info = json.load(main_info_file)

    main_db = main_info['main_db']
    main_schema = main_info['main_schema']

    # Configuração da conexão com o banco de dados
    database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
    master_engine = create_engine(database_uri)

    # Obter a lista de tabelas do esquema principal
    inspector = inspect(master_engine)
    main_tables = inspector.get_table_names(schema=main_schema)

    # Carregar lista de tabelas e esquemas
    with open('data_out/list_tables.json', 'r') as tables_file:
        tables_list = json.load(tables_file)

    for db, schemas in tables_list.items():
        set_tables = set()

        engine_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}'
        engine = create_engine(engine_uri)
        inspector = inspect(engine)
        for schema, tabelas in schemas.items():
            existing_tables = inspector.get_table_names(schema=schema)

            for table_cheker in main_tables:
                if table_cheker not in existing_tables:
                    set_tables.add(table_cheker)

            
                    original_metadata = MetaData(schema=main_schema)
                    original_table = Table(table_cheker, original_metadata, autoload_with=master_engine)
                    
                    destination_metadata = MetaData(schema=schema)
                    
                    # Criar novos objetos Column baseados nos da tabela original
                    new_columns = [Column(column.name, column.type, primary_key=column.primary_key, 
                                        nullable=column.nullable, default=column.default, 
                                        autoincrement=column.autoincrement) for column in original_table.columns]

                    new_table = Table(table_cheker, destination_metadata, *new_columns)
                    new_table.create(engine)
                    print(f"Created table {table_cheker} in {db}.{schema}")

        engine.dispose()
