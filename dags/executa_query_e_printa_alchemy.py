from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
from query_alchemy import execute_query

# Função para executar a query e imprimir os resultados
def print_postgres_query_alchemy():
    execute_query()
    

# Configurações padrão para a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Definir a DAG
dag = DAG(
    'postgres_query_alchemy2',
    default_args=default_args,
    description='Uma DAG simples para executar uma query no PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

# Tarefa para imprimir o resultado da query
print_query_task = PythonOperator(
    task_id='print_postgres_query_alchemy',
    python_callable=print_postgres_query_alchemy,
    dag=dag,
)

print_query_task
