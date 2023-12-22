from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook

# Função para executar a query e imprimir os resultados
def print_postgres_query():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('select * from public.usuario;')  # Sua query SQL
    for row in cursor.fetchall():
        print(row)
    cursor.close()
    conn.close()

# Configurações padrão para a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Definir a DAG
dag = DAG(
    'postgres_query_python_operator',
    default_args=default_args,
    description='Uma DAG simples para executar uma query no PostgreSQL',
    schedule_interval='@daily',
    catchup=False
)

# Tarefa para imprimir o resultado da query
print_query_task = PythonOperator(
    task_id='print_postgres_query',
    python_callable=print_postgres_query,
    dag=dag,
)

print_query_task
