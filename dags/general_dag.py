from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.list_dbs import list_dbs_name
from scripts.list_schemas import list_schemas
from scripts.list_tables import list_tables

# Function to list dbs and save the information
def list_dbs_and_save():
    dbnames = list_dbs_name()
    return dbnames

def list_schemas_and_save():
    schemas_names = list_schemas()
    return schemas_names

def list_tables_and_save():
    list_tablesnames = list_tables()
    return list_tablesnames
    

# Default Dag Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Dag Definition
dag = DAG(
    'General_dag1',
    default_args=default_args,
    description='Dag to general purpouse',
    schedule_interval='@daily',
    catchup=False
)

# Task to retrieve list of available databases and save them in a json file
list_dbs_task = PythonOperator(
    task_id='postgres_list_dbs',
    python_callable=list_dbs_and_save,
    dag=dag,
)


# Task to retrieve list of available schemas in each database
list_schemas_task = PythonOperator(
    task_id='postgres_list_schemas',
    python_callable=list_schemas_and_save,
    dag=dag,
)

# Task to retrieve list of available tables in each schema
list_tables_task = PythonOperator(
    task_id='postgres_list_tables',
    python_callable=list_tables_and_save,
    dag=dag,
)


list_dbs_task >> list_schemas_task >> list_tables_task