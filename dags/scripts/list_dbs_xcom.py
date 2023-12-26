from sqlalchemy import create_engine
from sqlalchemy.sql import text

def list_dbs_name():
    engine = create_engine('postgresql://postgres:3523@192.168.144.1:5432/postgres')
    with engine.connect() as conn:
        query = text("SELECT datname FROM pg_database")
        result = conn.execute(query)
        db_names = [row['datname'] for row in result]
    return db_names


def read_dbs_task(**kwargs):
    ti = kwargs['ti']
    db_names = ti.xcom_pull(task_ids='postgres_list_dbs', dag_id='postgres_list_dbs_dag')
    for name in db_names:
        print(name)
