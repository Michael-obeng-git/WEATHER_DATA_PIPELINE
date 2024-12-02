from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
}

with DAG('test_postgres_connection', default_args=default_args, schedule_interval='@once') as dag:
    test_connection = PostgresOperator(
        task_id='test_connection',
        postgres_conn_id='postgres_conn_id',
        sql='SELECT 1;'
    )