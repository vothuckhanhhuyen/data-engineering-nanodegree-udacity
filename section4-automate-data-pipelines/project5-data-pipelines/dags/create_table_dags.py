import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


with DAG('create_tables_dag', 
         start_date=datetime.datetime.now()) as dag:
    
    PostgresOperator(
        task_id="create_table",
        dag=dag,
        postgres_conn_id="redshift",
        sql='create_tables.sql'
    )