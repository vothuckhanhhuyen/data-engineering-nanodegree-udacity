from airflow import DAG
from airflow.operators import LoadDimensionOperator
from sparkify_dag import default_args


def load_dimension_table(parent_dag_name="",
                         task_id="",
                         redshift_conn_id="",
                         sql_query="",
                         table="",
                         mode="append-only",
                         *args, **kwargs):
    """
    Insert data into dimension table
    """
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        default_args=default_args,
        **kwargs
    )

    LoadDimensionOperator(
        task_id=task_id,
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        sql_query=sql_query,
        mode=mode
    )
    return dag
