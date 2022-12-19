from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from airflow.operators.subdag_operator import SubDagOperator
from helpers import SqlQueries

from subdag import load_dimension_table

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    "owner": "huyenvtk1",
    "depends_on_past": False,
    "start_date": datetime(2022, 12, 13),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

parent_dag_name = "sparkify_dag"
dag = DAG(parent_dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          end_date=datetime(2022, 12, 14),
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    file_format="json",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    file_format="json",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_task_id = "Load_user_dim_table"
load_user_dimension_table_subdag = SubDagOperator(
    subdag=load_dimension_table(
        parent_dag_name=parent_dag_name,
        task_id=load_user_dimension_task_id,
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.song_table_insert,
        mode="delete-load"
    ),
    task_id=load_user_dimension_task_id,
    dag=dag
)

load_song_dimension_task_id = "Load_song_dim_table"
load_song_dimension_table_subdag = SubDagOperator(
    subdag=load_dimension_table(
        parent_dag_name=parent_dag_name,
        task_id=load_song_dimension_task_id,
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        mode="delete-load"
    ),
    task_id=load_song_dimension_task_id,
    dag=dag
)

load_artist_dimension_task_id = "Load_artist_dim_table"
load_artist_dimension_table_subdag = SubDagOperator(
    subdag=load_dimension_table(
        parent_dag_name=parent_dag_name,
        task_id=load_artist_dimension_task_id,
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.song_table_insert,
        mode="delete-load"
    ),
    task_id=load_artist_dimension_task_id,
    dag=dag
)

load_time_dimension_task_id = "Load_time_dim_table"
load_time_dimension_table_subdag = SubDagOperator(
    subdag=load_dimension_table(
        parent_dag_name=parent_dag_name,
        task_id=load_time_dimension_task_id,
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.song_table_insert,
        mode="delete-load"
    ),
    task_id=load_time_dimension_task_id,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >>\
    load_songplays_table >> [load_user_dimension_table_subdag, load_song_dimension_table_subdag,
                             load_artist_dimension_table_subdag, load_time_dimension_table_subdag] >>\
    run_quality_checks >> end_operator
