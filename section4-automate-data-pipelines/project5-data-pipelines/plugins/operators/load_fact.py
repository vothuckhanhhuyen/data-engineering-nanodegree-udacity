from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        """
        Insert data into fact table from stagging tables
        """
        self.log.info("Connect to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(
            "Run query to insert data into fact table from stagging tables")
        redshift_hook.run(f"INSERT INTO {self.table} {self.sql_query}")
