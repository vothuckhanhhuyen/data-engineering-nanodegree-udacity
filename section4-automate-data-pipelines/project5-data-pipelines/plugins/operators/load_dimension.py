from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 mode="append-only",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table,
        self.mode = mode

    def execute(self, context):
        """
        Insert data into dimension table
        """
        self.log.info("Connect to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.mode == "delete-load":
            self.log.info(f"Run query to truncate dimension table {self.table}")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
        self.log.info(f"Run query to insert data into dimension table {self.table}")
        redshift_hook.run(str(self.sql_query))
