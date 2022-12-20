from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        """
        Execute a basic data quality checking
        """
        self.log.info("Connect to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            checks = [
                {"test_sql": f"SELECT COUNT(*) > 0 as count FROM {table}", "expected_result": True}
            ]
            for _, check in enumerate(checks):
                records = redshift_hook.get_records(check["test_sql"])
                if not check["expected_result"] == records[0][0]:
                    raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows")
                self.log.info(
                f"Data quality on table {table} check passed with {records[0][0]} records")  