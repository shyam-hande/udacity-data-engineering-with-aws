from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA60'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"executing data quality for the table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. No data in {table}")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. No data rows in {table}")
            self.log.info(
                f"Data quality checks passed on table {table}. It has {records[0][0]} records")
