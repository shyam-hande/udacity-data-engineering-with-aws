from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD8E'

    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.mode = mode

    def execute(self, context):
        self.log.info("staging table to dimension table loading")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.mode == "truncate-insert":
            self.log.info("Truncating dimension table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("inserting data into dimension table")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
