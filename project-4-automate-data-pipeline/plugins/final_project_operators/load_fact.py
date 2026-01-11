from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):

    ui_color = '#F98868'

    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("staging table to fact table loading")
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_query))
