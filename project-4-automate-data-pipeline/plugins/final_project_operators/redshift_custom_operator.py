from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook



class PostgreSQLOperator(BaseOperator):
    ui_color = '#99e698'
    template_fields = ('sql')
    template_ext = ('.sql')
    template_fields_renderers = {'sql': 'sql'}

    @apply_defaults
    def __init__(self,
                 *,
                 sql: str = '',
                 postgres_conn_id: str = 'postgres_default',
                 autocommit: bool = True,
                 **kwargs,
                 ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.autocommit = autocommit
        self.postgres_conn_id = postgres_conn_id
        

    def execute(self, context) -> None:
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        """
        SQL Statements executions on Redshift cluster
        """
        try:
            postgres_hook.run(self.sql, self.autocommit) if isinstance(self.sql, str) else [
                postgres_hook.run(query, self.autocommit) for query in self.sql]
            self.log.info('SQL Query Execution completed!!!')

        except Exception as e:
            raise
