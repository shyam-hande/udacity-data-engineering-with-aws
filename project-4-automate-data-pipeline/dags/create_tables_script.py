from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
import pendulum
from datetime import timedelta

from final_project_operators.redshift_custom_operator import PostgreSQLOperator

default_arguments = {
    'owner': 'Shyam',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup': False
}

@dag(
    default_args=default_arguments,
    description='Dag for creating tables in Redshift',
    schedule='0 * * * *'
)
def create_tables_fn():
    start_operator = EmptyOperator(task_id='begin_execution')

    create_redshift_tables = PostgreSQLOperator(
        task_id='create_tables',
        postgres_conn_id='redshift',
        sql='create_tables_ddl.sql'
    )

    end_operator = EmptyOperator(task_id='stop_execution')

    start_operator >> create_redshift_tables >> end_operator


create_tables_dag = create_tables_fn()