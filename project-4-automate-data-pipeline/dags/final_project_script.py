from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator

from udacity.common.final_project_sql_statements import SqlQueries

default_arguments = {
    'owner': 'Shyam',
    'start_date': datetime(2026, 1, 11),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup': False
}


@dag(
    default_args=default_arguments,
    description='Dag for loading and transforming data in Redshift',
    end_date=datetime(2016, 1, 12),
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='begin_execution')

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        aws_credentials_id='aws_creds',
        redshift_conn_id='redshift',
        table='staging_songs',
        s3_bucket='shyam-automate-data-pipelines',
        s3_key='song-data/A/A/'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        aws_credentials_id='aws_creds',
        redshift_conn_id='redshift',
        table='staging_events',
        s3_bucket='shyam-automate-data-pipelines',
        s3_key='log-data',
        log_json_file='log_path.json'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.song_table_insert,
        table='songs',
        mode='truncate-insert'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.artist_table_insert,
        table='artists',
        mode='truncate-insert'
    )

    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.songplay_table_insert,
        table='songplays'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.user_table_insert,
        table='users',
        mode='truncate-insert'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        redshift_conn_id='redshift',
        sql_query=SqlQueries.time_table_insert,
        table='time',
        mode='truncate-insert'
    )

    quality_checks = DataQualityOperator(
        task_id='data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songs', 'artists', 'songplays', 'users', 'time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_songs_to_redshift,stage_events_to_redshift ] >> \
        load_songplays_table >> [load_song_dimension_table, load_artist_dimension_table,load_user_dimension_table, load_time_dimension_table] >> \
        quality_checks >> end_operator


final_project_dag = final_project()
