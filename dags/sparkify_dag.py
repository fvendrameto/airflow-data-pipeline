import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (DataQualityOperator, LoadDimensionOperator,
                               LoadFactOperator, StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 12),
    'depends_on_past': False,
    # 'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
    # 'email_on_retry': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once',
          catchup=False)

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_format='s3://udacity-dend/log_json_path.json')

stage_songs_to_redshift = StageToRedshiftOperator(task_id='stage_songs',
                                                  dag=dag,
                                                  table='staging_songs',
                                                  s3_bucket='udacity-dend',
                                                  s3_key='song_data')

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    table='songplays',
    staging_select_query=SqlQueries.songplay_table_insert)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    table='users',
    staging_select_query=SqlQueries.user_table_insert)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    table='songs',
    staging_select_query=SqlQueries.song_table_insert)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    table='artists',
    staging_select_query=SqlQueries.artist_table_insert)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    table='time',
    staging_select_query=SqlQueries.time_table_insert)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    dag=dag,
    all_checks=[
        {
            'query': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM songs WHERE artistid IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM artists WHERE name IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM songplays WHERE start_time IS NULL',
            'expected_result': 0
        },
        {
            'query': 'SELECT COUNT(*) FROM songplays WHERE userid IS NULL',
            'expected_result': 0
        }
    ])

end_operator = DummyOperator(task_id='stop_execution', dag=dag)

# Run start operator first and load the raw data on staging tables
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# After that, load the fact table
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

# Next, load the dimension tables, in parallel
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# After loading all tables, run data quality checks to ensure everything works
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Finally, run end operator
run_quality_checks >> end_operator
