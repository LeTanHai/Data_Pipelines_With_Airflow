from datetime import datetime, timedelta
import os
from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from operators.create_table import CreateTableOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

REDSHIFT_ID = 'redshift'
AWS_CREDENTIALS_ID = 'aws_credentials'
REGION = 'us-west-2'
S3_BUCKET = 'proj3-bucket'
S3_LOG_KEY = 'log-data'
S3_SONG_KEY = 'song-data'
LOG_JSON_FILE = 'log_json_path.json'
LOG_JSON_PATH = "s3://{}/{}".format(S3_BUCKET, LOG_JSON_FILE)
SQL_FILE = 'create_tables.sql'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('udac_proj3_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Run this task if you have not created tables yet
# create_table = CreateTableOperator(
#     task_id = 'Create_tables',
#     dag=dag,
#     redshift_id = REDSHIFT_ID,
#     sql_file = SQL_FILE,
#     first_flag = False
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'staging_events',
    s3_bucket = S3_BUCKET,
    s3_key = S3_LOG_KEY,
    aws_credentials_id = AWS_CREDENTIALS_ID,
    region = REGION,
    log_json_path = LOG_JSON_PATH,
    delete_data = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'staging_songs',
    s3_bucket = S3_BUCKET,
    s3_key = S3_SONG_KEY,
    aws_credentials_id = AWS_CREDENTIALS_ID,
    region = REGION,
    log_json_path = 'auto',
    delete_data = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'songplays',
    sql_statement = SqlQueries.songplay_table_insert,
    delete_data = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'users',
    sql_statement = SqlQueries.user_table_insert,
    delete_data = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'songs',
    sql_statement = SqlQueries.song_table_insert,
    delete_data = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'artists',
    sql_statement = SqlQueries.artist_table_insert,
    delete_data = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    table_name = 'time',
    sql_statement = SqlQueries.time_table_insert,
    delete_data = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_id = REDSHIFT_ID,
    tables = {'users':'userid', 'songs':'songid', 'artists':'artistid', 'time':'start_time'}
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator