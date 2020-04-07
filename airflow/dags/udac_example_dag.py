from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1, 0, 0, 0, 0),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    json_option = 's3://udacity-dend/log_json_path.json',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id ='redshift',
    aws_credentials_id ='aws_credentials',
    s3_bucket ='udacity-dend',
    s3_key ='song_data',
    json_option = 'auto',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    fact_query = SqlQueries.songplay_table_insert,
    delect_or_append = 'append', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    dimension_query = SqlQueries.user_table_insert,
    delect_or_append = 'delete', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    dimension_query = SqlQueries.song_table_insert,
    delect_or_append = 'delete' # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    dimension_query = SqlQueries.artist_table_insert,
    delect_or_append = 'delete' # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    dimension_query = SqlQueries.time_table_insert,
    delect_or_append = 'delete' # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table='songplays',
    column='userid',
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ],
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
