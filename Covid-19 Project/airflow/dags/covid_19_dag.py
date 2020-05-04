import datetime as dt
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import KaggleToLocal, ExportToS3, S3ToRedshiftOperator, CreateOrDeleteOperator, LoadTableOperator, DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 4, 18, 0, 0, 0, 0),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('covid_19_dag',
          default_args=default_args,
          description='Loads data from kaggle to an s3 bucket',
          schedule_interval=None,
          max_active_runs=1,
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

datasets_to_local = KaggleToLocal(
    task_id = 'datasets_to_local',
    dag=dag, 
    dataset_or_file = 'dataset', # can either be 'dataset' to download the whole dataset or 'file' to download a specific file in a dataset. 
    file_name = '', # enter file name here, if the whole dataset is to be downloaded then leave as an empty string ('')
    dataset = [ # datasets to download
        {'dataset': 'imdevskp/corona-virus-report'},
        {'dataset': 'cristiangarrido/covid19geographicdistributionworldwide'},
        {'dataset': 'aellatif/covid19'},
        {'dataset': 'juanmah/world-cities'}
    ],
    path = '/home/workspace/', # directory to where is will be saved on your local machine
    )

export_to_s3 = ExportToS3(
    task_id = 'export_to_s3',
    dag=dag,
    filepath = '/home/workspace/', # directory that the files were saved to on your local machine
    bucket_name = 'covid-19-data-project', # S3 bucket name that the files will be sent to
    aws_conn_id = 's3_conn', # connection to s3, created in the airflow UI.
    )

delete_staging_tables_if_exists = CreateOrDeleteOperator(
    task_id='delete_staging_tables_if_exists',
    dag=dag,
    create_or_delete = 'delete', # create_or_delete can either be 'delete' to delete tables mentioned in the delete_table_queries variable in sql_queries.py or 'create' to create tables mentioned in the create_table_queries variable.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
)

create_staging_tables = CreateOrDeleteOperator(
    task_id='create_staging_tables',
    dag=dag,
    create_or_delete = 'create', # create_or_delete can either be 'delete' to delete tables mentioned in the delete_table_queries variable in sql_queries.py or 'create' to create tables mentioned in the create_table_queries variable.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
)

load_country_iso_staging_table = S3ToRedshiftOperator(
    task_id='load_country_iso_staging_table',
    dag=dag,
    table='country_iso_stage', # redshift table that data will be loaded to
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/Countries_ISO.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

load_hospital_beds_staging_table = S3ToRedshiftOperator(
    task_id='load_hospital_beds_staging_table',
    dag=dag,
    table='hospital_beds_stage', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/Hospital_beds_by_country.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

load_population_by_country_staging_table = S3ToRedshiftOperator(
    task_id='load_population_by_country_staging_table',
    dag=dag,
    table='population_by_country_stage', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/Total_population_by_Country_ISO3_Year_2018.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

load_cases_staging_table = S3ToRedshiftOperator(
    task_id='load_cases_staging_table',
    dag=dag,
    table='covid_19_cases_stage', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/covid_19_clean_complete.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

load_tweet_staging_table = S3ToRedshiftOperator(
    task_id='load_tweet_staging_table',
    dag=dag,
    table='tweets_stage', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/covid-19_tw.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

load_cities_staging_table = S3ToRedshiftOperator(
    task_id='load_cities_staging_table',
    dag=dag,
    table='cities_stage', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/worldcities.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

append_tweet_staging_table = S3ToRedshiftOperator(
    task_id='append_tweet_staging_table',
    dag=dag,
    table='tweets_stage', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    aws_credentials_id = 'aws_credentials', # aws connection, created in the airflow UI.
    s3_bucket = 'covid-19-data-project', # s3 bucket name.
    s3_key = 'data/db_tw.csv', # directory of the file in the s3 bucket.
    file_type = 'csv',
    region = 'us-west-2',
    delimiter = ',',
)

load_cases_history_table = LoadTableOperator(
    task_id='Load_covid_19_cases_history_table',
    dag=dag,
    table = 'covid_19_cases_history', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    query = SqlQueries.cases_daily_table_insert, # sql query used to load data into the redshift table.
    delect_or_append = 'delete', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_hospital_beds_table = LoadTableOperator(
    task_id='load_country_hospital_beds_table',
    dag=dag,
    table = 'country_hospital_beds', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    query = SqlQueries.hospital_beds_table_insert, # sql query used to load data into the redshift table.
    delect_or_append = 'delete', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_population_table = LoadTableOperator(
    task_id='load_country_population_table',
    dag=dag,
    table = 'country_population', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    query = SqlQueries.population_table_insert, # sql query used to load data into the redshift table.
    delect_or_append = 'delete', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_tweets_table = LoadTableOperator(
    task_id='load_tweets_table',
    dag=dag,
    table = 'country_tweets', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    query = SqlQueries.tweets_insert, # sql query used to load data into the redshift table.
    delect_or_append = 'delete', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

load_covid_cases_summary_table = LoadTableOperator(
    task_id='load_covid_cases_summary_table',
    dag=dag,
    table = 'covid_19_cases_summary', # redshift table that data will be loaded to.
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
    query = SqlQueries.covid_19_cases_summary, # sql query used to load data into the redshift table.
    delect_or_append = 'delete', # can either be 'append' to append data or 'delete' to truncate table and then add data. 
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift', # connection to redshift, created in the airflow UI.
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM covid_19_cases_history WHERE country is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM country_hospital_beds WHERE country is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM country_population WHERE country is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM country_tweets WHERE country is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM covid_19_cases_summary WHERE country is null", 'expected_result': 0}
    ],
)

delete_staging_tables = CreateOrDeleteOperator(
    task_id='delete_staging_tables',
    dag=dag,
    create_or_delete = 'delete',
    redshift_conn_id = 'redshift', # connection to redshift, created in the airflow UI.
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> datasets_to_local
start_operator >> delete_staging_tables_if_exists
datasets_to_local >> export_to_s3
delete_staging_tables_if_exists >> create_staging_tables
create_staging_tables >> load_country_iso_staging_table
export_to_s3 >> load_country_iso_staging_table
create_staging_tables >> load_hospital_beds_staging_table
export_to_s3 >> load_hospital_beds_staging_table
create_staging_tables >> load_population_by_country_staging_table
export_to_s3 >> load_population_by_country_staging_table
create_staging_tables >> load_cases_staging_table
export_to_s3 >> load_cases_staging_table
create_staging_tables >> load_tweet_staging_table
export_to_s3 >> load_tweet_staging_table
create_staging_tables >> load_cities_staging_table
export_to_s3 >> load_cities_staging_table
load_country_iso_staging_table >> append_tweet_staging_table
load_hospital_beds_staging_table >> append_tweet_staging_table
load_population_by_country_staging_table >> append_tweet_staging_table
load_cases_staging_table >> append_tweet_staging_table
load_tweet_staging_table >> append_tweet_staging_table
load_cities_staging_table >> append_tweet_staging_table
append_tweet_staging_table >> load_cases_history_table
append_tweet_staging_table >> load_hospital_beds_table
append_tweet_staging_table >> load_population_table
append_tweet_staging_table >> load_tweets_table
load_cases_history_table >> load_covid_cases_summary_table
load_hospital_beds_table >> load_covid_cases_summary_table
load_population_table >> load_covid_cases_summary_table
load_tweets_table >> load_covid_cases_summary_table
load_covid_cases_summary_table >> run_quality_checks
run_quality_checks >> delete_staging_tables
delete_staging_tables >> end_operator