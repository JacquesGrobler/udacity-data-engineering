import datetime as dt
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import CreateOrDeleteOperator, KaggleToS3, S3ToRedshiftOperator

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 4, 18, 0, 0, 0, 0),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('kaggle_to_s3',
          default_args=default_args,
          description='Loads data from kaggle to an s3 bucket',
          schedule_interval=None,
          max_active_runs=1,
         )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_tables = CreateOrDeleteOperator(
    task_id='create_tables',
    dag=dag,
    create_or_delete = 'create',
    redshift_conn_id = 'redshift',
)

kaggle_to_s3 = KaggleToS3(
    task_id = 'kaggle_to_s3',
    dag=dag,   
    datasets = [
        {'dataset': 'imdevskp/corona-virus-report'},
        {'dataset': 'cristiangarrido/covid19geographicdistributionworldwide'}
    ],
    path = 'data',
    bucket_name = 'covid-19-data-project',
    aws_conn_id = 's3_conn',
    )

country_iso_to_redshift = S3ToRedshiftOperator(
    task_id='country_iso_to_redshift',
    dag=dag,
    table='country_iso_stage',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'covid-19-data-project',
    s3_key = 'data/Countries_ISO.csv',
    csv_option = 'csv',
    delimiter = ',',
)

geographic_disbtribution_to_redshift = S3ToRedshiftOperator(
    task_id='geographic_disbtribution_to_redshift',
    dag=dag,
    table='covid_19_geographic_disbtribution_stage',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'covid-19-data-project',
    s3_key = 'data/COVID-19-geographic-disbtribution-worldwide.csv',
    csv_option = 'csv',
    delimiter = ',',
)

hospital_beds_to_redshift = S3ToRedshiftOperator(
    task_id='hospital_beds_to_redshift',
    dag=dag,
    table='hospital_beds_stage',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'covid-19-data-project',
    s3_key = 'data/Hospital_beds_by_country.csv',
    csv_option = 'csv',
    delimiter = ',',
)

population_by_country_to_redshift = S3ToRedshiftOperator(
    task_id='population_by_country_to_redshift',
    dag=dag,
    table='population_by_country_stage',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'covid-19-data-project',
    s3_key = 'data/Total_population_by_Country_ISO3_Year_2018.csv',
    csv_option = 'csv',
    delimiter = ',',
)

clean_complete_to_redshift = S3ToRedshiftOperator(
    task_id='clean_complete_to_redshift',
    dag=dag,
    table='covid_19_cases_stage',
    redshift_conn_id = 'redshift',
    aws_credentials_id = 'aws_credentials',
    s3_bucket = 'covid-19-data-project',
    s3_key = 'data/covid_19_clean_complete.csv',
    csv_option = 'csv',
    delimiter = ',',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_staging_tables
create_staging_tables >> kaggle_to_s3
kaggle_to_s3 >> country_iso_to_redshift
kaggle_to_s3 >> geographic_disbtribution_to_redshift
kaggle_to_s3 >> hospital_beds_to_redshift
kaggle_to_s3 >> population_by_country_to_redshift
kaggle_to_s3 >> clean_complete_to_redshift
#kaggle_to_s3 >> end_operator
