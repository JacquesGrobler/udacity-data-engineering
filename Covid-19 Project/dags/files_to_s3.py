import datetime as dt
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import ImportFromKaggle, ExportToS3

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 4, 15, 0, 0, 0, 0),
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

get_datasets = ImportFromKaggle(
    task_id = 'get_data_from_kaggle',
    dag=dag,   
    datasets = [
        {'dataset': 'sudalairajkumar/undata-country-profiles'},
        {'dataset': 'imdevskp/corona-virus-report'},
        {'dataset': 'cristiangarrido/covid19geographicdistributionworldwide'}
    ],
    path = 'data',
    )

export_files = ExportToS3(
    task_id = 'export_files_to_s3',
    dag = dag,
    filepath = 'data',
    bucket_name = 'covid-19-data-project',
    aws_conn_id = 's3_conn',
    )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> get_datasets
get_datasets >> export_files
export_files >> end_operator