import os
import glob
import kaggle
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class KaggleToS3(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 datasets = "",
                 path = "",
                 bucket_name = "",
                 aws_conn_id = "",
                 *args, **kwargs):

        super(KaggleToS3, self).__init__(*args, **kwargs)
        self.datasets = datasets
        self.path = path
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        
        kaggle_api = kaggle.KaggleApi()
        kaggle_api.authenticate()
        
        self.log.info('kaggle authentication successful.')
        self.log.info('Importing data from kaggle...')

        for data in self.datasets:
            dataset = data.get('dataset')
            kaggle_api.dataset_download_files(dataset, path=self.path, force=True, quiet=True, unzip=True)
        
        self.log.info('Data imported.')
        self.log.info('Staring export to s3...')
        
        all_files = []
        
        for root, dirs, files in os.walk(self.path):
            files = glob.glob(os.path.join(root,'*.csv'))
            for f in files :
                all_files.append((f)) 
        
        self.log.info('files to be exported: {}'.format(all_files))
        
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        
        self.log.info('Exporting files...')
        
        for file in all_files:
            s3.load_file(file, 
                         '{}'.format(file),
                         self.bucket_name,
                         replace=True,
                         encrypt=False,
                         gzip=False,
                         acl_policy=None)
            
        self.log.info('Files successfully exported.')
       

