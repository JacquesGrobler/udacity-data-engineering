import os
import glob
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ExportToS3(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 filepath = "",
                 bucket_name = "",
                 aws_conn_id = "",
                 *args, **kwargs):

        super(ExportToS3, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id
        
    def execute(self, context):
        
        all_files = []
        
        for root, dirs, files in os.walk(self.filepath):
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
        






