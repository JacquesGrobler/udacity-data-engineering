"""
The puprose of this custom operator is the download data from Kaggle to the local machine.
Either the entire dataset can be downloaded a specific file in the dataset.
When a specific file is downloaded it will automatically be zipped.
"""
import kaggle
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import re
import gzip
import shutil

class KaggleToLocal(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 dataset_or_file = "",
                 dataset = "",
                 file_name = "",
                 path = "",
                 *args, **kwargs):

        super(KaggleToLocal, self).__init__(*args, **kwargs)
        self.dataset_or_file = dataset_or_file
        self.dataset = dataset
        self.file_name = file_name
        self.path = path

    def execute(self, context):
        
        # authentication
        kaggle_api = kaggle.KaggleApi()
        kaggle_api.authenticate()
        
        self.log.info('kaggle authentication successful.')
        self.log.info('Importing data from kaggle...') 
        
        # downloads the entire dataset
        if self.dataset_or_file == "dataset":     
            for data in self.dataset:
                dataset = data.get('dataset')
                kaggle_api.dataset_download_files(dataset, path=self.path, force=True, quiet=True, unzip=True)

        # downloads a specific file in the dataset        
        elif self.dataset_or_file == "file":
            kaggle_api.dataset_download_file(self.dataset, self.file_name, path=self.path, force=True, quiet=True)
        
        self.log.info('Data imported to local.')
        
        