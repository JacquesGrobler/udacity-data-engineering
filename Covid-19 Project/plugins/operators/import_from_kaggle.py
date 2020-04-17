import kaggle
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ImportFromKaggle(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 datasets = "",
                 path = "",
                 *args, **kwargs):

        super(ImportFromKaggle, self).__init__(*args, **kwargs)
        self.datasets = datasets
        self.path = path

    def execute(self, context):
        
        kaggle_api = kaggle.KaggleApi()
        kaggle_api.authenticate()
        
        self.log.info('Importing data from kaggle...')

        for data in self.datasets:
            dataset = data.get('dataset')
            kaggle_api.dataset_download_files(dataset, path=self.path, force=True, quiet=True, unzip=True)
        
        self.log.info('Data imported.')
       

