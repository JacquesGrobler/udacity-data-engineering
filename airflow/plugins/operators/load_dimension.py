from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id="",
                 dimension_query = "",
                 delect_or_append = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.dimension_query = dimension_query
        self.delect_or_append = delect_or_append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.delect_or_append == 'append':
            self.log.info('Loading data into table')
            redshift.run("INSERT INTO {} {}".format(self.table, self.dimension_query))
            
        elif self.delect_or_append == 'delete':
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info('Loading data into table')
            redshift.run("INSERT INTO {} {}".format(self.table, self.dimension_query))
        
