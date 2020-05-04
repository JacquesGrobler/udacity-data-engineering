"""
The purpose of this custom operator is to insert data into tables in Redshift from other existing Redshift tables. 
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadTableOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table= "",
                 redshift_conn_id = "",
                 query = "",
                 delect_or_append = "",
                 *args, **kwargs):

        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.delect_or_append = delect_or_append

    def execute(self, context):
        # connects to redshift using the connection created in the airflow UI
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # appends data to the respective table.
        if self.delect_or_append == 'append':
            self.log.info('Loading data into table')
            redshift.run("INSERT INTO {} {}".format(self.table, self.query))
        
        # truncates the table first and then loads it with data
        elif self.delect_or_append == 'delete':
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info('Loading data into table')
            redshift.run("INSERT INTO {} {}".format(self.table, self.query))
