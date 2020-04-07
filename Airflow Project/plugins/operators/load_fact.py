from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table= "",
                 redshift_conn_id = "",
                 fact_query = "",
                 delect_or_append = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.fact_query = fact_query
        self.delect_or_append = delect_or_append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.delect_or_append == 'append':
            self.log.info('Loading data into table')
            redshift.run("INSERT INTO {} {}".format(self.table, self.fact_query))
            
        elif self.delect_or_append == 'delete':
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info('Loading data into table')
            redshift.run("INSERT INTO {} {}".format(self.table, self.fact_query))
