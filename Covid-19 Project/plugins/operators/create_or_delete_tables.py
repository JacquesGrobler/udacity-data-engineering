from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class CreateOrDeleteOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 create_or_delete = "",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(CreateOrDeleteOperator, self).__init__(*args, **kwargs)
        self.create_or_delete = create_or_delete
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Connection to redshift is successfull.')
        
        if self.create_or_delete == 'delete':
            self.log.info('Dropping tables...')
            for query in SqlQueries.delete_table_queries:
                redshift.run(query)
                self.log.info('Table dropped.')
         
        if self.create_or_delete == 'create':
            self.log.info('Creating tables...')
            for query in SqlQueries.create_table_queries:
                redshift.run(query)
                self.log.info('Table created.')