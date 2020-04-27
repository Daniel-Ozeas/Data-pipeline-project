from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = '',
                sql = '',
                truncate_table = True,
                table = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql,
        self.truncate_table = truncate_table,
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            redshift.run(f'TRUNCATE {self.table}')
        self.log.info(f'The table {self.table} was truncated.')

        redshift.run(f'INSERT INTO {self.table} {self.sql}')  
        self.log.info('The insertion in {self.table} was completed.')
