from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql = "",
                 truncate_table = True,
                 table= "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.table = table
        self.sql = sql

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            redshift.run(f'INSERT INTO {self.table} {self.sql} WHERE NOT EXISTS (SELECT * FROM {self.table})')
            self.log.info(f'Fact table with data.')
        else:
            redshift.run(f'TRUNCATE songplays')
            self.log.info(f'Fact table {self.table} truncated.')

        