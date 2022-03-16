from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
#         self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
            
        self.log.info("Inserting data into destination Redshift table")
        insert_query = "INSERT INTO {} {}".format(self.table, self.sql)
        redshift.run(insert_query)
