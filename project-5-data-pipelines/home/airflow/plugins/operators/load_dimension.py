from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 mode="append", # default mode
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.mode = mode

    def execute(self, context):
#         self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(self.redshift_conn_id)
        
        if (self.mode == "truncate-insert"):
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info("Inserting data into destination Redshift table")
        insert_query = "INSERT INTO {} {}".format(self.table, self.sql)
        redshift.run(insert_query)