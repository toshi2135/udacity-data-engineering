from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
#         self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook("redshift")
        
        # Check each table
        for check in self.checks:
            records = redshift_hook.get_records(check["test_sql"])
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. '{}' returned no results".format(check["table"]))
            num_records = records[0][0]
            if len(records) < 1:
                raise ValueError("There is less than one record is found. {} found.".format(num_records))
            self.log.info("Data quality on table '{}' check passed with {} records".format(check["table"], records[0][0]))