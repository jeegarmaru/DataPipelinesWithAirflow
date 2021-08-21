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
                 redshift_conn_id='redshift',
                 sql_tests=[],
                 expected_results=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_tests = sql_tests
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Running Data Quality tests')
        assert len(self.sql_tests) == len(self.expected_results)
        for sql_test, expected_result in zip(self.sql_tests, self.expected_results):
            result = redshift.get_records(sql_test)
            if not result or len(result[0]) <= 0 or (result[0][0] != expected_result):
                msg = f"Data Quality check with sql {sql_test} failed with the actual value of {result}"
                self.log.info(msg)
                raise ValueError(msg)
