from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        # Define your operators params (with defaults) here
        # Example:
        # conn_id = your-connection-name
        redshift_conn_id="",
        tests=[],
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info("Running DataQualityOperator")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            self.log.info(f"DQ Test: {test}")

            test_query = test["query"]
            test_result = test["result"]

            records = redshift.get_records(test_query)[0][0]
            if self.equals and records == test_result:
                self.log.info("DQ Passed")

            elif self.greater_than and records > test_result:
                self.log.info("DQ Passed")

            elif not self.greater_than and records < test_result:
                self.log.info("DQ Passed")

            else:
                self.log.info("DQ Failed")

            self.log.info("DQ Finished")
