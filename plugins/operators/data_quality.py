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

            records = redshift.get_records(test["query"])[0]
            self.log.info(r"Records {records}")
            records = records[0]

            if test["equals"] and records == test["expected"]:
                self.log.info(f"DQ Passed for {test}")

            elif test["greater_than"] and records > test["expected"]:
                self.log.info(f"DQ Passed for {test}")

            elif not test["greater_than"] and records < test["expected"]:
                self.log.info(f"DQ Passed for {test}")

            else:
                message = f"DQ Failed for {test}"
                self.log.info(message)
                raise ValueError(message)

        self.log.info("DQ Finished")
