from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"  # Color of the node in the DAG

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        region="us-east-1",
        json_option="auto",
        *args,
        **kwargs
    ):

        # super() gets you a termporary object from the parent class whose __init__ method gets called here.
        # Functionally what this does is it allows any arguments to the BaseOperator's constructor to be included
        # in StageToRedshiftOperator
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_option = json_option
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Logging here will show up in logs in Airflow
        self.log.info("StageToRedshiftOperator run for {self.table}, {s3.key}")

        # AWS connection is used for copying data out of the S3 bucket and into Redshift
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)

        # "Hooks" are how you connect to various services in Airflow. In this case Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        # Rendered key here allows execution_date to be passed in (or any other context variable)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.region,
            self.json_option,
        )
        redshift.run(formatted_sql)
