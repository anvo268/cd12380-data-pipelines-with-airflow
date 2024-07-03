from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

# All of my customer operators
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# These are defined via UI and then used to connect to Redshift or an S3 bucket
# Connections
REDSHIFT_CXN = "redshift"
AWS_CREDS = "aws_credentials"

# Default args for the *tasks* rather than the DAG. So these are like operator level arguments
default_args = {
    "owner": "udacity",
    # If you set this earlier the DAG will try and "catch up" from that date
    "start_date": pendulum.now(),
    # Like in Dataswarm, doesn't depend on past operators completing successfully
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}


# TaskFlow API means that DAG gets defined in a function (here final_project) and then becomes a DAG because
# of the @dag decorator
@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    # Prevents DAG from catching up
    catchup=False,
)
def final_project():

    # Dummy Operators just like you're used to in Airflow
    start_operator = DummyOperator(task_id="Begin_execution")
    end_operator = DummyOperator(task_id="End_execution")

    create_tables = {}
    for table in [
        "staging_events",
        "staging_songs",
        "songplays",
        "users",
        "songs",
        "artists",
        "time",
    ]:
        # PostgresOperator is a built in Operator for running a SQL query
        create_tables[table] = PostgresOperator(
            task_id=f"Create_{table}",
            postgres_conn_id=REDSHIFT_CXN,
            sql=getattr(SqlQueries, f"{table}_table_create"),
        )

    # Everything below here is just the custom operators. Just review StageToRedshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        redshift_conn_id=REDSHIFT_CXN,
        aws_credentials_id=AWS_CREDS,
        s3_bucket="udacity-data-pipelines-prj-976617867078",
        s3_key="log-data/",
        json_option="s3://udacity-data-pipelines-prj-976617867078/log_json_path.json",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        redshift_conn_id=REDSHIFT_CXN,
        aws_credentials_id=AWS_CREDS,
        s3_bucket="udacity-data-pipelines-prj-976617867078",
        s3_key="song-data/A",  # Intentionally using a subset to speed things up
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        insert_sql=SqlQueries.songplay_table_insert,
        table="songplays",
        redshift_conn_id=REDSHIFT_CXN,
    )

    load_dim_tables = {}
    for table_name in ["users", "songs", "artists", "time"]:
        load_dim_tables[table_name] = LoadDimensionOperator(
            task_id=f"Load_{table_name}_dim_table",
            insert_sql=getattr(SqlQueries, f"{table_name}_table_insert"),
            table=table_name,
            redshift_conn_id=REDSHIFT_CXN,
        )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id=REDSHIFT_CXN,
        tests=[
            {
                "query": "SELECT COUNT(*) FROM users WHERE user_id IS NULL",
                "expected": 0,
                "greater_than": False,
                "less_than": False,
                "equals": True,
            },
        ],
    )

    # DAG Depenendencies
    (start_operator >> create_tables["staging_events"] >> stage_events_to_redshift)
    (start_operator >> create_tables["staging_songs"] >> stage_songs_to_redshift)

    create_tables["songplays"] >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    for table_name, load_dim_table in load_dim_tables.items():
        create_tables[table_name] >> load_dim_table
        load_songplays_table >> load_dim_table
        load_dim_table >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
