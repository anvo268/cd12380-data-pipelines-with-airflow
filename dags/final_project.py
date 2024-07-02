from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# Connections
REDSHIFT_CXN = "redshift"
AWS_CREDS = "aws_credentials"

default_args = {
    "owner": "udacity",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    catchup=False,
)
def final_project():

    # Operators
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
        create_tables[table] = PostgresOperator(
            task_id=f"Create_{table}",
            postgres_conn_id=REDSHIFT_CXN,
            sql=getattr(SqlQueries, f"{table}_table_create"),
        )

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
