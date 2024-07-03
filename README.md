# Salient Points

* Project provides a good example of how to set up an Airflow pipeline that: 
    * Loads data from S3 to Redshift
    * Performs some basic transformations on that data to create a Snowflake schema
    * Demonstrates how to use the [TasksFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
    * Demonstrates how to define custom operators (Although I'm sure there are better options using built in 
    operators)

# Files

## Yes, Review Code
* [dags/final_project.py](https://github.com/anvo268/cd12380-data-pipelines-with-airflow/blob/anki-branch/dags/final_project.py) - Airflow DAG using TaskFlow API

* [plugins/operators/stage_redshift.py](https://github.com/anvo268/cd12380-data-pipelines-with-airflow/blob/anki-branch/plugins/operators/stage_redshift.py) - Sample custom operator

* [dags/udac_example.py](https://github.com/anvo268/cd12380-data-pipelines-with-airflow/blob/anki-branch/dags/udac_example_dag.py) - An example of how the DAG would look w/o using the TaskFlow API, along w/ some other
bells and whistles. Compare this to final_project.py. Dependencies and the like are not defined, it's just a skeleton


## Note, but don't review

* `docker-compose.yaml` - The docker configuration file for running the airflow server locally
    * YouTube Video explaining how to do this: [link](https://www.youtube.com/watch?v=aTaytcxy2Ck)
    * Run `docker-compose up` via terminal to start the server

* dags/ - Where the DAGs live. I think Airflow looks for this directory
* plugins/ - Where your custom operators live. I also think that if you downloaded some available plugin/operator, it 
would go here

# Airflow UI

Defining Airflow and Redshift connections. These get defined in the UI and can then be referenced in the pipelines

![alt text](<images/CleanShot 2024-07-02 at 14.39.04.png>)

Airflow DAG

![alt text](<images/CleanShot 2024-07-02 at 20.59.59.png>)